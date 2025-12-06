# 候选节点使用时机分析

## 当前实现分析

### 1. 确认：只有 Bind 失败时才尝试候选节点

通过代码分析，确认**当前实现中，只有在 Bind 阶段失败时才会尝试候选节点**。

调度流程各阶段的失败处理：

| 阶段 | 失败时是否尝试候选节点 | 代码位置 |
|------|---------------------|---------|
| **Assume** | ❌ 否 | `schedule_one.go:196-208` |
| **Reserve** | ❌ 否 | `schedule_one.go:214-227` |
| **Permit** | ❌ 否 | `schedule_one.go:233-259` |
| **WaitOnPermit** | ❌ 否 | `schedule_one.go:283-318` |
| **PreBind** | ❌ 否 | `schedule_one.go:324-344` |
| **Bind** | ✅ **是** | `schedule_one.go:350-445` |

### 2. 为什么其他阶段失败时不尝试候选节点？

#### 2.1 Assume 阶段失败

**原因分析：**
- Assume 失败通常表示**缓存状态不一致**（如节点信息已过期、Pod 已被其他调度器绑定等）
- 此时还没有真正占用任何资源，失败是**瞬时的、可恢复的**
- 重新调度（重新执行完整的调度流程）比尝试候选节点更合适，因为：
  - 可以重新评估所有节点（包括候选节点）
  - 可以获取最新的节点状态
  - 避免在缓存不一致的情况下继续操作

**结论：** Assume 阶段失败时不尝试候选节点是**合理的**。

#### 2.2 Reserve 阶段失败

**原因分析：**
- Reserve 失败通常表示**节点特定的资源预留失败**（如 Volume 绑定失败、网络资源不足等）
- 此时已经 Assume 了 Pod，但还没有真正绑定
- **理论上可以尝试候选节点**，因为：
  - 不同节点的资源状态可能不同
  - 候选节点可能可以成功预留资源
  - 可以避免重新调度带来的延迟

**当前不尝试的原因：**
- 代码设计时可能认为 Reserve 失败是**节点特定的问题**，应该重新调度
- 但实际场景中，Reserve 失败可能是**瞬时的、可恢复的**（如 Volume 服务暂时不可用）

**结论：** Reserve 阶段失败时**可以考虑**尝试候选节点。

#### 2.3 Permit 阶段失败

**原因分析：**
- Permit 插件可能基于**节点特定的策略**拒绝 Pod（如节点标签、污点等）
- 此时已经 Reserve 了资源
- **理论上可以尝试候选节点**，因为：
  - 不同节点的策略可能不同
  - 候选节点可能满足 Permit 插件的条件

**当前不尝试的原因：**
- Permit 失败通常表示**策略性的拒绝**，不是资源问题
- 如果 Permit 插件基于全局策略（而非节点特定），尝试候选节点可能无效

**结论：** Permit 阶段失败时**可以尝试候选节点**，但需要评估 Permit 插件的策略类型。

#### 2.4 WaitOnPermit 和 PreBind 阶段失败

**原因分析：**
- 这两个阶段失败通常表示**节点特定的操作失败**（如网络配置、存储准备等）
- 此时已经进行了很多操作（Assume、Reserve、Permit）
- **理论上可以尝试候选节点**，但：
  - 已经消耗了一定的时间和资源
  - 需要清理已执行的操作

**结论：** 这两个阶段失败时**可以尝试候选节点**，但需要仔细处理状态清理。

### 3. 改进建议：在更多阶段尝试候选节点

#### 3.1 建议在以下阶段尝试候选节点

1. **Reserve 阶段失败** ⭐⭐⭐（高优先级）
   - 原因：资源预留失败通常是节点特定的，候选节点可能成功
   - 影响：可以显著提高调度成功率，减少重新调度延迟

2. **PreBind 阶段失败** ⭐⭐（中优先级）
   - 原因：PreBind 失败通常是节点特定的操作失败
   - 影响：可以避免重新调度，但需要处理状态清理

3. **Permit 阶段失败** ⭐（低优先级）
   - 原因：取决于 Permit 插件的策略类型
   - 影响：如果 Permit 插件是节点特定的，尝试候选节点有意义

#### 3.2 不建议在以下阶段尝试候选节点

1. **Assume 阶段失败**
   - 原因：缓存状态不一致，应该重新调度

2. **WaitOnPermit 阶段失败**
   - 原因：已经等待了 Permit，失败后尝试候选节点可能意义不大

### 4. 实现方案

#### 4.1 提取候选节点尝试逻辑为通用函数

创建一个通用的函数来处理候选节点尝试，可以在多个阶段复用：

```go
// tryCandidateNodes 尝试将 Pod 绑定到候选节点列表中的节点
// 返回成功绑定的节点名称，如果所有候选节点都失败则返回错误
func (sched *Scheduler) tryCandidateNodes(
    ctx context.Context,
    fwk framework.Framework,
    state *framework.CycleState,
    assumedPod *v1.Pod,
    candidateNodes []CandidateNode,
    startTime time.Time,
    podInfo *framework.QueuedPodInfo,
) (string, error) {
    // 实现候选节点尝试逻辑
    // 包括：Assume -> Reserve -> Permit -> PreBind -> Bind
}
```

#### 4.2 在 Reserve 阶段失败时尝试候选节点

修改 Reserve 失败处理逻辑：

```go
if sts := fwk.RunReservePluginsReserve(...); !sts.IsSuccess() {
    sched.nodeHistoryManager.RecordConflict(scheduleResult.SuggestedHost)
    
    // 如果有候选节点，尝试候选节点
    if len(scheduleResult.CandidateNodes) > 1 {
        bindingNode, err := sched.tryCandidateNodes(
            schedulingCycleCtx, fwk, state, assumedPod,
            scheduleResult.CandidateNodes[1:], start, assumedPodInfo)
        if err == nil {
            // 成功绑定到候选节点
            return
        }
    }
    
    // 所有候选节点都失败，执行原有失败处理逻辑
    // ...
}
```

#### 4.3 在 PreBind 阶段失败时尝试候选节点

类似地，在 PreBind 失败时也可以尝试候选节点。

### 5. 注意事项

1. **状态清理**：在尝试候选节点前，必须清理之前节点的状态（Unreserve、ForgetPod）
2. **性能影响**：尝试候选节点会增加调度延迟，需要权衡
3. **插件兼容性**：某些插件可能不支持在多个节点上重复执行
4. **并发安全**：确保在尝试候选节点时正确处理并发访问

### 6. 总结

- **当前实现**：只有 Bind 失败时才尝试候选节点
- **改进方向**：在 Reserve 和 PreBind 阶段失败时也尝试候选节点
- **不建议改进**：Assume 和 WaitOnPermit 阶段
- **实现建议**：提取通用函数，在多个阶段复用候选节点尝试逻辑

