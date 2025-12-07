# Permit 插件重新执行分析

## 1. 问题分析

### 1.1 Permit 插件的特点

从代码 `pkg/scheduler/framework/interface.go:469-478` 可以看到：

```go
type PermitPlugin interface {
    Plugin
    // Permit 接收 nodeName 参数，说明可能依赖特定节点
    Permit(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) (*Status, time.Duration)
}
```

**关键点**：
1. Permit 插件接收 `nodeName` 参数，说明它**可能依赖特定节点**
2. Permit 可能返回：
   - `Success`: 批准绑定
   - `Wait`: 延迟绑定，需要等待
   - `Unschedulable`: 拒绝绑定
   - `Error`: 错误

### 1.2 Permit Wait 机制

当 Permit 返回 `Wait` 时：
1. 创建一个 `waitingPod` 并添加到 `waitingPodsMap`
2. 在绑定阶段调用 `WaitOnPermit` 等待结果
3. 等待期间，Permit 插件可以调用 `Allow()` 或 `Reject()` 来批准或拒绝

**位置**：`pkg/scheduler/framework/runtime/framework.go:1146-1183`

### 1.3 当前实现的问题

#### Reserve 阶段失败时
- ✅ 重新执行 Assume
- ✅ 重新执行 Reserve
- ❌ **没有重新执行 Permit**

#### PreBind 阶段失败时
- ✅ 重新执行 Assume
- ✅ 重新执行 Reserve
- ✅ 重新执行 PreBind
- ❌ **没有重新执行 Permit**

#### Bind 阶段失败时
- ✅ 重新执行 Assume
- ✅ 重新执行 Reserve
- ✅ 重新执行 Bind
- ❌ **没有重新执行 Permit**

## 2. 为什么需要重新执行 Permit？

### 2.1 Permit 可能依赖特定节点

Permit 插件可能基于以下节点特定信息做决策：
- 节点标签（Labels）
- 节点注解（Annotations）
- 节点污点（Taints）
- 节点资源状态
- 节点上的其他 Pod

**示例场景**：
- Permit 插件检查节点是否有特定标签
- 如果节点A有标签但节点B没有，Permit 结果可能不同
- 切换到候选节点时，必须重新执行 Permit

### 2.2 Permit Wait 状态需要处理

如果 Permit 已经返回 `Wait`：
1. 需要取消当前的等待状态（Reject waitingPod）
2. 切换到候选节点后，需要重新执行 Permit
3. 如果新的 Permit 返回 `Wait`，需要重新等待

### 2.3 状态一致性

Permit 插件可能在 `CycleState` 中存储节点特定的状态：
- 如果切换到候选节点，这些状态可能不一致
- 重新执行 Permit 可以确保状态正确

## 3. 完整方案

### 3.1 方案概述

在 Reserve、PreBind、Bind 阶段失败并尝试候选节点时，需要：

1. **取消 Permit Wait 状态**（如果存在）
2. **重新执行 Permit 插件**
3. **处理新的 Permit 结果**（Success/Wait/Reject）

### 3.2 实现细节

#### 3.2.1 辅助函数：取消 Permit Wait

```go
// cancelPermitWait 取消 Pod 的 Permit Wait 状态
func (sched *Scheduler) cancelPermitWait(fwk framework.Framework, pod *v1.Pod) {
    // 如果 Pod 正在等待 Permit，拒绝它
    if fwk.GetWaitingPod(pod.UID) != nil {
        fwk.RejectWaitingPod(pod.UID)
        klog.V(4).InfoS("Cancelled permit wait for pod", "pod", klog.KObj(pod))
    }
}
```

#### 3.2.2 辅助函数：重新执行 Permit

```go
// reexecutePermitForCandidate 为候选节点重新执行 Permit 插件
// 返回 Permit 状态和是否需要等待
func (sched *Scheduler) reexecutePermitForCandidate(
    ctx context.Context,
    fwk framework.Framework,
    state *framework.CycleState,
    pod *v1.Pod,
    nodeName string,
) (*framework.Status, bool) {
    permitStatus := fwk.RunPermitPlugins(ctx, state, pod, nodeName)
    
    if permitStatus.Code() == framework.Wait {
        // Permit 返回 Wait，需要等待
        return permitStatus, true
    }
    
    if !permitStatus.IsSuccess() {
        // Permit 失败或拒绝
        return permitStatus, false
    }
    
    // Permit 成功
    return nil, false
}
```

### 3.3 修改 Reserve 阶段失败处理

**位置**：`pkg/scheduler/schedule_one.go:215-260`

```go
if sts := fwk.RunReservePluginsReserve(...); !sts.IsSuccess() {
    // 取消 Permit Wait（如果存在）
    sched.cancelPermitWait(fwk, assumedPod)
    
    // 尝试候选节点
    success, candidateNode, curNextNum := sched.tryCandidateNodesForReserve(
        schedulingCycleCtx, fwk, state, assumedPod, assumedPodInfo,
        scheduleResult.CandidateNodes[1:], start)
    
    if success {
        // 重新执行 Permit
        permitStatus, needWait := sched.reexecutePermitForCandidate(
            schedulingCycleCtx, fwk, state, assumedPod, candidateNode)
        
        if !permitStatus.IsSuccess() {
            // Permit 失败，清理并继续尝试下一个候选节点
            // ...
            continue
        }
        
        if needWait {
            // Permit 返回 Wait，需要等待（在绑定阶段处理）
            // 更新 scheduleResult 并继续
        }
        
        // Permit 成功，更新 scheduleResult 并继续
        scheduleResult.SuggestedHost = candidateNode
    }
}
```

### 3.4 修改 PreBind 阶段失败处理

**位置**：`pkg/scheduler/schedule_one.go:324-400`

PreBind 阶段在绑定周期（goroutine）中，此时 Permit 可能已经等待完成。需要：

1. 取消 Permit Wait（如果还在等待）
2. 重新执行 Permit
3. 如果 Permit 返回 Wait，需要重新等待

### 3.5 修改 Bind 阶段失败处理

**位置**：`pkg/scheduler/schedule_one.go:407-490`

Bind 阶段也在绑定周期中，处理方式类似 PreBind。

### 3.6 修改 tryCandidateNodesForReserve

需要添加 Permit 重新执行逻辑。

### 3.7 修改 tryCandidateNodesForPreBind

需要添加 Permit 重新执行逻辑。

## 4. 注意事项

### 4.1 Permit Wait 的处理

- 如果 Permit 返回 `Wait`，需要在绑定阶段调用 `WaitOnPermit`
- 在绑定周期（goroutine）中，如果切换到候选节点，需要：
  1. 取消当前的 Wait（如果还在等待）
  2. 重新执行 Permit
  3. 如果新的 Permit 返回 Wait，重新等待

### 4.2 状态清理

- 切换节点前，必须清理之前节点的状态：
  - Unreserve
  - ForgetPod
  - 取消 Permit Wait

### 4.3 性能考虑

- 重新执行 Permit 会增加调度延迟
- 但这是必要的，因为 Permit 可能依赖节点特定信息

## 5. 实现建议

### 5.1 优先级

1. **高优先级**：Reserve 阶段失败时重新执行 Permit
   - 此时还在调度周期中，Permit 可能还没执行或刚执行
   - 处理相对简单

2. **中优先级**：PreBind 阶段失败时重新执行 Permit
   - 此时在绑定周期中，Permit 可能已经等待完成
   - 需要处理 Wait 状态

3. **低优先级**：Bind 阶段失败时重新执行 Permit
   - 此时在绑定周期中，Permit 通常已经完成
   - 但如果 Permit 还在等待，需要处理

### 5.2 简化方案

如果 Permit 插件通常不依赖节点，可以考虑：
- 只在 Reserve 阶段失败时重新执行 Permit
- PreBind 和 Bind 阶段失败时，假设 Permit 已经通过，不再重新执行

但这需要确认 Permit 插件的实现是否依赖节点。

