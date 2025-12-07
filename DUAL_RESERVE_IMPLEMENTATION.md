# 双重资源预留实现总结

## 实现概述

已实现双重资源预留策略，在多个调度器并发工作时：
- **总是为首选节点预留资源**（100%）
- **以 (1-p1)*p2 的概率同时为次优节点也预留资源**
- **绑定操作仍然使用首选节点**
- 在绑定成功或失败时，清理次优节点的预留

## 实现的功能

### 1. 数据结构修改

**修改位置**：`pkg/scheduler/scheduler.go:386-396`

在 `ScheduleResult` 结构体中添加了 `SecondaryReservedNode` 字段：

```go
type ScheduleResult struct {
    SuggestedHost   string
    CandidateNodes  []CandidateNode
    EvaluatedNodes  int
    FeasibleNodes   int
    SecondaryReservedNode string  // 如果次优节点被预留，记录节点名称；否则为空
}
```

### 2. 概率性预留决策函数

**函数**：`shouldReserveSecondaryNode`

**位置**：`pkg/scheduler/schedule_one.go:1632-1683`

**功能**：
- 判断是否应该同时为次优节点预留资源
- 计算概率：(1-p1)*p2，其中 p1 是首选节点的采纳概率，p2 是次优节点的采纳概率
- 生成随机数，如果满足条件，返回 true

### 3. Reserve 阶段集成

**位置**：`pkg/scheduler/schedule_one.go:216-330`

**流程**：
1. 总是为首选节点执行 Reserve（100%）
2. 如果首选节点 Reserve 失败，清理次优节点的预留（如果存在）
3. 在首选节点 Reserve 成功后，概率性为次优节点也执行 Reserve
4. 如果次优节点 Reserve 成功，记录到 `scheduleResult.SecondaryReservedNode`

### 4. 绑定阶段集成

**位置**：`pkg/scheduler/schedule_one.go:471-603`

**流程**：
1. 使用首选节点进行绑定
2. 如果绑定失败，清理次优节点的预留（如果存在）
3. 如果绑定成功，清理次优节点的预留（如果存在）

## 执行流程

```
Assume Pod 到首选节点
  ↓
为首选节点执行 Reserve（100%）
  ↓
如果首选节点 Reserve 失败：
  - 清理次优节点的预留（如果存在）
  - 尝试候选节点或失败
  ↓
如果首选节点 Reserve 成功：
  - 概率性预留决策：
    - 计算概率：(1-p1)*p2
    - 生成随机数 r
    - 如果 r < (1-p1)*p2：同时为次优节点也执行 Reserve
  - 如果为次优节点预留成功，记录到 scheduleResult.SecondaryReservedNode
  ↓
继续后续流程（Permit、绑定等）
  ↓
绑定阶段：
  - 使用首选节点进行绑定
  - 如果绑定成功：清理次优节点的预留
  - 如果绑定失败：清理次优节点的预留
```

## 关键代码

### 概率性预留决策

```go
// 计算次优节点预留概率：(1-p1)*p2
secondaryReserveProb := (1.0 - p1) * p2

// 生成随机数
randomValue := rand.Float64()

shouldReserve := randomValue < secondaryReserveProb
```

### Reserve 阶段

```go
// 总是为首选节点预留
fwk.RunReservePluginsReserve(schedulingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)

// 概率性为次优节点预留
if shouldReserveSecondary {
    secondaryPod := assumedPod.DeepCopy()
    secondaryPod.Spec.NodeName = secondaryNode
    if sts := fwk.RunReservePluginsReserve(schedulingCycleCtx, state, secondaryPod, secondaryNode); sts.IsSuccess() {
        scheduleResult.SecondaryReservedNode = secondaryNode
    }
}
```

### 绑定阶段清理

```go
// 绑定成功后清理次优节点的预留
if scheduleResult.SecondaryReservedNode != "" {
    secondaryPod := assumedPod.DeepCopy()
    secondaryPod.Spec.NodeName = scheduleResult.SecondaryReservedNode
    fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, secondaryPod, scheduleResult.SecondaryReservedNode)
}
```

## 优势

1. **防止资源竞争**：通过概率性为次优节点预留，减少其他调度器占用次优节点的可能性
2. **提高调度成功率**：如果首选节点绑定失败，可以快速切换到已预留的次优节点
3. **灵活性**：根据采纳概率动态调整预留策略
4. **资源利用**：只在概率性情况下为两个节点预留资源，不会总是占用两个节点的资源
5. **状态一致性**：正确清理次优节点的预留，避免资源泄漏

## 注意事项

1. **资源占用**：在概率性情况下，会同时为两个节点预留资源，需要确保 Reserve 插件支持这种场景
2. **状态一致性**：需要正确清理次优节点的预留，避免资源泄漏
3. **日志记录**：记录概率性预留决策，便于调试和监控
4. **Assume 状态**：次优节点的 Reserve 不需要 Assume（因为绑定仍然使用首选节点）

## 示例场景

假设：
- 首选节点的采纳概率 p1 = 0.8
- 次优节点的采纳概率 p2 = 0.6

则：
- 首选节点被预留的概率：100%（总是预留）
- 次优节点被预留的概率：(1-0.8)*0.6 = 12%

这样，在多个调度器并发工作时：
- 100% 的情况下为首选节点预留资源
- 12% 的情况下同时为次优节点也预留资源，防止其他调度器占用
- 绑定操作仍然使用首选节点
- 如果绑定成功，清理次优节点的预留
- 如果绑定失败，清理次优节点的预留

## 测试建议

1. **单元测试**：
   - 测试 `shouldReserveSecondaryNode` 函数的概率计算
   - 测试不同概率值下的行为

2. **集成测试**：
   - 测试多个调度器同时调度时的行为
   - 验证双重资源预留的正确性
   - 验证资源清理的正确性

3. **压力测试**：
   - 测试高并发场景下的性能
   - 验证概率性预留的效果

