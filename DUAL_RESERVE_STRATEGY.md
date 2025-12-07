# 双重资源预留策略完整方案

## 1. 需求分析

### 1.1 场景
- 多个调度器实例同时工作
- 调度器选择了多个备选节点（首选节点、次优节点等）
- 首选节点的采纳概率为 p1，次优节点的采纳概率为 p2

### 1.2 目标
- **总是为首选节点预留资源**（100%）
- **以 (1-p1)*p2 的概率同时为次优节点也预留资源**
- **绑定操作仍然使用首选节点**
- 这样，在概率性情况下，会同时为两个节点预留资源，防止其他调度器占用次优节点

### 1.3 关键点
- 在 Reserve 阶段，总是为首选节点执行 Reserve
- 以 (1-p1)*p2 的概率同时为次优节点也执行 Reserve
- 需要记录次优节点是否被预留，以便后续清理
- 绑定操作使用首选节点
- 如果绑定成功，清理次优节点的预留（如果有）
- 如果绑定失败，可以尝试次优节点（如果已经预留了）

## 2. 实现方案

### 2.1 方案概述

在 Reserve 阶段，执行双重预留：

1. **总是为首选节点预留**：
   - 为首选节点执行 Reserve 插件

2. **概率性为次优节点预留**：
   - 计算概率：(1-p1)*p2
   - 生成随机数，如果满足条件，同时为次优节点也执行 Reserve
   - 记录次优节点是否被预留

3. **绑定阶段**：
   - 使用首选节点进行绑定
   - 如果绑定成功，清理次优节点的预留（如果有）
   - 如果绑定失败，可以尝试次优节点（如果已经预留了）

### 2.2 数据结构修改

需要在 `ScheduleResult` 中添加字段来记录次优节点是否被预留：

```go
type ScheduleResult struct {
    SuggestedHost   string
    EvaluatedNodes  int
    FeasibleNodes   int
    CandidateNodes  []CandidateNode
    SecondaryReservedNode string  // 如果次优节点被预留，记录节点名称；否则为空
}
```

### 2.3 实现步骤

#### 步骤1：修改 `determineReserveStrategy` 函数

改为返回是否应该同时为次优节点预留：

```go
// shouldReserveSecondaryNode 判断是否应该同时为次优节点预留资源
// 返回：是否应该为次优节点预留
func (sched *Scheduler) shouldReserveSecondaryNode(
    candidateNodes []CandidateNode,
    pod *v1.Pod,
) bool {
    if len(candidateNodes) < 2 {
        return false
    }

    p1 := candidateNodes[0].AdoptionProbability
    p2 := candidateNodes[1].AdoptionProbability

    // 确保概率值在有效范围内
    if p1 < 0 { p1 = 0 }
    if p1 > 1 { p1 = 1 }
    if p2 < 0 { p2 = 0 }
    if p2 > 1 { p2 = 1 }

    // 计算次优节点预留概率：(1-p1)*p2
    secondaryReserveProb := (1.0 - p1) * p2

    // 生成随机数
    randomValue := rand.Float64()

    shouldReserve := randomValue < secondaryReserveProb

    klog.V(4).InfoS("Determining secondary node reserve by probability",
        "pod", klog.KObj(pod),
        "primaryNode", candidateNodes[0].Name,
        "primaryProb", p1,
        "secondaryNode", candidateNodes[1].Name,
        "secondaryProb", p2,
        "secondaryReserveProb", secondaryReserveProb,
        "randomValue", randomValue,
        "shouldReserve", shouldReserve)

    return shouldReserve
}
```

#### 步骤2：修改 Reserve 阶段逻辑

在 Reserve 阶段，执行双重预留：

```go
// --- 运行 Reserve 插件（首选节点）---
// 总是为首选节点预留资源
if sts := fwk.RunReservePluginsReserve(schedulingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost); !sts.IsSuccess() {
    // Reserve 失败处理...
}

// --- 概率性为次优节点预留资源 ---
// 判断是否应该同时为次优节点预留
if len(scheduleResult.CandidateNodes) >= 2 {
    shouldReserveSecondary := sched.shouldReserveSecondaryNode(
        scheduleResult.CandidateNodes,
        assumedPod,
    )

    if shouldReserveSecondary {
        secondaryNode := scheduleResult.CandidateNodes[1].Name
        klog.V(4).InfoS("Reserving secondary node by probability",
            "pod", klog.KObj(assumedPod),
            "secondaryNode", secondaryNode)

        // 为次优节点执行 Reserve
        // 注意：需要创建一个临时的 Pod 副本，因为 Reserve 插件可能需要 Pod 的 NodeName
        secondaryPod := assumedPod.DeepCopy()
        secondaryPod.Spec.NodeName = secondaryNode

        // 尝试为次优节点执行 Reserve
        if sts := fwk.RunReservePluginsReserve(schedulingCycleCtx, state, secondaryPod, secondaryNode); sts.IsSuccess() {
            // 记录次优节点被预留
            scheduleResult.SecondaryReservedNode = secondaryNode
            klog.V(4).InfoS("Successfully reserved secondary node",
                "pod", klog.KObj(assumedPod),
                "secondaryNode", secondaryNode)
        } else {
            // 次优节点 Reserve 失败，记录但不影响主流程
            klog.V(4).InfoS("Failed to reserve secondary node, continuing with primary node only",
                "pod", klog.KObj(assumedPod),
                "secondaryNode", secondaryNode,
                "status", sts)
        }
    }
}
```

#### 步骤3：修改绑定阶段逻辑

在绑定成功或失败时，清理次优节点的预留：

```go
// --- 尝试绑定到首选节点 ---
bindingNode := scheduleResult.SuggestedHost
err := sched.bind(bindingCycleCtx, fwk, assumedPod, bindingNode, state)

if err != nil {
    // 绑定失败
    // 如果次优节点被预留了，尝试使用次优节点
    if scheduleResult.SecondaryReservedNode != "" {
        // 清理首选节点的预留
        fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, assumedPod, bindingNode)
        
        // 尝试使用次优节点绑定
        // ...
    }
    
    // 清理次优节点的预留（如果存在）
    if scheduleResult.SecondaryReservedNode != "" {
        secondaryPod := assumedPod.DeepCopy()
        secondaryPod.Spec.NodeName = scheduleResult.SecondaryReservedNode
        fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, secondaryPod, scheduleResult.SecondaryReservedNode)
    }
} else {
    // 绑定成功
    // 清理次优节点的预留（如果存在）
    if scheduleResult.SecondaryReservedNode != "" {
        secondaryPod := assumedPod.DeepCopy()
        secondaryPod.Spec.NodeName = scheduleResult.SecondaryReservedNode
        fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, secondaryPod, scheduleResult.SecondaryReservedNode)
        klog.V(4).InfoS("Cleaned up secondary node reservation after successful binding",
            "pod", klog.KObj(assumedPod),
            "primaryNode", bindingNode,
            "secondaryNode", scheduleResult.SecondaryReservedNode)
    }
}
```

## 3. 完整实现代码

### 3.1 修改 ScheduleResult 结构

在 `pkg/scheduler/scheduler.go` 中：

```go
type ScheduleResult struct {
    SuggestedHost   string
    EvaluatedNodes  int
    FeasibleNodes   int
    CandidateNodes  []CandidateNode
    SecondaryReservedNode string  // 如果次优节点被预留，记录节点名称；否则为空
}
```

### 3.2 添加辅助函数

在 `pkg/scheduler/schedule_one.go` 中添加：

1. `shouldReserveSecondaryNode`: 判断是否应该同时为次优节点预留
2. `reserveSecondaryNode`: 为次优节点执行 Reserve
3. `unreserveSecondaryNode`: 清理次优节点的预留

### 3.3 修改 Reserve 阶段

在 Reserve 阶段，执行双重预留。

### 3.4 修改绑定阶段

在绑定成功或失败时，清理次优节点的预留。

## 4. 执行流程

```
Assume Pod 到首选节点
  ↓
为首选节点执行 Reserve（100%）
  ↓
概率性预留决策：
  - 计算概率：(1-p1)*p2
  - 生成随机数 r
  - 如果 r < (1-p1)*p2：同时为次优节点也执行 Reserve
  ↓
如果为次优节点预留：
  - 为次优节点执行 Reserve
  - 记录 scheduleResult.SecondaryReservedNode
  ↓
继续后续流程（Permit、绑定等）
  ↓
绑定阶段：
  - 使用首选节点进行绑定
  - 如果绑定成功：清理次优节点的预留
  - 如果绑定失败：可以尝试次优节点（如果已经预留了），然后清理预留
```

## 5. 优势

1. **防止资源竞争**：通过概率性为次优节点预留，减少其他调度器占用次优节点的可能性
2. **提高调度成功率**：如果首选节点绑定失败，可以快速切换到已预留的次优节点
3. **灵活性**：根据采纳概率动态调整预留策略
4. **资源利用**：只在概率性情况下为两个节点预留资源，不会总是占用两个节点的资源

## 6. 注意事项

1. **资源占用**：在概率性情况下，会同时为两个节点预留资源，需要确保 Reserve 插件支持这种场景
2. **状态一致性**：需要正确清理次优节点的预留，避免资源泄漏
3. **日志记录**：记录概率性预留决策，便于调试和监控
4. **Assume 状态**：次优节点的 Reserve 不需要 Assume（因为绑定仍然使用首选节点）

## 7. 测试建议

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

## 8. 示例场景

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
- 如果绑定失败，可以快速切换到已预留的次优节点

