# 概率性资源预留实现总结

## 实现概述

已实现概率性资源预留策略，在多个调度器并发工作时，根据采纳概率为首选节点或次优节点预留资源。

## 实现的功能

### 1. 概率性预留决策

**函数**：`determineReserveStrategy`

**逻辑**：
- 首选节点的采纳概率为 p1，次优节点的采纳概率为 p2
- 以 p1 的概率为首选节点预留
- 如果不为首选节点预留（概率 1-p1），则以 p2 的概率为次优节点预留
- 这样，次优节点被预留的概率是 (1-p1)*p2

**实现位置**：`pkg/scheduler/schedule_one.go:1607-1675`

### 2. 候选节点列表调整

**函数**：`adjustCandidateNodesOrder`

**功能**：
- 如果为次优节点预留，将预留的节点移到候选节点列表的第一位
- 确保后续流程使用预留的节点作为主节点

**实现位置**：`pkg/scheduler/schedule_one.go:1677-1715`

### 3. Reserve 阶段集成

**位置**：`pkg/scheduler/schedule_one.go:212-250`

**流程**：
1. 在 Assume 之后、Reserve 之前，调用 `determineReserveStrategy` 决定预留策略
2. 如果为次优节点预留：
   - 调整候选节点列表的顺序
   - 更新 `scheduleResult.SuggestedHost` 为预留的节点
   - 更新 `assumedPod.Spec.NodeName`
   - 重新 Assume Pod 到新的节点
3. 执行 Reserve 插件

## 执行流程

```
Assume Pod 到首选节点
  ↓
概率性预留决策：
  - 生成随机数 r
  - 如果 r < p1：为首选节点预留
  - 否则如果 r < p1 + (1-p1)*p2：为次优节点预留
  - 否则：fallback 到首选节点
  ↓
如果为次优节点预留：
  - 调整候选节点列表顺序
  - 更新 scheduleResult.SuggestedHost
  - 更新 assumedPod.Spec.NodeName
  - 重新 Assume Pod 到次优节点
  ↓
执行 Reserve（为选中的节点）
  ↓
继续后续流程（Permit、绑定等）
```

## 关键代码

### 概率性预留决策

```go
// 计算预留概率
primaryReserveProb := p1
secondaryReserveProb := (1.0 - p1) * p2

// 生成随机数
randomValue := rand.Float64()

if randomValue < primaryReserveProb {
    // 为首选节点预留
    return candidateNodes[0].Name, false
} else if randomValue < primaryReserveProb+secondaryReserveProb {
    // 为次优节点预留
    return candidateNodes[1].Name, true
} else {
    // Fallback：为首选节点预留
    return candidateNodes[0].Name, false
}
```

### Reserve 阶段集成

```go
// 概率性资源预留决策
reservedNode, reservedSecondary := sched.determineReserveStrategy(
    scheduleResult.CandidateNodes,
    assumedPod,
)

// 如果为次优节点预留，调整候选节点列表和 Assume
if reservedSecondary {
    scheduleResult.CandidateNodes = sched.adjustCandidateNodesOrder(
        scheduleResult.CandidateNodes,
        reservedNode,
    )
    scheduleResult.SuggestedHost = reservedNode
    assumedPod.Spec.NodeName = reservedNode
    // 重新 Assume Pod 到新的节点
    // ...
}

// 执行 Reserve
fwk.RunReservePluginsReserve(...)
```

## 优势

1. **防止资源竞争**：通过概率性预留，减少多个调度器同时竞争同一节点的情况
2. **提高调度成功率**：为次优节点预留资源，增加调度成功的概率
3. **灵活性**：根据采纳概率动态调整预留策略
4. **向后兼容**：如果只有一个候选节点，行为与原来一致

## 注意事项

1. **概率值范围**：确保概率值在 [0, 1] 范围内
2. **状态一致性**：如果 Reserve 失败，需要清理状态并尝试备选节点
3. **日志记录**：记录概率性预留决策，便于调试和监控
4. **Assume 重新执行**：如果为次优节点预留，需要重新 Assume Pod 到新节点

## 测试建议

1. **单元测试**：
   - 测试 `determineReserveStrategy` 函数的概率计算
   - 测试不同概率值下的行为

2. **集成测试**：
   - 测试多个调度器同时调度时的行为
   - 验证资源预留的正确性

3. **压力测试**：
   - 测试高并发场景下的性能
   - 验证概率性预留的效果

## 示例场景

假设：
- 首选节点的采纳概率 p1 = 0.8
- 次优节点的采纳概率 p2 = 0.6

则：
- 首选节点被预留的概率：0.8
- 次优节点被预留的概率：(1-0.8)*0.6 = 0.12
- Fallback 到首选节点的概率：1 - 0.8 - 0.12 = 0.08

这样，在多个调度器并发工作时，可以：
- 80% 的情况下为首选节点预留资源
- 12% 的情况下为次优节点预留资源，防止其他调度器占用
- 8% 的情况下 fallback 到首选节点

