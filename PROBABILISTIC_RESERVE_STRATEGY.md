# 概率性资源预留策略完整方案

## 1. 需求分析

### 1.1 场景
- 多个调度器实例同时工作
- 调度器选择了多个备选节点（首选节点、次优节点等）
- 首选节点的采纳概率为 p1，次优节点的采纳概率为 p2

### 1.2 目标
- 以 p1 的概率为首选节点预留资源
- 如果不为首选节点预留（概率 1-p1），则以 p2 的概率为次优节点预留资源
- 这样，次优节点被预留的概率是 (1-p1)*p2
- **目的**：防止其他调度器提前占用次优节点

### 1.3 关键点
- 在 Reserve 阶段根据概率决定预留哪个节点
- 一个 Pod 只能为一个节点预留资源（不能同时为多个节点预留）
- 如果为首选节点预留，就不为次优节点预留
- 如果不为首选节点预留，则根据概率决定是否为次优节点预留

## 2. 实现方案

### 2.1 方案概述

在 Reserve 阶段，根据概率决定预留策略：

1. **计算预留概率**：
   - 首选节点预留概率：p1
   - 次优节点预留概率：(1-p1)*p2

2. **根据概率选择预留节点**：
   - 生成随机数 r (0 <= r < 1)
   - 如果 r < p1：为首选节点预留
   - 否则如果 r < p1 + (1-p1)*p2：为次优节点预留
   - 否则：不预留（fallback 到首选节点）

3. **执行预留**：
   - 为选中的节点执行 Reserve
   - 更新 `scheduleResult.SuggestedHost` 为预留的节点
   - 保留其他候选节点作为备选

### 2.2 实现步骤

#### 步骤1：在 Reserve 阶段前添加概率性预留逻辑

在 `schedule_one.go` 的 Reserve 阶段前，添加概率性预留逻辑：

```go
// 根据采纳概率决定预留策略
reservedNode, shouldReserveSecondary := sched.determineReserveStrategy(
    scheduleResult.CandidateNodes,
    assumedPod,
)

if shouldReserveSecondary {
    // 为次优节点预留
    // 更新 scheduleResult.SuggestedHost 为次优节点
    // 执行 Reserve
} else {
    // 为首选节点预留（原有逻辑）
    // 执行 Reserve
}
```

#### 步骤2：实现概率性预留决策函数

```go
// determineReserveStrategy 根据采纳概率决定预留策略
// 返回：预留的节点名称，是否应该为次优节点预留
func (sched *Scheduler) determineReserveStrategy(
    candidateNodes []CandidateNode,
    pod *v1.Pod,
) (string, bool) {
    if len(candidateNodes) < 2 {
        // 只有一个候选节点，直接为首选节点预留
        return candidateNodes[0].Name, false
    }

    p1 := candidateNodes[0].AdoptionProbability
    p2 := candidateNodes[1].AdoptionProbability

    // 计算预留概率
    // 首选节点预留概率：p1
    // 次优节点预留概率：(1-p1)*p2
    primaryReserveProb := p1
    secondaryReserveProb := (1.0 - p1) * p2

    // 生成随机数
    randomValue := rand.Float64()

    klog.V(4).InfoS("Determining reserve strategy by probability",
        "pod", klog.KObj(pod),
        "primaryNode", candidateNodes[0].Name,
        "primaryProb", p1,
        "secondaryNode", candidateNodes[1].Name,
        "secondaryProb", p2,
        "primaryReserveProb", primaryReserveProb,
        "secondaryReserveProb", secondaryReserveProb,
        "randomValue", randomValue)

    if randomValue < primaryReserveProb {
        // 为首选节点预留
        klog.V(4).InfoS("Reserving primary node by probability",
            "pod", klog.KObj(pod),
            "node", candidateNodes[0].Name,
            "probability", primaryReserveProb)
        return candidateNodes[0].Name, false
    } else if randomValue < primaryReserveProb+secondaryReserveProb {
        // 为次优节点预留
        klog.V(4).InfoS("Reserving secondary node by probability",
            "pod", klog.KObj(pod),
            "node", candidateNodes[1].Name,
            "probability", secondaryReserveProb)
        return candidateNodes[1].Name, true
    } else {
        // Fallback：为首选节点预留
        klog.V(4).InfoS("Fallback to primary node reservation",
            "pod", klog.KObj(pod),
            "node", candidateNodes[0].Name)
        return candidateNodes[0].Name, false
    }
}
```

#### 步骤3：修改 Reserve 阶段逻辑

在 Reserve 阶段，根据概率性预留决策执行预留：

```go
// --- 概率性资源预留决策 ---
// 根据采纳概率决定为首选节点还是次优节点预留资源
reservedNode, reservedSecondary := sched.determineReserveStrategy(
    scheduleResult.CandidateNodes,
    assumedPod,
)

// 更新 scheduleResult.SuggestedHost 为预留的节点
originalSuggestedHost := scheduleResult.SuggestedHost
scheduleResult.SuggestedHost = reservedNode

// 如果为次优节点预留，需要调整候选节点列表的顺序
if reservedSecondary {
    // 将次优节点移到第一位，首选节点移到第二位
    // 这样后续流程会使用次优节点作为主节点
    // 但保留首选节点作为备选
    // ...
}

// --- 运行 Reserve 插件 ---
if sts := fwk.RunReservePluginsReserve(schedulingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost); !sts.IsSuccess() {
    // Reserve 失败处理...
}
```

### 2.3 候选节点列表调整

如果为次优节点预留，需要调整候选节点列表的顺序，确保：
- 次优节点作为主节点（`scheduleResult.SuggestedHost`）
- 首选节点作为第一个备选节点
- 其他节点保持顺序

```go
// adjustCandidateNodesOrder 调整候选节点列表的顺序
// 如果为次优节点预留，将次优节点移到第一位
func (sched *Scheduler) adjustCandidateNodesOrder(
    candidateNodes []CandidateNode,
    reservedNode string,
) []CandidateNode {
    if len(candidateNodes) < 2 {
        return candidateNodes
    }

    // 找到预留节点在列表中的位置
    reservedIndex := -1
    for i, node := range candidateNodes {
        if node.Name == reservedNode {
            reservedIndex = i
            break
        }
    }

    if reservedIndex <= 0 {
        // 预留节点已经是第一个，或者没找到，不需要调整
        return candidateNodes
    }

    // 创建新的候选节点列表
    newCandidates := make([]CandidateNode, 0, len(candidateNodes))
    
    // 首先添加预留节点
    newCandidates = append(newCandidates, candidateNodes[reservedIndex])
    
    // 然后按顺序添加其他节点（不包括预留节点）
    for i, node := range candidateNodes {
        if i != reservedIndex {
            newCandidates = append(newCandidates, node)
        }
    }

    return newCandidates
}
```

## 3. 完整实现代码

### 3.1 添加辅助函数

在 `schedule_one.go` 中添加以下函数：

1. `determineReserveStrategy`: 根据概率决定预留策略
2. `adjustCandidateNodesOrder`: 调整候选节点列表的顺序

### 3.2 修改 Reserve 阶段

在 Reserve 阶段前添加概率性预留决策逻辑。

## 4. 执行流程

```
调度阶段
  ↓
选择候选节点（最多3个）
  ↓
计算采纳概率
  ↓
概率性预留决策：
  - 生成随机数 r
  - 如果 r < p1：为首选节点预留
  - 否则如果 r < p1 + (1-p1)*p2：为次优节点预留
  - 否则：fallback 到首选节点
  ↓
调整候选节点列表顺序（如果为次优节点预留）
  ↓
执行 Reserve（为选中的节点）
  ↓
继续后续流程（Permit、绑定等）
```

## 5. 优势

1. **防止资源竞争**：通过概率性预留，减少多个调度器同时竞争同一节点的情况
2. **提高调度成功率**：为次优节点预留资源，增加调度成功的概率
3. **灵活性**：根据采纳概率动态调整预留策略
4. **向后兼容**：如果只有一个候选节点，行为与原来一致

## 6. 注意事项

1. **概率计算**：确保概率值在 [0, 1] 范围内
2. **状态一致性**：如果 Reserve 失败，需要清理状态并尝试备选节点
3. **日志记录**：记录概率性预留决策，便于调试和监控
4. **性能影响**：概率计算和节点列表调整的开销很小

## 7. 测试建议

1. **单元测试**：
   - 测试 `determineReserveStrategy` 函数的概率计算
   - 测试不同概率值下的行为

2. **集成测试**：
   - 测试多个调度器同时调度时的行为
   - 验证资源预留的正确性

3. **压力测试**：
   - 测试高并发场景下的性能
   - 验证概率性预留的效果

