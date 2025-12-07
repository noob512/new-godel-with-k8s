# 基于累积概率的节点选择方案

## 概述

本方案实现了基于累积概率的节点选择逻辑，用于在调度时根据节点的采纳概率选择主节点和备选节点。

## 核心逻辑

### 概率选择规则

1. **第一个节点**：采纳率为 p1，有 **p1** 的概率直接 reserve 第一个节点
2. **第二个节点**：如果第一个节点没有被选中（概率 1-p1），第二个节点的采纳率为 p2，有 **(1-p1) × p2** 的概率 reserve 第二个节点
3. **第三个节点**：如果前两个都没被选中，第三个节点的采纳率为 p3，有 **(1-p1) × (1-p2) × p3** 的概率 reserve 第三个节点
4. **Fallback**：如果所有节点都没有被选中，则 fallback 到第一个节点

### 累积概率计算

累积概率的计算公式：

- **节点1的累积概率**：`P1 = p1`
- **节点2的累积概率**：`P2 = p1 + (1-p1) × p2`
- **节点3的累积概率**：`P3 = p1 + (1-p1) × p2 + (1-p1) × (1-p2) × p3`

### 选择算法

1. 生成一个随机数 `r` (0 ≤ r < 1)
2. 如果 `r < P1`，选择节点1
3. 否则如果 `r < P2`，选择节点2
4. 否则如果 `r < P3`，选择节点3
5. 否则 fallback 到节点1

### 备选节点列表

- **最多选择3个备选节点**（包括主节点）
- 主节点是选中的节点（放在列表第一位）
- 其他节点按原始顺序添加（不包括已选中的节点）

## 实现细节

### 函数：`selectNodesByCumulativeProbability`

```go
func (sched *Scheduler) selectNodesByCumulativeProbability(
    candidateNodes []CandidateNode,
    maxCandidates int,
    pod *v1.Pod,
) []CandidateNode
```

**参数：**
- `candidateNodes`: 候选节点列表（已按得分排序）
- `maxCandidates`: 最多返回的候选节点数量（默认3）
- `pod`: 待调度的 Pod

**返回值：**
- 选中的候选节点列表（最多3个，主节点在第一位）

**算法流程：**

1. 限制候选节点数量为 `maxCandidates`
2. 生成随机数 `randomValue` (0-1)
3. 遍历候选节点，计算累积概率
4. 找到第一个累积概率大于随机数的节点作为主节点
5. 如果没有节点被选中，fallback 到第一个节点
6. 构建返回列表：主节点 + 其他节点（最多3个）

### 示例

假设有三个节点，采纳概率分别为：
- 节点A: p1 = 0.8
- 节点B: p2 = 0.6
- 节点C: p3 = 0.4

**累积概率计算：**
- P1 = 0.8
- P2 = 0.8 + (1-0.8) × 0.6 = 0.8 + 0.12 = 0.92
- P3 = 0.92 + (1-0.8) × (1-0.6) × 0.4 = 0.92 + 0.032 = 0.952

**选择结果：**
- 如果 `r < 0.8`：选择节点A，返回 [A, B, C]
- 如果 `0.8 ≤ r < 0.92`：选择节点B，返回 [B, A, C]
- 如果 `0.92 ≤ r < 0.952`：选择节点C，返回 [C, A, B]
- 如果 `r ≥ 0.952`：fallback 到节点A，返回 [A, B, C]

## 代码位置

### 主要修改

1. **`schedulePod` 函数** (`pkg/scheduler/schedule_one.go:617-636`)
   - 计算每个候选节点的采纳概率
   - 调用 `selectNodesByCumulativeProbability` 选择节点
   - 确定主节点和备选节点列表

2. **`selectNodesByCumulativeProbability` 函数** (`pkg/scheduler/schedule_one.go:1356-1450`)
   - 实现累积概率选择算法
   - 返回最多3个候选节点

## 优势

1. **概率分布合理**：高采纳概率的节点有更高的被选中概率
2. **备选机制**：保留多个备选节点，提高容错能力
3. **Fallback 机制**：确保总是有节点可选
4. **可配置**：`maxCandidates` 参数可以调整备选节点数量

## 日志输出

函数会输出详细的日志信息，包括：
- 随机数 `randomValue`
- 每个节点的采纳概率和累积概率
- 选中的节点索引
- 最终的候选节点列表

日志级别：
- `V(4)`: 选中节点和最终结果
- `V(5)`: 详细的概率计算过程

## 使用示例

```go
// 在 schedulePod 中调用
const maxSelectedCandidates = 3
selectedCandidates := sched.selectNodesByCumulativeProbability(
    candidateNodes, 
    maxSelectedCandidates, 
    pod)

// selectedCandidates[0] 是主节点
// selectedCandidates[1:] 是备选节点
```

## 注意事项

1. **候选节点必须已按得分排序**：函数假设输入列表已按得分从高到低排序
2. **采纳概率范围**：采纳概率应该在 [0, 1] 范围内
3. **节点数量限制**：最多返回 `maxCandidates` 个节点（默认3个）
4. **随机数生成**：使用 `rand.Float64()` 生成随机数，确保在 [0, 1) 范围内

