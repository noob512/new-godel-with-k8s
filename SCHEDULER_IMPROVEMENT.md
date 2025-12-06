# Kubernetes 调度器改进方案

## 需求概述

传统的 Kubernetes 调度器在选择节点时只会选择得分最高的节点使用。本方案实现了以下改进：

1. **多候选节点保留**：保留多个候选节点（按得分从高到低排序），当最高得分节点失效时可以直接使用第二高节点
2. **采纳概率计算**：结合节点得分、历史成功率和新鲜度计算采纳概率，采纳概率越高，则直接占用节点（以免其它pod抢占）的概率越高

## 实现方案

### 1. 节点历史统计管理器

**文件**: `pkg/scheduler/internal/nodehistory/node_history.go`

实现了 `NodeHistoryManager` 来跟踪每个节点的历史统计信息：

- **成功次数** (SuccessCount)：成功调度到该节点的次数
- **冲突次数** (ConflictCount)：调度冲突的次数（如节点失效、资源不足等）
- **最后同步时间** (LastSyncTime)：最后一次同步时间

关键方法：
- `RecordSuccess(nodeName string)`: 记录节点调度成功
- `RecordConflict(nodeName string)`: 记录节点调度冲突
- `GetSuccessRate(nodeName string)`: 计算成功率
- `GetStaleness(nodeName string)`: 获取新鲜度（自上次同步的秒数）
- `CalculateAdoptionProbability(nodeName string, score int64)`: 计算采纳概率

### 2. 采纳概率计算公式

```
successRate = 成功次数 / (成功+冲突+1)
staleness = 自上次同步的秒数
AdoptionProbability = Score × successRate × exp(-staleness/100)
```

其中：
- `Score` 是节点的调度得分（归一化到 0-1 范围，MaxNodeScore = 100）
- `successRate` 是历史成功率
- `exp(-staleness/100)` 是新鲜度衰减因子，时间越久衰减越大

### 3. 多候选节点选择

**文件**: `pkg/scheduler/schedule_one.go`

新增 `selectCandidateNodes` 函数，选择多个候选节点（默认最多5个），按得分从高到低排序。

修改 `schedulePod` 函数：
1. 选择多个候选节点
2. 计算每个候选节点的采纳概率
3. 根据采纳概率筛选候选节点：
   - 对于每个候选节点，如果采纳概率 > 0.5，则以该概率决定是否保留（例如0.8的概率直接占用）
   - 如果采纳概率 <= 0.5，则不保留该节点
   - 如果筛选后没有候选节点，fallback到第一个节点（确保调度可以继续）

### 4. 节点失效处理

在绑定阶段，如果首选节点绑定失败：
1. 记录冲突统计
2. 自动尝试候选节点列表中的下一个节点
3. 对每个候选节点重新执行 Assume、Reserve、Bind 流程
4. 如果所有候选节点都失败，则重新调度

### 5. 统计信息更新

在以下场景更新节点统计信息：
- **绑定成功**：调用 `RecordSuccess`
- **绑定失败**：调用 `RecordConflict`
- **Reserve 失败**：调用 `RecordConflict`
- **Permit 失败**：调用 `RecordConflict`
- **Assume 失败**：调用 `RecordConflict`
- **PreBind 失败**：调用 `RecordConflict`
- **WaitOnPermit 失败**：调用 `RecordConflict`
- **节点删除**：调用 `RemoveNode` 清理统计信息

### 6. 数据结构修改

**文件**: `pkg/scheduler/scheduler.go`

修改 `ScheduleResult` 结构，新增 `CandidateNodes` 字段：

```go
type ScheduleResult struct {
    SuggestedHost   string
    CandidateNodes  []CandidateNode  // 新增：候选节点列表
    EvaluatedNodes  int
    FeasibleNodes   int
}

type CandidateNode struct {
    Name                string
    Score               int64
    AdoptionProbability float64
}
```

在 `Scheduler` 结构体中新增 `nodeHistoryManager` 字段。

## 代码修改清单

### 新增文件
1. `pkg/scheduler/internal/nodehistory/node_history.go` - 节点历史统计管理器
2. `pkg/scheduler/internal/nodehistory/README.md` - 功能说明文档

### 修改文件
1. `pkg/scheduler/scheduler.go`
   - 添加 `nodeHistoryManager` 字段
   - 修改 `ScheduleResult` 结构，新增 `CandidateNodes` 字段
   - 在 `newScheduler` 中初始化 `nodeHistoryManager`

2. `pkg/scheduler/schedule_one.go`
   - 新增 `selectCandidateNodes` 函数
   - 修改 `schedulePod` 函数，实现多候选节点选择和采纳概率计算
   - 修改绑定逻辑，支持候选节点自动切换
   - 在所有失败场景添加统计信息更新

3. `pkg/scheduler/eventhandlers.go`
   - 在 `deleteNodeFromCache` 中添加节点统计信息清理

## 配置参数

当前实现中的可配置参数（可在代码中调整）：

- **maxCandidates**：最多保留的候选节点数量（默认：5）
- **adoptionThreshold**：采纳概率阈值（默认：0.5），超过此值则直接占用节点

## 性能考虑

1. **内存开销**：每个节点的统计信息占用少量内存（约几十字节），对于大规模集群影响较小
2. **计算开销**：采纳概率计算是 O(1) 操作，对调度性能影响可忽略
3. **并发安全**：使用读写锁保护统计信息的并发访问

## 使用示例

调度器会自动使用新功能，无需额外配置。当调度一个 Pod 时：

1. 调度器会选择多个候选节点（最多5个）
2. 计算每个节点的采纳概率
3. 如果采纳概率高（>0.5），直接占用首选节点
4. 如果采纳概率低，保留多个候选节点
5. 如果首选节点绑定失败，自动尝试下一个候选节点

## 日志输出

调度器会输出以下日志（可通过日志级别控制）：

- `V(4)`: 候选节点选择、采纳概率计算、节点切换尝试
- `V(5)`: 详细的采纳概率计算过程

示例日志：
```
I0401 10:00:00.000000       1 schedule_one.go:XXX] High adoption probability, adopting node immediately pod="default/test-pod" node="node-1" adoptionProbability=0.75
I0401 10:00:01.000000       1 schedule_one.go:XXX] Binding failed for primary node, trying candidate nodes pod="default/test-pod" primaryNode="node-1" error="..."
I0401 10:00:02.000000       1 schedule_one.go:XXX] Successfully bound pod to candidate node pod="default/test-pod" node="node-2" candidateIndex=1
```

## 未来改进方向

1. 将配置参数提取到调度器配置中，支持动态配置
2. 支持更灵活的采纳概率阈值配置（可按节点类型、Pod 类型等）
3. 添加监控指标，跟踪采纳概率和候选节点使用情况
4. 支持节点统计信息的持久化存储（如存储到 etcd）
5. 支持统计信息的定期清理（如清理长时间未使用的节点统计）

## 测试建议

1. **单元测试**：测试 `NodeHistoryManager` 的各个方法
2. **集成测试**：测试多候选节点选择和切换逻辑
3. **压力测试**：测试大规模集群下的性能影响
4. **故障测试**：测试节点失效时的自动切换功能


