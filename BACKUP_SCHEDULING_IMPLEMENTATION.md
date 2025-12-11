# 备选调度方案完整实现

基于 sim.ipynb 中的思想，本项目实现了完整的备选调度方案，用于在多调度器并行环境下减少调度冲突。

## 一、核心思想

### 1. 备选调度方案（Backup Scheduling）

每个调度器在调度时除了选择得分最高的节点外，同时保留 B 个得分次高的节点作为备选。当首选节点发生调度冲突时，可以直接使用备选节点，而无需重新执行完整的调度流程。

### 2. 采纳概率（Adoption Probability）

综合考虑节点的调度分数、历史成功率和信息新鲜度：

```
AdoptionProbability = Score × successRate × exp(-staleness/100)
```

其中：
- `Score`: 节点的归一化评分 (0~1)
- `successRate = 成功次数 / (成功次数 + 冲突次数 + 1)`
- `staleness`: 自上次同步的秒数

### 3. 接收率统计（Acceptance Frequency）

记录各级候选节点的接收频率：
- `accept_freq[0]`: 首选节点接收次数
- `accept_freq[1~N]`: 第 1~N 个备选节点接收次数  
- `accept_freq[N+1]`: 所有节点都被拒绝的次数

### 4. 本地状态更新策略

| 策略 | 描述 |
|------|------|
| `first` | 只更新首选节点的本地状态 |
| `all` | 更新所有备选节点的本地状态（悲观） |
| `p` | 按照历史接收率概率更新（默认） |
| `p-slot` | 结合资源槽得分进行概率加权更新 |

## 二、代码结构

### 1. NodeHistoryManager (`pkg/scheduler/internal/nodehistory/node_history.go`)

节点历史统计管理器，负责：
- 记录节点的成功/冲突次数
- 计算成功率和新鲜度
- 计算采纳概率
- 管理接收率统计
- 提供更新概率计算

```go
// 核心结构
type NodeHistoryManager struct {
    nodeStats      map[string]*NodeStats
    acceptFreq     *AcceptanceFrequency
    numBackup      int
    updateStrategy UpdateStrategy
}

// 主要方法
func (m *NodeHistoryManager) RecordSuccess(nodeName string)
func (m *NodeHistoryManager) RecordConflict(nodeName string)
func (m *NodeHistoryManager) CalculateAdoptionProbability(nodeName string, score int64) float64
func (m *NodeHistoryManager) RecordAcceptance(candidateIndex int)
func (m *NodeHistoryManager) RecordRejection()
func (m *NodeHistoryManager) GetUpdateProbability(backupIndex int, nodeScore int64) float64
```

### 2. 配置选项 (`pkg/scheduler/scheduler.go`)

```go
// 调度器选项
type schedulerOptions struct {
    // ... 其他选项 ...
    
    // 备选调度配置
    numBackupNodes         int                        // 备选节点数量（默认 3）
    backupUpdateStrategy   nodehistory.UpdateStrategy // 更新策略（默认 "p"）
    enableSecondaryReserve bool                       // 是否启用次优节点预留（默认 true）
}

// Option 函数
func WithNumBackupNodes(n int) Option
func WithBackupUpdateStrategy(strategy nodehistory.UpdateStrategy) Option
func WithEnableSecondaryReserve(enable bool) Option
```

### 3. 调度流程 (`pkg/scheduler/schedule_one.go`)

#### 候选节点选择
```go
// 选择多个候选节点（使用配置的备选节点数量）
maxCandidates := sched.nodeHistoryManager.GetNumBackup() + 1
candidateNodes, err := selectCandidateNodes(priorityList, maxCandidates)

// 计算每个候选节点的采纳概率
for i := range candidateNodes {
    candidateNodes[i].AdoptionProbability = sched.nodeHistoryManager.CalculateAdoptionProbability(
        candidateNodes[i].Name,
        candidateNodes[i].Score,
    )
}
```

#### 概率性次优节点预留
```go
// 以 (1-p1)*p2 的概率为次优节点预留资源
if sched.enableSecondaryReserve && len(scheduleResult.CandidateNodes) >= 2 {
    shouldReserveSecondary := sched.shouldReserveSecondaryNode(...)
    if shouldReserveSecondary {
        // 为次优节点执行 Reserve
    }
}
```

#### 接收率统计更新
```go
// 绑定成功时记录接收
sched.nodeHistoryManager.RecordAcceptance(acceptedIndex)

// 所有候选节点都失败时记录拒绝
sched.nodeHistoryManager.RecordRejection()
```

## 三、使用方式

### 1. 创建调度器时配置

```go
sched, err := scheduler.New(
    godelSchedulerName,
    schedulerName,
    crdClient,
    crdInformerFactory,
    client,
    informerFactory,
    dynInformerFactory,
    recorderFactory,
    stopCh,
    // 配置备选调度选项
    scheduler.WithNumBackupNodes(3),                    // 保留 3 个备选节点
    scheduler.WithBackupUpdateStrategy(nodehistory.UpdateStrategyProbability),
    scheduler.WithEnableSecondaryReserve(true),
)
```

### 2. 获取统计信息

```go
// 获取统计摘要
summary := sched.nodeHistoryManager.GetStatsSummary()

// 获取特定节点的成功率
successRate := sched.nodeHistoryManager.GetSuccessRate(nodeName)

// 获取接收频率
acceptFreq := sched.nodeHistoryManager.GetAcceptFrequencyCopy()
```

## 四、与 sim.ipynb 的对应关系

| sim.ipynb 概念 | 本项目实现 |
|---------------|-----------|
| `num_backup` | `numBackupNodes` 配置选项 |
| `update_strategy` | `UpdateStrategy` 类型 |
| `accept_freq` | `AcceptanceFrequency` 结构 |
| `slot_scores` | 节点评分 (`Score`) |
| `sync_global` | 通过 Cache 和 Informer 实现 |
| `select_slots` | `selectCandidateNodes` 函数 |

## 五、效果预期

根据 sim.ipynb 的模拟结果：

| 配置 | 冲突率 | 完成时间 |
|-----|--------|---------|
| 无备选 (B=0) | 22.34% | 37.0s |
| 1个备选 (B=1) | 13.2% | 34.1s |
| 2个备选 (B=2) | 10.6% | 33.3s |

备选调度方案能够：
- 显著降低调度冲突率
- 缩短任务完成时间
- 在多调度器并行场景下更具优势

## 六、后续优化方向

1. **分区同步 (Partition Sync)**：类似 sim.ipynb 中的 `diffSync`，不同调度器同步不同分区的状态
2. **延迟优先调度 (Latency-first)**：优先从最新鲜的分区选择节点
3. **动态调整备选数量**：根据冲突率动态调整备选节点数量
4. **统计信息持久化**：将统计信息持久化，避免重启丢失

