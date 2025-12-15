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

### 5. 分区同步（Partition Sync）

将节点分成多个分区，不同调度器可以同步不同分区的状态：

| 同步模式 | 描述 |
|---------|------|
| `globSync` | 全局同步：每次同步整个集群状态（默认） |
| `sameSync` | 相同分区同步：所有调度器同步相同分区 |
| `diffSync` | 差异分区同步：不同调度器同步不同分区（性能最佳） |

**同步间隔控制（syncGap）**：
- 与 sim.ipynb 一致，只有距离上次同步超过 `syncGap` 时间才会触发同步
- 默认同步间隔为 1 秒
- 可通过 `WithSyncGap()` 配置

**diffSync 工作流程**：
1. 每个调度器的 `partition_index` 从 `scheduler_index % num_partition` 开始
2. 每次同步时，先同步当前 `partition_index` 指向的分区
3. 同步后，`partition_index` 递增并轮换到下一个分区
4. 不同调度器在同一时刻会同步不同的分区

### 6. 调度策略（Schedule Strategy）

| 策略 | 描述 |
|------|------|
| `quality` | 质量优先：从所有节点中选择得分最高的节点（默认） |
| `latency` | 延迟优先：优先从最新鲜的分区选择节点 |

## 二、代码结构

### 1. NodeHistoryManager (`pkg/scheduler/internal/nodehistory/node_history.go`)

节点历史统计管理器，负责：
- 记录节点的成功/冲突次数
- 计算成功率和新鲜度
- 计算采纳概率
- 管理接收率统计
- 提供更新概率计算
- **分区管理和同步**
- **分区新鲜度追踪**

```go
// 核心结构
type NodeHistoryManager struct {
    nodeStats          map[string]*NodeStats
    acceptFreq         *AcceptanceFrequency
    numBackup          int
    updateStrategy     UpdateStrategy
    
    // 分区同步相关
    syncMode           SyncMode          // 同步模式
    scheduleStrategy   ScheduleStrategy  // 调度策略
    syncGap            time.Duration     // 同步间隔时间（与 sim.ipynb 中的 sync_gap 对应）
    lastSyncTime       time.Time         // 上次同步时间（与 sim.ipynb 中的 last_sync 对应）
    lastSyncedPartition int              // 上次同步的分区索引
    numPartitions      int               // 分区数量
    currentPartitionIndex int            // 当前同步分区索引
    schedulerIndex     int               // 调度器索引
    partitionSyncTimes []time.Time       // 分区同步时间
    nodeToPartition    map[string]int    // 节点到分区的映射
}

// 主要方法
func (m *NodeHistoryManager) RecordSuccess(nodeName string)
func (m *NodeHistoryManager) RecordConflict(nodeName string)
func (m *NodeHistoryManager) CalculateAdoptionProbability(nodeName string, score int64) float64
func (m *NodeHistoryManager) RecordAcceptance(candidateIndex int)
func (m *NodeHistoryManager) RecordRejection()
func (m *NodeHistoryManager) GetUpdateProbability(backupIndex int, nodeScore int64) float64

// 分区同步相关方法
func (m *NodeHistoryManager) AssignNodeToPartition(nodeName string) int
func (m *NodeHistoryManager) GetNodePartition(nodeName string) int
func (m *NodeHistoryManager) SyncPartition() SyncPartitionResult  // 返回同步结果
func (m *NodeHistoryManager) GetPartitionStaleness() []PartitionStaleness
func (m *NodeHistoryManager) SortNodesByPartitionFreshness(nodes []string) []string
func (m *NodeHistoryManager) IsLatencyFirst() bool
func (m *NodeHistoryManager) GetLastSyncedPartition() int
func (m *NodeHistoryManager) GetSyncGap() time.Duration
func (m *NodeHistoryManager) SetSyncGap(gap time.Duration)

// SyncPartitionResult 同步分区的结果
type SyncPartitionResult struct {
    Synced           bool   // 是否进行了同步
    SyncedPartitions []int  // 同步的分区列表
    IsFullSync       bool   // 是否是全量同步
}
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
    
    // 分区同步配置
    syncMode         nodehistory.SyncMode         // 同步模式（默认 "globSync"）
    scheduleStrategy nodehistory.ScheduleStrategy // 调度策略（默认 "quality"）
    numPartitions    int                          // 分区数量（默认 1）
    schedulerIndex   int                          // 调度器索引（默认 0）
    syncGap          time.Duration                // 同步间隔（默认 1 秒）
}

// Option 函数
func WithNumBackupNodes(n int) Option
func WithBackupUpdateStrategy(strategy nodehistory.UpdateStrategy) Option
func WithEnableSecondaryReserve(enable bool) Option
func WithSyncMode(mode nodehistory.SyncMode) Option
func WithScheduleStrategy(strategy nodehistory.ScheduleStrategy) Option
func WithNumPartitions(n int) Option
func WithSchedulerIndex(index int) Option
func WithSyncGap(gap time.Duration) Option  // 设置同步间隔
```

### 3. 缓存分区同步 (`pkg/scheduler/internal/cache/`)

#### Snapshot 分区结构
```go
type Snapshot struct {
    // ... 原有字段 ...
    
    // 分区同步相关字段
    numPartitions        int                      // 分区数量
    nodeToPartition      map[string]int           // 节点到分区的映射
    partitionGenerations []int64                  // 每个分区的 generation
    partitionNodeLists   [][]*framework.NodeInfo  // 每个分区的节点列表
}

// 分区相关方法
func (s *Snapshot) InitPartitions(numPartitions int)
func (s *Snapshot) AssignNodeToPartition(nodeName string) int
func (s *Snapshot) GetNodePartition(nodeName string) int
func (s *Snapshot) GetPartitionGeneration(partitionID int) int64
func (s *Snapshot) SetPartitionGeneration(partitionID int, generation int64)
func (s *Snapshot) RebuildPartitionNodeLists()
func (s *Snapshot) IsPartitioned() bool
```

#### Cache 分区同步方法
```go
// UpdateSnapshotPartitioned 分区同步更新快照
// 只更新指定分区的节点信息，其他分区保持不变
func (cache *cacheImpl) UpdateSnapshotPartitioned(
    nodeSnapshot *Snapshot,
    partitionIDs []int,
    nodeToPartitionFunc func(string) int,
) error
```

### 4. 调度流程 (`pkg/scheduler/schedule_one.go`)

#### 分区同步
```go
// 在每次调度周期开始时触发分区同步
sched.nodeHistoryManager.SyncPartition()

// 根据同步模式选择全量更新或分区更新
if err := sched.updateSnapshotWithPartitionSync(); err != nil {
    return result, err
}
```

#### updateSnapshotWithPartitionSync 方法
```go
// 根据同步模式更新快照
// - 全局同步模式：使用全量更新
// - 分区同步模式：只更新特定分区的节点信息
func (sched *Scheduler) updateSnapshotWithPartitionSync() error
```

#### 候选节点选择
```go
// 选择多个候选节点（使用配置的备选节点数量）
maxCandidates := sched.nodeHistoryManager.GetNumBackup() + 1
candidateNodes, err := selectCandidateNodes(priorityList, maxCandidates)

// 延迟优先策略：按分区新鲜度重新排序候选节点
if sched.nodeHistoryManager.IsLatencyFirst() && len(candidateNodes) > 1 {
    candidateNodes = sched.sortCandidatesByPartitionFreshness(candidateNodes, pod)
}

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

### 1. 基本配置（备选调度）

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

### 2. 启用分区同步（推荐用于多调度器场景）

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
    // 备选调度配置
    scheduler.WithNumBackupNodes(3),
    scheduler.WithBackupUpdateStrategy(nodehistory.UpdateStrategyProbability),
    scheduler.WithEnableSecondaryReserve(true),
    // 分区同步配置
    scheduler.WithSyncMode(nodehistory.SyncModeDiff),        // 差异分区同步
    scheduler.WithScheduleStrategy(nodehistory.ScheduleStrategyLatency), // 延迟优先
    scheduler.WithNumPartitions(10),                         // 10 个分区
    scheduler.WithSchedulerIndex(schedulerID),               // 调度器索引
)
```

### 3. 获取统计信息

```go
// 获取统计摘要
summary := sched.nodeHistoryManager.GetStatsSummary()

// 获取特定节点的成功率
successRate := sched.nodeHistoryManager.GetSuccessRate(nodeName)

// 获取接收频率
acceptFreq := sched.nodeHistoryManager.GetAcceptFrequencyCopy()

// 获取分区新鲜度
partitionStaleness := sched.nodeHistoryManager.GetPartitionStaleness()
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
| `par_sync` | `SyncMode` (sameSync/diffSync) |
| `diff_sync` | `SyncModeDiff` |
| `schedule_strategy` | `ScheduleStrategy` (quality/latency) |
| `partition_sync_time` | `partitionSyncTimes` |
| `get_partition_staleness` | `GetPartitionStaleness()` |
| `_select_slots_latency_first` | `sortCandidatesByPartitionFreshness()` |

## 五、效果预期

### 备选调度效果

根据 sim.ipynb 的模拟结果：

| 配置 | 冲突率 | 完成时间 |
|-----|--------|---------|
| 无备选 (B=0) | 22.34% | 37.0s |
| 1个备选 (B=1) | 13.2% | 34.1s |
| 2个备选 (B=2) | 10.6% | 33.3s |

### 分区同步效果

当分区数量 P=30，同步时间间隔 G=3s 时：

| 策略组合 | 完成时间 |
|---------|---------|
| `diffSync + latency` | 31.8s（最优） |
| `diffSync + quality` | 49.6s |
| `sameSync + latency` | 50.8s |
| `globSync` (基线) | 51.1s |
| `sameSync + quality` | 53.0s |

**结论：`diffSync + latency` 组合是唯一稳定优于基线的方案。**

## 六、算法详解

### 1. 分区同步算法

与 sim.ipynb 中的 `sync_global()` 完全对应：

```
在每次调度周期开始时调用 SyncPartition()：
1. 检查同步间隔：if (current_time - last_sync < sync_gap) return
   - 与 sim.ipynb 中的 `if current_time - self.last_sync >= self.sync_gap` 对应
   
2. 根据 syncMode 决定同步哪些分区：
   - globSync: 同步所有分区的时间戳
   - sameSync: 同步当前分区的时间戳，然后递增 partition_index
   - diffSync: 先同步当前 partition_index 指向的分区，然后递增 partition_index
     （与 sim.ipynb 一致：先同步再递增）

3. 返回 SyncPartitionResult：
   - Synced: 是否进行了同步
   - SyncedPartitions: 刚刚同步的分区列表
   - IsFullSync: 是否是全量同步
```

### 2. 延迟优先调度算法

```
1. 计算每个分区的新鲜度（staleness = 当前时间 - 最后同步时间）
2. 按新鲜度排序分区（staleness 越小越优先）
3. 对候选节点计算组合分数：
   combinedScore = originalIndex + partitionRank × weightFactor
4. 按组合分数重新排序候选节点
```

### 3. 分区分配算法

```
使用节点名称的哈希值来确定分区：
partitionID = hash(nodeName) % numPartitions

这确保：
- 相同的节点总是分配到相同的分区
- 节点在分区间均匀分布
```

### 4. 缓存分区同步算法

```
在 updateSnapshotWithPartitionSync(syncResult) 中：
1. 检查同步模式：
   - globSync: 使用传统的全量 UpdateSnapshot()
   - sameSync/diffSync: 使用分区同步

2. 检查 SyncPartition() 返回的结果：
   - 如果 syncResult.Synced == false：距离上次同步时间不够，跳过更新
   - 如果 syncResult.IsFullSync == true：执行全量更新

3. 分区同步流程：
   a. 初始化快照分区信息（如果尚未初始化）
   b. 使用 syncResult.SyncedPartitions 作为需要更新的分区列表
      （直接使用 SyncPartition() 返回的分区，确保一致性）
   c. 调用 UpdateSnapshotPartitioned() 只更新指定分区的节点

4. UpdateSnapshotPartitioned() 内部：
   a. 遍历缓存中的所有节点
   b. 对每个节点：
      - 使用 nodeToPartitionFunc 确定节点所属分区
      - 如果节点不属于需要更新的分区，跳过
      - 否则检查 generation 并更新节点信息
   c. 更新指定分区的 generation
   d. 重建分区节点列表
```

### 5. 性能优势

分区同步的性能优势：
- **减少同步开销**：每次只同步一部分节点，减少锁竞争和内存复制
- **提高并发性**：不同调度器同步不同分区，减少资源争用
- **更快响应**：延迟优先策略优先使用最新信息，提高决策质量

## 七、YAML 配置示例

### 1. KubeSchedulerConfiguration YAML 配置

新增的配置字段可以在 `KubeSchedulerConfiguration` 中设置：

```yaml
apiVersion: kubescheduler.config.k8s.io/v1beta3
kind: KubeSchedulerConfiguration
leaderElection:
  leaderElect: true
clientConnection:
  kubeconfig: /etc/kubernetes/scheduler.conf
profiles:
  - schedulerName: default-scheduler

# ========== 备选调度配置 ==========
# 备选节点数量（默认 3）
numBackupNodes: 3

# 本地状态更新策略：first, all, p, p-slot（默认 p）
backupUpdateStrategy: "p"

# 是否启用次优节点概率预留（默认 true）
enableSecondaryReserve: true

# ========== 分区同步配置 ==========
# 同步模式：globSync, sameSync, diffSync（默认 globSync）
syncMode: "diffSync"

# 调度策略：quality, latency（默认 quality）
scheduleStrategy: "latency"

# 分区数量（默认 1，即不分区）
numPartitions: 10

# 调度器索引（用于 diffSync 模式，不同调度器应设置不同值）
schedulerIndex: 0

# 同步间隔时间（秒），默认 1
syncGapSeconds: 1
```

### 2. 多调度器部署示例

在多调度器环境中，每个调度器需要设置不同的 `schedulerIndex`：

**调度器 1 配置 (`scheduler-1.yaml`)：**
```yaml
apiVersion: kubescheduler.config.k8s.io/v1beta3
kind: KubeSchedulerConfiguration
profiles:
  - schedulerName: multi-scheduler

syncMode: "diffSync"
scheduleStrategy: "latency"
numPartitions: 10
schedulerIndex: 0  # 调度器 1
syncGapSeconds: 1
```

**调度器 2 配置 (`scheduler-2.yaml`)：**
```yaml
apiVersion: kubescheduler.config.k8s.io/v1beta3
kind: KubeSchedulerConfiguration
profiles:
  - schedulerName: multi-scheduler

syncMode: "diffSync"
scheduleStrategy: "latency"
numPartitions: 10
schedulerIndex: 1  # 调度器 2
syncGapSeconds: 1
```

**调度器 3 配置 (`scheduler-3.yaml`)：**
```yaml
apiVersion: kubescheduler.config.k8s.io/v1beta3
kind: KubeSchedulerConfiguration
profiles:
  - schedulerName: multi-scheduler

syncMode: "diffSync"
scheduleStrategy: "latency"
numPartitions: 10
schedulerIndex: 2  # 调度器 3
syncGapSeconds: 1
```

### 3. Kubernetes Deployment 示例

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: custom-scheduler
  namespace: kube-system
spec:
  replicas: 3
  selector:
    matchLabels:
      component: custom-scheduler
  template:
    metadata:
      labels:
        component: custom-scheduler
    spec:
      containers:
      - name: scheduler
        image: your-scheduler-image:latest
        command:
        - kube-scheduler
        - --config=/etc/kubernetes/scheduler-config.yaml
        - --v=4
        volumeMounts:
        - name: config
          mountPath: /etc/kubernetes/scheduler-config.yaml
          subPath: scheduler-config.yaml
        env:
        # 使用环境变量来设置调度器索引
        - name: SCHEDULER_INDEX
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
      volumes:
      - name: config
        configMap:
          name: scheduler-config
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: scheduler-config
  namespace: kube-system
data:
  scheduler-config.yaml: |
    apiVersion: kubescheduler.config.k8s.io/v1beta3
    kind: KubeSchedulerConfiguration
    profiles:
      - schedulerName: custom-scheduler
    
    # 备选调度配置
    numBackupNodes: 3
    backupUpdateStrategy: "p"
    enableSecondaryReserve: true
    
    # 分区同步配置
    syncMode: "diffSync"
    scheduleStrategy: "latency"
    numPartitions: 10
    # 注意：schedulerIndex 需要在启动时根据 Pod 名称动态设置
    schedulerIndex: 0
    syncGapSeconds: 1
```

### 4. 配置说明

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `numBackupNodes` | int32 | 3 | 备选节点数量 |
| `backupUpdateStrategy` | string | "p" | 本地状态更新策略 |
| `enableSecondaryReserve` | bool | true | 是否启用次优节点预留 |
| `syncMode` | string | "globSync" | 同步模式 |
| `scheduleStrategy` | string | "quality" | 调度策略 |
| `numPartitions` | int32 | 1 | 分区数量 |
| `schedulerIndex` | int32 | 0 | 调度器索引 |
| `syncGapSeconds` | int64 | 1 | 同步间隔（秒）|

## 八、后续优化方向

1. ~~**分区同步 (Partition Sync)**：类似 sim.ipynb 中的 `diffSync`，不同调度器同步不同分区的状态~~ ✅ 已实现
2. ~~**延迟优先调度 (Latency-first)**：优先从最新鲜的分区选择节点~~ ✅ 已实现
3. ~~**缓存分区同步**：修改 UpdateSnapshot 实现真正的分区同步~~ ✅ 已实现
4. ~~**YAML 配置支持**：通过 YAML 文件配置所有新增选项~~ ✅ 已实现
5. **动态调整备选数量**：根据冲突率动态调整备选节点数量
6. **统计信息持久化**：将统计信息持久化，避免重启丢失
7. **自适应分区数量**：根据集群规模自动调整分区数量
8. **分区同步间隔自适应**：根据负载动态调整同步间隔
