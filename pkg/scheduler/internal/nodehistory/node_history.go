/*
Copyright 2024 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nodehistory

import (
	"math"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// UpdateStrategy 定义本地状态更新策略
type UpdateStrategy string

const (
	// UpdateStrategyFirst 只更新首选节点的本地状态
	UpdateStrategyFirst UpdateStrategy = "first"
	// UpdateStrategyAll 更新所有备选节点的本地状态（悲观）
	UpdateStrategyAll UpdateStrategy = "all"
	// UpdateStrategyProbability 按照历史接收率概率更新
	UpdateStrategyProbability UpdateStrategy = "p"
	// UpdateStrategyProbabilitySlot 结合资源槽得分进行概率加权更新
	UpdateStrategyProbabilitySlot UpdateStrategy = "p-slot"
)

// SyncMode 定义同步模式
type SyncMode string

const (
	// SyncModeGlobal 全局同步：每次同步整个集群状态
	SyncModeGlobal SyncMode = "globSync"
	// SyncModeSame 相同分区同步：所有调度器同步相同分区
	SyncModeSame SyncMode = "sameSync"
	// SyncModeDiff 差异分区同步：不同调度器同步不同分区（轮流）
	SyncModeDiff SyncMode = "diffSync"
)

// ScheduleStrategy 定义调度策略
type ScheduleStrategy string

const (
	// ScheduleStrategyQuality 质量优先：从所有节点中选择得分最高的节点
	ScheduleStrategyQuality ScheduleStrategy = "quality"
	// ScheduleStrategyLatency 延迟优先：优先从最新鲜的分区选择节点
	ScheduleStrategyLatency ScheduleStrategy = "latency"
)

// PartitionInfo 记录分区的同步信息
type PartitionInfo struct {
	// PartitionID 分区 ID
	PartitionID int
	// LastSyncTime 最后同步时间
	LastSyncTime time.Time
	// NodeNames 属于该分区的节点名称列表
	NodeNames []string
}

// PartitionStaleness 分区新鲜度信息
type PartitionStaleness struct {
	// PartitionID 分区 ID
	PartitionID int
	// Staleness 陈旧度（秒）
	Staleness float64
}

// AcceptanceFrequency 记录各级候选节点的接收频率
// 索引 0 表示首选节点接收次数
// 索引 1-N 表示第 1-N 个备选节点接收次数
// 最后一个索引表示所有节点都被拒绝的次数
type AcceptanceFrequency struct {
	// Frequencies 记录各级候选节点的接收频率
	// [primary_accept, backup1_accept, backup2_accept, ..., all_rejected]
	Frequencies []int64
	// mu 保护并发访问
	mu sync.RWMutex
}

// NodeStats 存储节点的历史统计信息
type NodeStats struct {
	// SuccessCount 成功调度到该节点的次数
	SuccessCount int64
	// ConflictCount 调度冲突的次数（如节点失效、资源不足等）
	ConflictCount int64
	// LastSyncTime 最后一次同步时间
	LastSyncTime time.Time
	// mu 保护并发访问
	mu sync.RWMutex
}

// NodeHistoryManager 管理所有节点的历史统计信息
type NodeHistoryManager struct {
	// nodeStats 存储每个节点的统计信息
	nodeStats map[string]*NodeStats
	// mu 保护并发访问
	mu sync.RWMutex
	// clock 用于获取当前时间（便于测试）
	clock func() time.Time
	// acceptFreq 记录各级候选节点的接收频率（全局统计）
	acceptFreq *AcceptanceFrequency
	// numBackup 备选节点数量
	numBackup int
	// updateStrategy 本地状态更新策略
	updateStrategy UpdateStrategy

	// ========== 分区同步相关字段 ==========
	// syncMode 同步模式（globSync, sameSync, diffSync）
	syncMode SyncMode
	// scheduleStrategy 调度策略（quality, latency）
	scheduleStrategy ScheduleStrategy
	// numPartitions 分区数量
	numPartitions int
	// currentPartitionIndex 当前同步的分区索引（用于 diffSync 模式）
	currentPartitionIndex int
	// schedulerIndex 调度器索引（用于 diffSync 模式，不同调度器从不同分区开始）
	schedulerIndex int
	// partitionSyncTimes 每个分区的最后同步时间
	partitionSyncTimes []time.Time
	// nodeToPartition 节点到分区的映射
	nodeToPartition map[string]int
	// partitionMu 保护分区相关字段的并发访问
	partitionMu sync.RWMutex

	// ========== 同步间隔控制相关字段 ==========
	// syncGap 同步间隔时间（秒），只有距离上次同步超过这个时间才会触发同步
	syncGap time.Duration
	// lastSyncTime 上次同步时间
	lastSyncTime time.Time
	// lastSyncedPartition 上次同步的分区索引（-1 表示全量同步或未同步）
	lastSyncedPartition int
}

// NewNodeHistoryManager 创建一个新的节点历史管理器
func NewNodeHistoryManager() *NodeHistoryManager {
	return NewNodeHistoryManagerWithConfig(3, UpdateStrategyProbability)
}

// DefaultSyncGap 默认同步间隔时间
const DefaultSyncGap = 1 * time.Second

// NewNodeHistoryManagerWithConfig 创建一个带配置的节点历史管理器
// numBackup: 备选节点数量
// updateStrategy: 本地状态更新策略
func NewNodeHistoryManagerWithConfig(numBackup int, updateStrategy UpdateStrategy) *NodeHistoryManager {
	return NewNodeHistoryManagerFull(numBackup, updateStrategy, SyncModeGlobal, ScheduleStrategyQuality, 1, 0, DefaultSyncGap)
}

// NewNodeHistoryManagerFull 创建一个完整配置的节点历史管理器
// numBackup: 备选节点数量
// updateStrategy: 本地状态更新策略
// syncMode: 同步模式
// scheduleStrategy: 调度策略
// numPartitions: 分区数量
// schedulerIndex: 调度器索引（用于 diffSync 模式）
// syncGap: 同步间隔时间
func NewNodeHistoryManagerFull(
	numBackup int,                 // 备选节点数量
	updateStrategy UpdateStrategy, // 本地状态更新策略
	syncMode SyncMode,             // 同步模式
	scheduleStrategy ScheduleStrategy, // 调度策略
	numPartitions int,             // 分区数量
	schedulerIndex int,            // 调度器索引（用于 diffSync 模式）
	syncGap time.Duration,         // 同步间隔时间
) *NodeHistoryManager {
	// 接收频率数组长度 = 1(首选) + numBackup(备选) + 1(全部拒绝)
	freqSize := numBackup + 2

	// 确保分区数量至少为 1
	if numPartitions < 1 {
		numPartitions = 1
	}

	// 确保同步间隔至少为 0
	if syncGap < 0 {
		syncGap = DefaultSyncGap
	}

	// 初始化分区同步时间
	partitionSyncTimes := make([]time.Time, numPartitions)
	now := time.Now()
	for i := range partitionSyncTimes {
		partitionSyncTimes[i] = now
	}

	return &NodeHistoryManager{
		nodeStats: make(map[string]*NodeStats), // 节点统计信息映射表
		clock:     time.Now,                    // 当前时间函数
		acceptFreq: &AcceptanceFrequency{       // 接收频率统计
			Frequencies: make([]int64, freqSize), // 频率数组，大小为备选节点数+2
		},
		numBackup:             numBackup,                       // 备选节点数量
		updateStrategy:        updateStrategy,                  // 本地状态更新策略
		syncMode:              syncMode,                        // 同步模式
		scheduleStrategy:      scheduleStrategy,                // 调度策略
		numPartitions:         numPartitions,                   // 分区数量
		currentPartitionIndex: schedulerIndex % numPartitions,  // 当前分区索引，通过调度器索引取模得到
		schedulerIndex:        schedulerIndex,                  // 调度器索引
		partitionSyncTimes:    partitionSyncTimes,              // 分区同步时间数组
		nodeToPartition:       make(map[string]int),            // 节点到分区的映射表
		syncGap:               syncGap,                         // 同步间隔时间
		lastSyncTime:          now,                             // 上次同步时间初始化为当前时间
		lastSyncedPartition:   -1,                              // -1 表示尚未同步
	}
}

// NewAcceptanceFrequency 创建一个新的接收频率统计器
func NewAcceptanceFrequency(numBackup int) *AcceptanceFrequency {
	// 数组大小 = 1(首选) + numBackup(备选) + 1(全部拒绝)
	return &AcceptanceFrequency{
		Frequencies: make([]int64, numBackup+2),
	}
}

// GetOrCreateStats 获取或创建节点的统计信息
// GetOrCreateStats 获取指定节点的统计信息对象。
// 如果该节点的统计信息对象不存在，则创建一个新的。
func (m *NodeHistoryManager) GetOrCreateStats(nodeName string) *NodeStats {
	// 获取 NodeHistoryManager 的互斥锁，以保证并发安全。
	// 在函数返回时释放锁。
	m.mu.Lock()
	defer m.mu.Unlock()

	// 尝试从 nodeStats 映射中获取指定节点的统计信息。
	stats, exists := m.nodeStats[nodeName]

	// 如果指定节点的统计信息不存在。
	if !exists {
		// 创建一个新的 NodeStats 结构体实例。
		stats = &NodeStats{
			// 初始化最后同步时间为当前时间。
			LastSyncTime: m.clock(),
		}
		// 将新创建的统计信息对象存入 nodeStats 映射中，键为节点名称。
		m.nodeStats[nodeName] = stats
	}

	// 返回节点的统计信息对象（无论是已存在的还是新创建的）。
	return stats
}

// GetStats 获取节点的统计信息（如果不存在则返回nil）
func (m *NodeHistoryManager) GetStats(nodeName string) *NodeStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nodeStats[nodeName]
}

// RecordSuccess 记录节点调度成功
func (m *NodeHistoryManager) RecordSuccess(nodeName string) {
	stats := m.GetOrCreateStats(nodeName)
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.SuccessCount++
	stats.LastSyncTime = m.clock()
	klog.V(5).InfoS("Recorded success for node", "node", nodeName, "successCount", stats.SuccessCount)
}

// RecordConflict 记录节点调度冲突
// RecordConflict 记录指定节点发生了一次调度冲突。
// 冲突通常发生在调度过程中，例如假设Pod失败、绑定失败或等待许可失败等情况。
func (m *NodeHistoryManager) RecordConflict(nodeName string) {
	// 获取或创建指定节点的统计信息对象。
	// 如果该节点的统计信息不存在，则创建一个新的。
	stats := m.GetOrCreateStats(nodeName)

	// 获取该节点统计信息对象的互斥锁，以保证并发安全。
	// 在函数返回时释放锁。
	stats.mu.Lock()
	defer stats.mu.Unlock()

	// 将该节点的冲突计数器加一。
	stats.ConflictCount++

	// 更新该节点最后一次同步（记录冲突）的时间戳。
	// 使用 NodeHistoryManager 的时钟获取当前时间。
	stats.LastSyncTime = m.clock()

	// 记录一条调试级别的日志，显示节点的冲突计数已更新。
	klog.InfoS("Recorded conflict for node", "node", nodeName, "conflictCount", stats.ConflictCount)
}

// UpdateSyncTime 更新节点的同步时间
func (m *NodeHistoryManager) UpdateSyncTime(nodeName string) {
	stats := m.GetOrCreateStats(nodeName)
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.LastSyncTime = m.clock()
}

// GetSuccessRate 计算节点的成功率
// successRate = 成功次数 / (成功+冲突+1)
func (m *NodeHistoryManager) GetSuccessRate(nodeName string) float64 {
	stats := m.GetStats(nodeName)
	if stats == nil {
		return 0.0
	}
	stats.mu.RLock()
	defer stats.mu.RUnlock()

	success := float64(stats.SuccessCount)
	conflict := float64(stats.ConflictCount)
	total := success + conflict + 1.0

	if total == 0 {
		return 0.0
	}
	return success / total
}

// GetStaleness 获取节点的新鲜度（自上次同步的秒数）
func (m *NodeHistoryManager) GetStaleness(nodeName string) float64 {
	stats := m.GetStats(nodeName)
	if stats == nil {
		return 0.0
	}
	stats.mu.RLock()
	defer stats.mu.RUnlock()

	now := m.clock()
	staleness := now.Sub(stats.LastSyncTime).Seconds()
	return staleness
}

// CalculateAdoptionProbability 计算节点的采纳概率。
// 采纳概率综合考虑了节点的调度分数、历史成功率和时间新鲜度。
// 公式为: AdoptionProbability = Score × successRate × exp(-staleness/100)
// 这个概率用于在调度决策中，对历史表现好、分数高且信息较新的节点给予更高的优先级。
func (m *NodeHistoryManager) CalculateAdoptionProbability(nodeName string, score int64) float64 {
	// 获取指定节点的历史成功调度率。
	// 成功率越高，节点被再次选中的概率也越大。
	successRate := m.GetSuccessRate(nodeName)

	// 获取指定节点的陈旧度（距离上次成功调度的时间）。
	// 陈旧度越高，表示节点信息越旧，其被选中的概率会相应降低。
	staleness := m.GetStaleness(nodeName)

	// 将原始调度分数归一化到 [0, 1] 范围内。
	// 假设调度分数的最大值为 MaxNodeScore (这里是 100)。
	// 归一化是为了让分数与其他 [0, 1] 范围的因子（如成功率）相乘时具有合理的权重。
	const MaxNodeScore = 100.0
	normalizedScore := float64(score) / MaxNodeScore

	// 计算基于时间陈旧度的新鲜度衰减因子。
	// 使用指数函数 exp(-staleness/100.0)，陈旧度越大，衰减因子越小（趋向于 0），
	// 这会降低采纳概率，从而惩罚那些长时间未成功调度的节点。
	freshnessFactor := math.Exp(-staleness / 100.0)

	// 综合归一化分数、成功率和新鲜度衰减因子，计算最终的采纳概率。
	// 这个概率值越高，表示该节点在当前调度决策中越有吸引力。
	adoptionProb := normalizedScore * successRate * freshnessFactor

	// 记录调试日志，输出计算采纳概率时用到的各项参数和最终结果。
	klog.V(5).InfoS("Calculated adoption probability",
		"node", nodeName, // 节点名称
		"score", score, // 原始调度分数
		"normalizedScore", normalizedScore, // 归一化后的调度分数
		"successRate", successRate, // 历史成功率
		"staleness", staleness, // 陈旧度
		"freshnessFactor", freshnessFactor, // 新鲜度衰减因子
		"adoptionProbability", adoptionProb) // 最终计算出的采纳概率

	// 返回计算得到的采纳概率。
	return adoptionProb
}

// RemoveNode 移除节点的统计信息（当节点被删除时调用）
func (m *NodeHistoryManager) RemoveNode(nodeName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodeStats, nodeName)
	klog.V(4).InfoS("Removed node stats", "node", nodeName)
}

// ========== 接收频率统计相关方法 ==========

// RecordAcceptance 记录节点被接收（调度成功）
// candidateIndex: 0 表示首选节点，1-N 表示第 1-N 个备选节点
func (m *NodeHistoryManager) RecordAcceptance(candidateIndex int) {
	if m.acceptFreq == nil {
		return
	}
	m.acceptFreq.mu.Lock()
	defer m.acceptFreq.mu.Unlock()

	// 确保索引在有效范围内
	if candidateIndex >= 0 && candidateIndex < len(m.acceptFreq.Frequencies)-1 {
		m.acceptFreq.Frequencies[candidateIndex]++
		klog.V(5).InfoS("Recorded acceptance",
			"candidateIndex", candidateIndex,
			"count", m.acceptFreq.Frequencies[candidateIndex])
	}
}

// RecordRejection 记录所有候选节点都被拒绝
func (m *NodeHistoryManager) RecordRejection() {
	if m.acceptFreq == nil {
		return
	}
	m.acceptFreq.mu.Lock()
	defer m.acceptFreq.mu.Unlock()

	// 最后一个索引表示全部拒绝
	lastIndex := len(m.acceptFreq.Frequencies) - 1
	m.acceptFreq.Frequencies[lastIndex]++
	klog.V(5).InfoS("Recorded rejection",
		"count", m.acceptFreq.Frequencies[lastIndex])
}

// GetAcceptanceRate 获取指定级别候选节点的接收率
// candidateIndex: 0 表示首选节点，1-N 表示第 1-N 个备选节点
// 返回值: 该级别候选节点的接收率（0.0 ~ 1.0）
func (m *NodeHistoryManager) GetAcceptanceRate(candidateIndex int) float64 {
	if m.acceptFreq == nil {
		return 0.0
	}
	m.acceptFreq.mu.RLock()
	defer m.acceptFreq.mu.RUnlock()

	// 计算总次数
	var total int64
	for _, freq := range m.acceptFreq.Frequencies {
		total += freq
	}

	if total == 0 || candidateIndex < 0 || candidateIndex >= len(m.acceptFreq.Frequencies) {
		return 0.0
	}

	return float64(m.acceptFreq.Frequencies[candidateIndex]) / float64(total)
}

// GetUpdateProbability 根据更新策略计算指定备选节点的更新概率
// backupIndex: 0 表示第一个备选节点，1 表示第二个备选节点，以此类推
// nodeScore: 节点评分（用于 p-slot 策略）
// 返回值: 该备选节点应该更新本地状态的概率（0.0 ~ 1.0）
func (m *NodeHistoryManager) GetUpdateProbability(backupIndex int, nodeScore int64) float64 {
	switch m.updateStrategy {
	case UpdateStrategyFirst:
		// 只更新首选节点，备选节点不更新
		return 0.0
	case UpdateStrategyAll:
		// 更新所有备选节点
		return 1.0
	case UpdateStrategyProbability, UpdateStrategyProbabilitySlot:
		// 按照历史接收率概率更新
		// 更新概率 = 1 - P(前面所有节点都被接收的概率)
		// 例如：backup0 的更新概率 = 1 - P(primary accepted) = P(primary rejected)
		if m.acceptFreq == nil {
			return 0.5 // 默认概率
		}

		m.acceptFreq.mu.RLock()
		defer m.acceptFreq.mu.RUnlock()

		var total int64
		for _, freq := range m.acceptFreq.Frequencies {
			total += freq
		}

		if total == 0 {
			return 0.5 // 默认概率
		}

		// 计算前面所有节点（包括首选节点和之前的备选节点）被接收的累积概率
		// candidateIndex = backupIndex + 1（因为 0 是首选节点）
		var successBefore int64
		for i := 0; i <= backupIndex; i++ {
			if i < len(m.acceptFreq.Frequencies) {
				successBefore += m.acceptFreq.Frequencies[i]
			}
		}

		// 更新概率 = 1 - (前面所有节点接收次数 / 总次数)
		pUpdate := 1.0 - float64(successBefore)/float64(total)

		// 对于 p-slot 策略，需要考虑节点评分
		if m.updateStrategy == UpdateStrategyProbabilitySlot && nodeScore > 0 {
			// 归一化评分并调整概率
			const MaxNodeScore = 100.0
			normalizedScore := float64(nodeScore) / MaxNodeScore
			if normalizedScore > 1.0 {
				normalizedScore = 1.0
			}
			// 评分越高，更新概率越高
			pUpdate = pUpdate * normalizedScore
		}

		return pUpdate
	default:
		return 0.5
	}
}

// GetAcceptFrequencyCopy 获取接收频率的副本（用于同步）
func (m *NodeHistoryManager) GetAcceptFrequencyCopy() []int64 {
	if m.acceptFreq == nil {
		return nil
	}
	m.acceptFreq.mu.RLock()
	defer m.acceptFreq.mu.RUnlock()

	copy := make([]int64, len(m.acceptFreq.Frequencies))
	for i, freq := range m.acceptFreq.Frequencies {
		copy[i] = freq
	}
	return copy
}

// SetAcceptFrequency 设置接收频率（用于同步）
func (m *NodeHistoryManager) SetAcceptFrequency(frequencies []int64) {
	if m.acceptFreq == nil || frequencies == nil {
		return
	}
	m.acceptFreq.mu.Lock()
	defer m.acceptFreq.mu.Unlock()

	// 确保数组长度匹配
	if len(frequencies) != len(m.acceptFreq.Frequencies) {
		klog.V(4).InfoS("Accept frequency length mismatch, resizing",
			"expected", len(m.acceptFreq.Frequencies),
			"got", len(frequencies))
		return
	}

	for i, freq := range frequencies {
		m.acceptFreq.Frequencies[i] = freq
	}
}

// GetNumBackup 获取备选节点数量
func (m *NodeHistoryManager) GetNumBackup() int {
	return m.numBackup
}

// GetUpdateStrategy 获取更新策略
func (m *NodeHistoryManager) GetUpdateStrategy() UpdateStrategy {
	return m.updateStrategy
}

// SetUpdateStrategy 设置更新策略
func (m *NodeHistoryManager) SetUpdateStrategy(strategy UpdateStrategy) {
	m.updateStrategy = strategy
}

// GetTotalAttempts 获取总调度尝试次数
func (m *NodeHistoryManager) GetTotalAttempts() int64 {
	if m.acceptFreq == nil {
		return 0
	}
	m.acceptFreq.mu.RLock()
	defer m.acceptFreq.mu.RUnlock()

	var total int64
	for _, freq := range m.acceptFreq.Frequencies {
		total += freq
	}
	return total
}

// GetStatsSummary 获取统计摘要（用于日志和监控）
func (m *NodeHistoryManager) GetStatsSummary() map[string]interface{} {
	summary := make(map[string]interface{})
	summary["numBackup"] = m.numBackup
	summary["updateStrategy"] = string(m.updateStrategy)
	summary["totalAttempts"] = m.GetTotalAttempts()
	summary["syncMode"] = string(m.syncMode)
	summary["scheduleStrategy"] = string(m.scheduleStrategy)
	summary["numPartitions"] = m.numPartitions

	if m.acceptFreq != nil {
		summary["acceptFrequencies"] = m.GetAcceptFrequencyCopy()
	}

	m.mu.RLock()
	summary["nodeCount"] = len(m.nodeStats)
	m.mu.RUnlock()

	return summary
}

// ========== 分区同步相关方法 ==========

// AssignNodeToPartition 将节点分配到分区
// 使用节点名称的哈希值来确定分区，确保相同的节点总是分配到相同的分区
// AssignNodeToPartition 将指定节点分配到一个分区
// 如果节点已存在分配关系，则直接返回现有分区ID；否则根据节点名计算并分配新的分区ID
func (m *NodeHistoryManager) AssignNodeToPartition(nodeName string) int {
	// 加锁以保证并发安全，防止多个goroutine同时修改共享数据
	m.partitionMu.Lock()
	defer m.partitionMu.Unlock()

	// 检查该节点是否已经被分配过分区
	// 如果存在分配记录，则直接返回已分配的分区ID，确保一致性
	if partitionID, exists := m.nodeToPartition[nodeName]; exists {
		return partitionID
	}

	// 使用简单的哈希函数将节点名称映射到分区ID
	// 通过累加节点名称中每个字符的ASCII值来计算哈希
	var hash int
	for _, c := range nodeName { // 遍历节点名称中的每一个字符
		hash += int(c) // 将字符转换为其对应的整数值（ASCII码），并累加到hash
	}
	// 使用模运算将哈希值映射到有效的分区范围内 [0, numPartitions-1]
	// 这确保了无论哈希值多大，最终的分区ID都在合法区间内
	partitionID := hash % m.numPartitions

	// 将节点名称与其计算出的分区ID建立映射关系并存储
	m.nodeToPartition[nodeName] = partitionID
	
	// 记录节点分配到分区的日志信息，便于调试和监控
	klog.V(5).InfoS("Assigned node to partition",
		"node", nodeName,           // 被分配的节点名称
		"partition", partitionID,   // 分配给该节点的分区ID
		"numPartitions", m.numPartitions) // 总分区数量

	// 返回分配的分区ID
	return partitionID
}

// GetNodePartition 获取节点所属的分区
func (m *NodeHistoryManager) GetNodePartition(nodeName string) int {
	m.partitionMu.RLock()
	if partitionID, exists := m.nodeToPartition[nodeName]; exists {
		m.partitionMu.RUnlock()
		return partitionID
	}
	m.partitionMu.RUnlock()

	// 如果节点还没有分配分区，则分配一个
	return m.AssignNodeToPartition(nodeName)
}

// SyncPartitionResult 同步分区的结果
type SyncPartitionResult struct {
	// Synced 是否进行了同步（如果距离上次同步时间小于 syncGap，则不同步）
	Synced bool
	// SyncedPartitions 同步的分区列表（-1 表示全量同步）
	SyncedPartitions []int
	// IsFullSync 是否是全量同步
	IsFullSync bool
}

// SyncPartition 同步指定分区的节点信息
// 在 diffSync 模式下，每次调用会轮换到下一个分区
// 返回同步结果，包含是否同步、同步的分区列表
func (m *NodeHistoryManager) SyncPartition() SyncPartitionResult {
	// 使用互斥锁保护共享资源，确保并发安全
	m.partitionMu.Lock()
	defer m.partitionMu.Unlock()

	// 获取当前时间戳
	now := m.clock()

	// 检查同步间隔：只有距离上次同步超过 syncGap 时间才进行同步
	// 这与 sim.ipynb 中的 `if current_time - self.last_sync >= self.sync_gap` 对应
	if m.syncGap > 0 && now.Sub(m.lastSyncTime) < m.syncGap {
		// 距离上次同步时间不够，不进行同步
		return SyncPartitionResult{
			Synced:           false,           // 本次未进行同步
			SyncedPartitions: nil,             // 未同步的分区列表为空
			IsFullSync:       false,           // 不是全量同步
		}
	}

	// 更新上次同步时间
	m.lastSyncTime = now

	// 根据不同的同步模式执行相应的同步逻辑
	switch m.syncMode {
	case SyncModeGlobal:
		// 全局同步模式：所有分区同时被同步
		// 遍历所有分区，将它们的同步时间都更新为当前时间
		for i := range m.partitionSyncTimes {
			m.partitionSyncTimes[i] = now
		}
		m.lastSyncedPartition = -1 // -1 表示全量同步
		klog.V(5).InfoS("Global sync completed",
			"numPartitions", m.numPartitions) // 记录分区总数
		return SyncPartitionResult{
			Synced:           true,            // 成功同步
			SyncedPartitions: nil,             // nil 表示所有分区都被同步
			IsFullSync:       true,            // 标记为全量同步
		}

	case SyncModeSame:
		// 相同分区同步模式：所有调度器同步同一个分区
		// 先记录当前要同步的分区
		syncedPartition := m.currentPartitionIndex
		if syncedPartition < len(m.partitionSyncTimes) {
			m.partitionSyncTimes[syncedPartition] = now
		}
		m.lastSyncedPartition = syncedPartition
		// 轮换到下一个分区
		m.currentPartitionIndex = (m.currentPartitionIndex) % m.numPartitions
		klog.V(5).InfoS("Same partition sync completed",
			"syncedPartition", syncedPartition,    // 本次同步的分区
			"nextPartition", m.currentPartitionIndex) // 下次将同步的分区
		return SyncPartitionResult{
			Synced:           true,                    // 成功同步
			SyncedPartitions: []int{syncedPartition},  // 同步的分区列表（单个分区）
			IsFullSync:       false,                   // 不是全量同步
		}

	case SyncModeDiff:
		// 差异分区同步模式：不同调度器同步不同分区
		// 每个调度器负责不同的分区，实现负载分散
		// 先记录当前要同步的分区（与 sim.ipynb 一致：先同步再递增）
		syncedPartition := m.currentPartitionIndex
		if syncedPartition < len(m.partitionSyncTimes) {
			m.partitionSyncTimes[syncedPartition] = now
		}
		m.lastSyncedPartition = syncedPartition
		klog.V(5).InfoS("Diff partition sync completed",
			"schedulerIndex", m.schedulerIndex,      // 当前调度器索引
			"syncedPartition", syncedPartition,      // 本次同步的分区
			"nextPartition", (m.currentPartitionIndex+1)%m.numPartitions) // 下次将同步的分区
		// 轮换到下一个分区（与 sim.ipynb 一致：同步后递增）
		m.currentPartitionIndex = (m.currentPartitionIndex + 1) % m.numPartitions
		return SyncPartitionResult{
			Synced:           true,                    // 成功同步
			SyncedPartitions: []int{syncedPartition},  // 同步的分区列表（单个分区）
			IsFullSync:       false,                   // 不是全量同步
		}
	}

	// 默认返回：当同步模式不匹配时
	return SyncPartitionResult{
		Synced:           false,           // 同步失败
		SyncedPartitions: nil,             // 无同步的分区
		IsFullSync:       false,           // 不是全量同步
	}
}

// GetLastSyncedPartition 获取上次同步的分区索引
// 返回值：分区索引，-1 表示全量同步或尚未同步
func (m *NodeHistoryManager) GetLastSyncedPartition() int {
	m.partitionMu.RLock()
	defer m.partitionMu.RUnlock()
	return m.lastSyncedPartition
}

// GetSyncGap 获取同步间隔时间
func (m *NodeHistoryManager) GetSyncGap() time.Duration {
	return m.syncGap
}

// SetSyncGap 设置同步间隔时间
func (m *NodeHistoryManager) SetSyncGap(gap time.Duration) {
	m.partitionMu.Lock()
	defer m.partitionMu.Unlock()
	if gap < 0 {
		gap = 0
	}
	m.syncGap = gap
}

// GetPartitionStaleness 获取所有分区的新鲜度（按新鲜度从小到大排序）
// 返回值: 分区新鲜度列表，staleness 越小表示越新鲜
// GetPartitionStaleness 获取所有分区的新鲜度信息，并按新鲜度排序
// 返回一个按新鲜度升序排列的分区新鲜度数组（staleness越小表示越新鲜）
func (m *NodeHistoryManager) GetPartitionStaleness() []PartitionStaleness {
	// 使用读锁，因为只需要读取数据而不需要修改，允许并发读取
	m.partitionMu.RLock()
	defer m.partitionMu.RUnlock()

	// 获取当前时间戳，用于计算各分区的陈旧程度
	now := m.clock()
	// 创建一个长度为分区总数的数组，用于存储每个分区的新鲜度信息
	staleness := make([]PartitionStaleness, m.numPartitions)

	// 遍历所有分区，计算每个分区的陈旧程度（从上次同步到现在的时间间隔）
	for i := 0; i < m.numPartitions; i++ {
		// 计算当前时间与该分区最后同步时间的差值，转换为秒
		// Sub方法返回一个time.Duration类型，Seconds()将其转换为浮点秒数
		stale := now.Sub(m.partitionSyncTimes[i]).Seconds()
		
		// 构造分区新鲜度信息结构体
		staleness[i] = PartitionStaleness{
			PartitionID: i,      // 分区ID
			Staleness:   stale,  // 陈旧程度（秒）
		}
	}

	// 按新鲜度排序：使用冒泡排序算法，将staleness越小（越新鲜）的元素排在前面
	// 外层循环控制排序趟数，最多需要n-1趟
	for i := 0; i < len(staleness)-1; i++ {
		// 内层循环进行相邻元素比较和交换
		for j := i + 1; j < len(staleness); j++ {
			// 如果前面元素的陈旧程度大于后面元素，则交换位置
			// 这样确保较小的staleness（更新鲜）排在前面
			if staleness[i].Staleness > staleness[j].Staleness {
				// 交换两个PartitionStaleness结构体
				staleness[i], staleness[j] = staleness[j], staleness[i]
			}
		}
	}

	// 返回按新鲜度排序后的分区新鲜度数组
	// 排序后，数组第一个元素是最新鲜的分区，最后一个是最陈旧的分区
	return staleness
}

// GetNodeStaleness 获取节点的新鲜度（基于其所属分区的同步时间）
func (m *NodeHistoryManager) GetNodePartitionStaleness(nodeName string) float64 {
	partitionID := m.GetNodePartition(nodeName)

	m.partitionMu.RLock()
	defer m.partitionMu.RUnlock()

	if partitionID >= 0 && partitionID < len(m.partitionSyncTimes) {
		now := m.clock()
		return now.Sub(m.partitionSyncTimes[partitionID]).Seconds()
	}

	return 0.0
}

// SortNodesByPartitionFreshness 根据分区新鲜度对节点进行排序
// nodes: 节点名称列表
// 返回值: 按分区新鲜度排序后的节点列表（最新鲜的分区中的节点排在前面）
func (m *NodeHistoryManager) SortNodesByPartitionFreshness(nodes []string) []string {
	if len(nodes) == 0 || m.scheduleStrategy != ScheduleStrategyLatency {
		return nodes
	}

	// 获取分区新鲜度
	partitionStaleness := m.GetPartitionStaleness()

	// 创建分区 ID 到排序位置的映射
	partitionOrder := make(map[int]int)
	for order, ps := range partitionStaleness {
		partitionOrder[ps.PartitionID] = order
	}

	// 按分区新鲜度对节点排序
	type nodeWithOrder struct {
		name  string
		order int
	}
	nodesWithOrder := make([]nodeWithOrder, len(nodes))
	for i, nodeName := range nodes {
		partitionID := m.GetNodePartition(nodeName)
		order, exists := partitionOrder[partitionID]
		if !exists {
			order = m.numPartitions // 未知分区排在最后
		}
		nodesWithOrder[i] = nodeWithOrder{name: nodeName, order: order}
	}

	// 排序
	for i := 0; i < len(nodesWithOrder)-1; i++ {
		for j := i + 1; j < len(nodesWithOrder); j++ {
			if nodesWithOrder[i].order > nodesWithOrder[j].order {
				nodesWithOrder[i], nodesWithOrder[j] = nodesWithOrder[j], nodesWithOrder[i]
			}
		}
	}

	// 提取排序后的节点名称
	sortedNodes := make([]string, len(nodes))
	for i, nwo := range nodesWithOrder {
		sortedNodes[i] = nwo.name
	}

	klog.V(5).InfoS("Sorted nodes by partition freshness",
		"numNodes", len(nodes),
		"scheduleStrategy", m.scheduleStrategy)

	return sortedNodes
}

// GetSyncMode 获取同步模式
func (m *NodeHistoryManager) GetSyncMode() SyncMode {
	return m.syncMode
}

// SetSyncMode 设置同步模式
func (m *NodeHistoryManager) SetSyncMode(mode SyncMode) {
	m.syncMode = mode
}

// GetScheduleStrategy 获取调度策略
func (m *NodeHistoryManager) GetScheduleStrategy() ScheduleStrategy {
	return m.scheduleStrategy
}

// SetScheduleStrategy 设置调度策略
func (m *NodeHistoryManager) SetScheduleStrategy(strategy ScheduleStrategy) {
	m.scheduleStrategy = strategy
}

// GetNumPartitions 获取分区数量
func (m *NodeHistoryManager) GetNumPartitions() int {
	return m.numPartitions
}

// SetNumPartitions 设置分区数量
// 注意：这会重置分区同步时间和节点分区映射
func (m *NodeHistoryManager) SetNumPartitions(numPartitions int) {
	if numPartitions < 1 {
		numPartitions = 1
	}

	m.partitionMu.Lock()
	defer m.partitionMu.Unlock()

	m.numPartitions = numPartitions

	// 重新初始化分区同步时间
	now := m.clock()
	m.partitionSyncTimes = make([]time.Time, numPartitions)
	for i := range m.partitionSyncTimes {
		m.partitionSyncTimes[i] = now
	}

	// 清空节点分区映射，让节点重新分配
	m.nodeToPartition = make(map[string]int)

	// 重置当前分区索引
	m.currentPartitionIndex = m.schedulerIndex % numPartitions

	klog.V(4).InfoS("Reset partitions",
		"numPartitions", numPartitions,
		"schedulerIndex", m.schedulerIndex)
}

// GetSchedulerIndex 获取调度器索引
func (m *NodeHistoryManager) GetSchedulerIndex() int {
	return m.schedulerIndex
}

// SetSchedulerIndex 设置调度器索引
func (m *NodeHistoryManager) SetSchedulerIndex(index int) {
	m.partitionMu.Lock()
	defer m.partitionMu.Unlock()

	m.schedulerIndex = index
	// 在 diffSync 模式下，不同调度器从不同分区开始
	m.currentPartitionIndex = index % m.numPartitions
}

// GetPartitionSyncTimes 获取所有分区的同步时间（用于调试）
func (m *NodeHistoryManager) GetPartitionSyncTimes() []time.Time {
	m.partitionMu.RLock()
	defer m.partitionMu.RUnlock()

	times := make([]time.Time, len(m.partitionSyncTimes))
	copy(times, m.partitionSyncTimes)
	return times
}

// IsLatencyFirst 返回是否使用延迟优先策略
func (m *NodeHistoryManager) IsLatencyFirst() bool {
	return m.scheduleStrategy == ScheduleStrategyLatency
}
