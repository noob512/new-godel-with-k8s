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
}

// NewNodeHistoryManager 创建一个新的节点历史管理器
func NewNodeHistoryManager() *NodeHistoryManager {
	return NewNodeHistoryManagerWithConfig(3, UpdateStrategyProbability)
}

// NewNodeHistoryManagerWithConfig 创建一个带配置的节点历史管理器
// numBackup: 备选节点数量
// updateStrategy: 本地状态更新策略
func NewNodeHistoryManagerWithConfig(numBackup int, updateStrategy UpdateStrategy) *NodeHistoryManager {
	// 接收频率数组长度 = 1(首选) + numBackup(备选) + 1(全部拒绝)
	freqSize := numBackup + 2
	return &NodeHistoryManager{
		nodeStats: make(map[string]*NodeStats),
		clock:     time.Now,
		acceptFreq: &AcceptanceFrequency{
			Frequencies: make([]int64, freqSize),
		},
		numBackup:      numBackup,
		updateStrategy: updateStrategy,
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

	if m.acceptFreq != nil {
		summary["acceptFrequencies"] = m.GetAcceptFrequencyCopy()
	}

	m.mu.RLock()
	summary["nodeCount"] = len(m.nodeStats)
	m.mu.RUnlock()

	return summary
}
