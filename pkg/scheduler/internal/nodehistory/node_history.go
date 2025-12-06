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
}

// NewNodeHistoryManager 创建一个新的节点历史管理器
func NewNodeHistoryManager() *NodeHistoryManager {
	return &NodeHistoryManager{
		nodeStats: make(map[string]*NodeStats),
		clock:     time.Now,
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
		"node", nodeName,                 // 节点名称
		"score", score,                   // 原始调度分数
		"normalizedScore", normalizedScore, // 归一化后的调度分数
		"successRate", successRate,       // 历史成功率
		"staleness", staleness,           // 陈旧度
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
