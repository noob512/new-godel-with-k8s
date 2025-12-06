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
func (m *NodeHistoryManager) GetOrCreateStats(nodeName string) *NodeStats {
	m.mu.Lock()
	defer m.mu.Unlock()

	stats, exists := m.nodeStats[nodeName]
	if !exists {
		stats = &NodeStats{
			LastSyncTime: m.clock(),
		}
		m.nodeStats[nodeName] = stats
	}
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
func (m *NodeHistoryManager) RecordConflict(nodeName string) {
	stats := m.GetOrCreateStats(nodeName)
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.ConflictCount++
	stats.LastSyncTime = m.clock()
	klog.V(5).InfoS("Recorded conflict for node", "node", nodeName, "conflictCount", stats.ConflictCount)
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

// CalculateAdoptionProbability 计算节点的采纳概率
// AdoptionProbability = Score × successRate × exp(-staleness/100)
func (m *NodeHistoryManager) CalculateAdoptionProbability(nodeName string, score int64) float64 {
	successRate := m.GetSuccessRate(nodeName)
	staleness := m.GetStaleness(nodeName)

	// 将score归一化到0-1范围（MaxNodeScore = 100）
	const MaxNodeScore = 100.0
	normalizedScore := float64(score) / MaxNodeScore

	// 计算新鲜度衰减因子
	freshnessFactor := math.Exp(-staleness / 100.0)

	adoptionProb := normalizedScore * successRate * freshnessFactor

	klog.V(5).InfoS("Calculated adoption probability",
		"node", nodeName,
		"score", score,
		"normalizedScore", normalizedScore,
		"successRate", successRate,
		"staleness", staleness,
		"freshnessFactor", freshnessFactor,
		"adoptionProbability", adoptionProb)

	return adoptionProb
}

// RemoveNode 移除节点的统计信息（当节点被删除时调用）
func (m *NodeHistoryManager) RemoveNode(nodeName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodeStats, nodeName)
	klog.V(4).InfoS("Removed node stats", "node", nodeName)
}
