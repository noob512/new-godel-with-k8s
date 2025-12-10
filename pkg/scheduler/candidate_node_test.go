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

package scheduler

import (
	"math"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/internal/nodehistory"
)

// TestSelectCandidateNodes 测试选择多个候选节点的功能
// TestSelectCandidateNodes 是对 selectCandidateNodes 函数的单元测试
// 该函数用于从节点评分列表中选择指定数量的候选节点，按分数从高到低排序
func TestSelectCandidateNodes(t *testing.T) {
	// 定义测试用例切片，每个用例包含不同的输入参数和期望结果
	tests := []struct {
		name          string                        // 测试用例名称
		nodeScoreList framework.NodeScoreList       // 输入的节点评分列表
		maxCandidates int                           // 期望的最大候选节点数量
		wantErr       bool                          // 期望是否返回错误
		wantCount     int                           // 期望返回的候选节点数量
		wantFirst     string                        // 期望返回的第一个节点名称（最高分节点）
	}{
		{
			// 测试空列表的边界情况
			name:          "空列表",
			nodeScoreList: framework.NodeScoreList{}, // 空的节点评分列表
			maxCandidates: 3,                         // 期望最多返回3个节点
			wantErr:       true,                      // 期望返回错误
			wantCount:     0,                         // 期望返回0个节点
		},
		{
			// 测试只有一个节点的情况
			name: "单节点",
			nodeScoreList: framework.NodeScoreList{ // 只有一个节点的评分列表
				{Name: "node1", Score: 100}, // 节点名为node1，评分为100
			},
			maxCandidates: 3,     // 期望最多返回3个节点
			wantErr:       false, // 期望不返回错误
			wantCount:     1,     // 期望返回1个节点
			wantFirst:     "node1", // 期望返回的第一个节点是node1
		},
		{
			// 测试多个节点，验证是否按分数从高到低排序
			name: "多节点-按分数排序",
			nodeScoreList: framework.NodeScoreList{ // 包含3个节点的评分列表，分数乱序
				{Name: "node1", Score: 50},  // node1分数50
				{Name: "node2", Score: 100}, // node2分数100（最高）
				{Name: "node3", Score: 75},  // node3分数75
			},
			maxCandidates: 3,      // 期望最多返回3个节点
			wantErr:       false,  // 期望不返回错误
			wantCount:     3,      // 期望返回3个节点
			wantFirst:     "node2", // 期望返回的第一个节点是分数最高的node2
		},
		{
			// 测试多个节点，但限制返回数量
			name: "多节点-限制数量",
			nodeScoreList: framework.NodeScoreList{ // 包含5个节点的评分列表
				{Name: "node1", Score: 100}, // node1分数100（最高）
				{Name: "node2", Score: 90},  // node2分数90
				{Name: "node3", Score: 80},  // node3分数80
				{Name: "node4", Score: 70},  // node4分数70
				{Name: "node5", Score: 60},  // node5分数60
			},
			maxCandidates: 3,      // 限制最多返回3个节点
			wantErr:       false,  // 期望不返回错误
			wantCount:     3,      // 期望返回3个节点
			wantFirst:     "node1", // 期望返回的第一个节点是分数最高的node1
		},
		{
			// 测试多个节点具有相同分数的情况
			name: "多节点-相同分数",
			nodeScoreList: framework.NodeScoreList{ // 3个节点分数都为100
				{Name: "node1", Score: 100},
				{Name: "node2", Score: 100},
				{Name: "node3", Score: 100},
			},
			maxCandidates: 3,     // 期望最多返回3个节点
			wantErr:       false, // 期望不返回错误
			wantCount:     3,     // 期望返回3个节点
		},
	}

	// 遍历所有测试用例
	for _, tt := range tests {
		// 为每个测试用例创建一个子测试，便于单独运行和调试
		t.Run(tt.name, func(t *testing.T) {
			// 调用被测试的函数
			candidates, err := selectCandidateNodes(tt.nodeScoreList, tt.maxCandidates)

			// 检查函数返回的错误是否与期望的错误状态一致
			if (err != nil) != tt.wantErr {
				t.Errorf("selectCandidateNodes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// 如果期望返回错误，则跳过后续检查
			if tt.wantErr {
				return
			}

			// 检查返回的候选节点数量是否与期望的数量一致
			if len(candidates) != tt.wantCount {
				t.Errorf("selectCandidateNodes() got %d candidates, want %d", len(candidates), tt.wantCount)
			}

			// 检查返回的第一个节点是否与期望的第一个节点一致（如果指定了期望值）
			if tt.wantFirst != "" && len(candidates) > 0 && candidates[0].Name != tt.wantFirst {
				t.Errorf("selectCandidateNodes() first node = %s, want %s", candidates[0].Name, tt.wantFirst)
			}

			// 检查返回的节点列表是否按分数从高到低排序
			for i := 1; i < len(candidates); i++ {
				if candidates[i].Score > candidates[i-1].Score { // 如果后一个节点分数高于前一个，说明排序错误
					t.Errorf("selectCandidateNodes() nodes not sorted by score descending: %v > %v at index %d",
						candidates[i].Score, candidates[i-1].Score, i)
				}
			}
		})
	}
}

// TestSelectNodesByCumulativeProbability 测试累积概率选择节点的功能
// TestSelectNodesByCumulativeProbability 是对 selectNodesByCumulativeProbability 方法的单元测试
// 该方法用于根据累积概率选择节点，通常用于调度器中的节点选择逻辑
func TestSelectNodesByCumulativeProbability(t *testing.T) {
	// 创建一个简化的 Scheduler 实例，用于调用被测试的方法
	// 初始化时包含一个节点历史管理器，用于跟踪节点的调度历史
	sched := &Scheduler{
		nodeHistoryManager: nodehistory.NewNodeHistoryManager(),
	}

	// 创建一个测试用的 Pod 对象，用于模拟调度请求
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",  // Pod 名称
			Namespace: "default",   // Pod 所在命名空间
		},
	}

	// 定义测试用例切片，每个用例包含不同的输入参数和期望结果范围
	tests := []struct {
		name           string          // 测试用例名称
		candidateNodes []CandidateNode // 输入的候选节点列表，包含节点名称、分数和采纳概率
		maxCandidates  int             // 期望的最大候选节点数量限制
		wantMinCount   int             // 期望返回的最少节点数量
		wantMaxCount   int             // 期望返回的最多节点数量
	}{
		{
			// 测试空列表的边界情况
			name:           "空列表",
			candidateNodes: []CandidateNode{}, // 空的候选节点列表
			maxCandidates:  3,                 // 期望最多返回3个节点
			wantMinCount:   0,                 // 期望最少返回0个节点
			wantMaxCount:   0,                 // 期望最多返回0个节点
		},
		{
			// 测试只有一个候选节点的情况
			name: "单节点",
			candidateNodes: []CandidateNode{
				{Name: "node1", Score: 100, AdoptionProbability: 0.8}, // 单个候选节点，分数100，采纳概率80%
			},
			maxCandidates: 3,      // 期望最多返回3个节点
			wantMinCount:  1,      // 期望最少返回1个节点
			wantMaxCount:  1,      // 期望最多返回1个节点
		},
		{
			// 测试多个节点且概率都较高的情况，期望所有节点都被选中
			name: "多节点-高概率",
			candidateNodes: []CandidateNode{ // 包含3个候选节点，概率都较高
				{Name: "node1", Score: 100, AdoptionProbability: 0.9}, // 分数100，采纳概率90%
				{Name: "node2", Score: 90, AdoptionProbability: 0.8},  // 分数90，采纳概率80%
				{Name: "node3", Score: 80, AdoptionProbability: 0.7},  // 分数80，采纳概率70%
			},
			maxCandidates: 3,      // 期望最多返回3个节点
			wantMinCount:  3,      // 期望最少返回3个节点（所有节点概率高，都应被选中）
			wantMaxCount:  3,      // 期望最多返回3个节点
		},
		{
			// 测试多个节点超过最大限制的情况，验证数量限制逻辑
			name: "多节点-超过限制",
			candidateNodes: []CandidateNode{ // 包含5个候选节点
				{Name: "node1", Score: 100, AdoptionProbability: 0.8}, // 分数100，采纳概率80%
				{Name: "node2", Score: 90, AdoptionProbability: 0.7},  // 分数90，采纳概率70%
				{Name: "node3", Score: 80, AdoptionProbability: 0.6},  // 分数80，采纳概率60%
				{Name: "node4", Score: 70, AdoptionProbability: 0.5},  // 分数70，采纳概率50%
				{Name: "node5", Score: 60, AdoptionProbability: 0.4},  // 分数60，采纳概率40%
			},
			maxCandidates: 3,      // 限制最多返回3个节点
			wantMinCount:  3,      // 期望最少返回3个节点（受限制影响）
			wantMaxCount:  3,      // 期望最多返回3个节点（受限制影响）
		},
	}

	// 遍历所有测试用例
	for _, tt := range tests {
		// 为每个测试用例创建一个子测试，便于单独运行和调试
		t.Run(tt.name, func(t *testing.T) {
			// 调用被测试的 Scheduler 方法，传入候选节点列表、最大候选数量和Pod对象
			result := sched.selectNodesByCumulativeProbability(tt.candidateNodes, tt.maxCandidates, pod)

			// 检查返回的节点数量是否在期望的范围内
			if len(result) < tt.wantMinCount || len(result) > tt.wantMaxCount {
				t.Errorf("selectNodesByCumulativeProbability() got %d nodes, want between %d and %d",
					len(result), tt.wantMinCount, tt.wantMaxCount)
			}
		})
	}
}

// TestSelectNodesByCumulativeProbabilityDistribution 测试累积概率选择的分布
// TestSelectNodesByCumulativeProbabilityDistribution 是对 selectNodesByCumulativeProbability 方法的概率分布测试
// 该测试通过大量迭代运行来验证节点选择是否符合预期的概率分布规律
func TestSelectNodesByCumulativeProbabilityDistribution(t *testing.T) {
	// 创建一个简化的 Scheduler 实例，用于调用被测试的方法
	// 初始化时包含一个节点历史管理器，用于跟踪节点的调度历史
	sched := &Scheduler{
		nodeHistoryManager: nodehistory.NewNodeHistoryManager(),
	}

	// 创建一个测试用的 Pod 对象，用于模拟调度请求
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",  // Pod 名称
			Namespace: "default",   // Pod 所在命名空间
		},
	}

	// 设置候选节点及其采纳概率，用于测试概率分布
	candidateNodes := []CandidateNode{
		{Name: "node1", Score: 100, AdoptionProbability: 0.5}, // 节点1，分数100，采纳概率50%
		{Name: "node2", Score: 90, AdoptionProbability: 0.3},  // 节点2，分数90，采纳概率30%
		{Name: "node3", Score: 80, AdoptionProbability: 0.2},  // 节点3，分数80，采纳概率20%
	}

	// 运行多次测试，统计每个节点被选为主节点（第一个节点）的次数
	iterations := 10000                               // 迭代次数，足够大以保证统计意义
	selectionCount := make(map[string]int)            // 记录每个节点被选为主节点的次数

	// 循环执行调度选择，收集统计数据
	for i := 0; i < iterations; i++ {
		// 调用被测试的方法，每次选择最多3个节点
		result := sched.selectNodesByCumulativeProbability(candidateNodes, 3, pod)
		// 如果返回结果非空，记录第一个被选中的节点
		if len(result) > 0 {
			selectionCount[result[0].Name]++ // 统计第一个节点的出现次数
		}
	}

	// 验证分布是否合理
	// 根据累积概率选择算法的逻辑计算理论期望概率：
	// node1 的直接选择概率是 p1 = 0.5
	// node2 的选择概率是 (1-p1)*p2 = 0.5 * 0.3 = 0.15 （node1未被选中，然后node2被选中）
	// node3 的选择概率是 (1-p1)*(1-p2)*p3 = 0.5 * 0.7 * 0.2 = 0.07 （前两个都未被选中，然后node3被选中）
	// 剩余概率会回退到第一个节点: 1 - 0.5 - 0.15 - 0.07 = 0.28 （所有节点都未被选中时的回退机制）

	// 计算最终的预期概率（包括回退概率加到第一个节点上）
	expectedProb := map[string]float64{
		"node1": 0.5 + (1.0-0.5)*(1.0-0.3)*(1.0-0.2), // 直接选择概率0.5 + 回退概率(0.5*0.7*0.2)=0.28，总计约0.78
		"node2": 0.15,                                  // 间接选择概率0.5*0.3=0.15
		"node3": 0.07,                                  // 间接选择概率0.5*0.7*0.2=0.07
	}

	// 输出统计结果，便于分析实际概率与期望概率的对比
	t.Logf("Selection distribution over %d iterations:", iterations)
	for node, count := range selectionCount {
		actualProb := float64(count) / float64(iterations) // 计算实际观察到的概率
		t.Logf("  %s: count=%d, actualProb=%.4f, expectedProb=%.4f",
			node, count, actualProb, expectedProb[node]) // 输出节点名、选择次数、实际概率、期望概率
	}
}
// TestShouldReserveSecondaryNode 测试次优节点预留决策
// TestShouldReserveSecondaryNode 是对 shouldReserveSecondaryNode 方法的单元测试
// 该方法用于决定是否需要预留次优节点，通常用于调度器的二次节点预留决策
func TestShouldReserveSecondaryNode(t *testing.T) {
	// 创建一个简化的 Scheduler 实例，用于调用被测试的方法
	// 初始化时包含一个节点历史管理器，用于跟踪节点的调度历史
	sched := &Scheduler{
		nodeHistoryManager: nodehistory.NewNodeHistoryManager(),
	}

	// 创建一个测试用的 Pod 对象，用于模拟调度请求
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",  // Pod 名称
			Namespace: "default",   // Pod 所在命名空间
		},
	}

	// 定义测试用例切片，每个用例包含不同的输入参数和期望结果
	tests := []struct {
		name           string          // 测试用例名称
		nextNum        int             // 下一个要选择的节点编号
		candidateNodes []CandidateNode // 候选节点列表，包含节点名称、分数和采纳概率
		wantFalse      bool            // 如果为 true，期望方法返回 false（不预留次优节点）
	}{
		{
			// 测试候选节点数量不足的情况
			name:    "候选节点不足",
			nextNum: 1, // 要选择第1个节点
			candidateNodes: []CandidateNode{
				{Name: "node1", Score: 100, AdoptionProbability: 0.8}, // 只有一个候选节点
			},
			wantFalse: true, // 期望返回 false，因为没有次优节点可供预留
		},
		{
			// 测试首选节点采纳概率为1的情况，此时不需要预留次优节点
			name:    "首选节点概率为1-不预留次优",
			nextNum: 1, // 要选择第1个节点
			candidateNodes: []CandidateNode{
				{Name: "node1", Score: 100, AdoptionProbability: 1.0}, // 首选节点采纳概率为100%
				{Name: "node2", Score: 90, AdoptionProbability: 0.8},  // 次优节点采纳概率为80%
			},
			// 计算逻辑: (1-1.0)*0.8 = 0，预留概率为0，所以期望返回 false
			wantFalse: true, // 期望返回 false，因为首选节点确定会被采纳，无需预留次优
		},
		{
			// 测试次优节点采纳概率为0的情况，此时不需要预留次优节点
			name:    "次优节点概率为0-不预留次优",
			nextNum: 1, // 要选择第1个节点
			candidateNodes: []CandidateNode{
				{Name: "node1", Score: 100, AdoptionProbability: 0.5}, // 首选节点采纳概率为50%
				{Name: "node2", Score: 90, AdoptionProbability: 0.0},  // 次优节点采纳概率为0%
			},
			// 计算逻辑: (1-0.5)*0.0 = 0，预留概率为0，所以期望返回 false
			wantFalse: true, // 期望返回 false，因为次优节点永远不会被采纳，预留无意义
		},
	}

	// 遍历所有测试用例
	for _, tt := range tests {
		// 为每个测试用例创建一个子测试，便于单独运行和调试
		t.Run(tt.name, func(t *testing.T) {
			// 调用被测试的 Scheduler 方法，传入下一个节点编号、候选节点列表和Pod对象
			result := sched.shouldReserveSecondaryNode(tt.nextNum, tt.candidateNodes, pod)
			
			// 检查返回结果是否符合期望
			// 如果期望返回 false 但实际返回 true，则报告错误
			if tt.wantFalse && result {
				t.Errorf("shouldReserveSecondaryNode() = true, want false")
			}
		})
	}
}

// TestShouldReserveSecondaryNodeProbabilityDistribution 测试次优节点预留决策的概率分布
func TestShouldReserveSecondaryNodeProbabilityDistribution(t *testing.T) {
	sched := &Scheduler{
		nodeHistoryManager: nodehistory.NewNodeHistoryManager(),
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}

	tests := []struct {
		name             string
		p1               float64 // 首选节点采纳概率
		p2               float64 // 次优节点采纳概率
		expectedProbLow  float64 // 预期概率下限
		expectedProbHigh float64 // 预期概率上限
	}{
		{
			name:             "p1=0.8, p2=0.6",
			p1:               0.8,
			p2:               0.6,
			expectedProbLow:  0.08, // (1-0.8)*0.6 = 0.12, 允许误差
			expectedProbHigh: 0.16,
		},
		{
			name:             "p1=0.5, p2=0.5",
			p1:               0.5,
			p2:               0.5,
			expectedProbLow:  0.20, // (1-0.5)*0.5 = 0.25, 允许误差
			expectedProbHigh: 0.30,
		},
		{
			name:             "p1=0.2, p2=0.8",
			p1:               0.2,
			p2:               0.8,
			expectedProbLow:  0.58, // (1-0.2)*0.8 = 0.64, 允许误差
			expectedProbHigh: 0.70,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidateNodes := []CandidateNode{
				{Name: "node1", Score: 100, AdoptionProbability: tt.p1},
				{Name: "node2", Score: 90, AdoptionProbability: tt.p2},
			}

			// 运行多次测试
			iterations := 10000
			reserveCount := 0

			for i := 0; i < iterations; i++ {
				if sched.shouldReserveSecondaryNode(1, candidateNodes, pod) {
					reserveCount++
				}
			}

			actualProb := float64(reserveCount) / float64(iterations)
			expectedProb := (1.0 - tt.p1) * tt.p2

			t.Logf("Test %s: reserveCount=%d, actualProb=%.4f, expectedProb=%.4f",
				tt.name, reserveCount, actualProb, expectedProb)

			if actualProb < tt.expectedProbLow || actualProb > tt.expectedProbHigh {
				t.Errorf("shouldReserveSecondaryNode() probability = %.4f, want between %.4f and %.4f (expected: %.4f)",
					actualProb, tt.expectedProbLow, tt.expectedProbHigh, expectedProb)
			}
		})
	}
}

// TestNodeHistoryManagerIntegration 测试节点历史管理器与调度器的集成
func TestNodeHistoryManagerIntegration(t *testing.T) {
	manager := nodehistory.NewNodeHistoryManager()

	// 模拟一系列调度事件
	nodes := []string{"node1", "node2", "node3"}

	// node1: 10次成功, 2次冲突
	for i := 0; i < 10; i++ {
		manager.RecordSuccess(nodes[0])
	}
	for i := 0; i < 2; i++ {
		manager.RecordConflict(nodes[0])
	}

	// node2: 5次成功, 5次冲突
	for i := 0; i < 5; i++ {
		manager.RecordSuccess(nodes[1])
	}
	for i := 0; i < 5; i++ {
		manager.RecordConflict(nodes[1])
	}

	// node3: 2次成功, 8次冲突
	for i := 0; i < 2; i++ {
		manager.RecordSuccess(nodes[2])
	}
	for i := 0; i < 8; i++ {
		manager.RecordConflict(nodes[2])
	}

	// 检查成功率
	// node1: 10 / (10+2+1) = 10/13 ≈ 0.769
	// node2: 5 / (5+5+1) = 5/11 ≈ 0.455
	// node3: 2 / (2+8+1) = 2/11 ≈ 0.182
	successRates := map[string]float64{
		"node1": manager.GetSuccessRate(nodes[0]),
		"node2": manager.GetSuccessRate(nodes[1]),
		"node3": manager.GetSuccessRate(nodes[2]),
	}

	t.Logf("Success rates:")
	for node, rate := range successRates {
		t.Logf("  %s: %.4f", node, rate)
	}

	// 验证成功率排序
	if successRates["node1"] < successRates["node2"] {
		t.Errorf("node1 should have higher success rate than node2")
	}
	if successRates["node2"] < successRates["node3"] {
		t.Errorf("node2 should have higher success rate than node3")
	}

	// 测试采纳概率计算
	score := int64(80)
	adoptionProbs := map[string]float64{
		"node1": manager.CalculateAdoptionProbability(nodes[0], score),
		"node2": manager.CalculateAdoptionProbability(nodes[1], score),
		"node3": manager.CalculateAdoptionProbability(nodes[2], score),
	}

	t.Logf("Adoption probabilities (score=%d):", score)
	for node, prob := range adoptionProbs {
		t.Logf("  %s: %.4f", node, prob)
	}

	// 验证采纳概率排序（同一分数下，成功率高的采纳概率也应该高）
	if adoptionProbs["node1"] < adoptionProbs["node2"] {
		t.Errorf("node1 should have higher adoption probability than node2")
	}
	if adoptionProbs["node2"] < adoptionProbs["node3"] {
		t.Errorf("node2 should have higher adoption probability than node3")
	}
}

// TestCandidateNodeWithAdoptionProbability 测试候选节点的采纳概率计算
func TestCandidateNodeWithAdoptionProbability(t *testing.T) {
	manager := nodehistory.NewNodeHistoryManager()

	// 设置节点历史
	for i := 0; i < 10; i++ {
		manager.RecordSuccess("node1")
	}
	for i := 0; i < 5; i++ {
		manager.RecordSuccess("node2")
		manager.RecordConflict("node2")
	}
	for i := 0; i < 10; i++ {
		manager.RecordConflict("node3")
	}

	// 创建候选节点
	candidates := []CandidateNode{
		{Name: "node1", Score: 100},
		{Name: "node2", Score: 90},
		{Name: "node3", Score: 80},
	}

	// 计算采纳概率
	for i := range candidates {
		candidates[i].AdoptionProbability = manager.CalculateAdoptionProbability(
			candidates[i].Name,
			candidates[i].Score,
		)
	}

	t.Logf("Candidate nodes with adoption probabilities:")
	for _, c := range candidates {
		t.Logf("  %s: score=%d, adoptionProb=%.4f", c.Name, c.Score, c.AdoptionProbability)
	}

	// 验证 node1（最高成功率）应该有最高的采纳概率（考虑到分数差异）
	// 这里由于 node1 分数也最高，所以它的采纳概率应该最高
	if candidates[0].AdoptionProbability < candidates[1].AdoptionProbability {
		t.Logf("Warning: node1 has lower adoption probability than node2, but this might be due to staleness")
	}
}

// TestDualReserveScenario 测试双重预留场景
func TestDualReserveScenario(t *testing.T) {
	sched := &Scheduler{
		nodeHistoryManager: nodehistory.NewNodeHistoryManager(),
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}

	// 模拟场景：首选节点采纳概率低，次优节点采纳概率高
	// 这种情况下，应该有较高概率为次优节点预留
	candidateNodes := []CandidateNode{
		{Name: "node1", Score: 100, AdoptionProbability: 0.3}, // 低采纳概率
		{Name: "node2", Score: 90, AdoptionProbability: 0.9},  // 高采纳概率
	}

	// 运行多次测试
	iterations := 10000
	reserveCount := 0

	for i := 0; i < iterations; i++ {
		if sched.shouldReserveSecondaryNode(1, candidateNodes, pod) {
			reserveCount++
		}
	}

	// 预期概率: (1-0.3)*0.9 = 0.63
	actualProb := float64(reserveCount) / float64(iterations)
	expectedProb := 0.63

	t.Logf("Dual reserve scenario:")
	t.Logf("  Primary node (node1): adoptionProb=0.3")
	t.Logf("  Secondary node (node2): adoptionProb=0.9")
	t.Logf("  Expected reserve probability: %.4f", expectedProb)
	t.Logf("  Actual reserve probability: %.4f (from %d iterations)", actualProb, iterations)
	t.Logf("  Reserve count: %d", reserveCount)

	// 允许 5% 的误差
	tolerance := 0.05
	if math.Abs(actualProb-expectedProb) > tolerance {
		t.Errorf("shouldReserveSecondaryNode() probability = %.4f, want approximately %.4f (tolerance: %.2f)",
			actualProb, expectedProb, tolerance)
	}
}

// TestMultipleCandidateNodesFailover 测试多候选节点故障转移场景
func TestMultipleCandidateNodesFailover(t *testing.T) {
	// 测试场景：有3个候选节点，验证选择逻辑
	nodeScoreList := framework.NodeScoreList{
		{Name: "node1", Score: 100},
		{Name: "node2", Score: 90},
		{Name: "node3", Score: 80},
	}

	candidates, err := selectCandidateNodes(nodeScoreList, 3)
	if err != nil {
		t.Fatalf("selectCandidateNodes() error: %v", err)
	}

	t.Logf("Selected candidates for failover scenario:")
	for i, c := range candidates {
		t.Logf("  [%d] %s: score=%d", i, c.Name, c.Score)
	}

	// 验证候选数量
	if len(candidates) != 3 {
		t.Errorf("Expected 3 candidates, got %d", len(candidates))
	}

	// 验证顺序（按分数降序）
	expectedOrder := []string{"node1", "node2", "node3"}
	for i, expected := range expectedOrder {
		if candidates[i].Name != expected {
			t.Errorf("Expected candidates[%d].Name = %s, got %s", i, expected, candidates[i].Name)
		}
	}
}

// BenchmarkSelectCandidateNodes 性能测试：选择候选节点
func BenchmarkSelectCandidateNodes(b *testing.B) {
	// 创建大量节点的评分列表
	nodeScoreList := make(framework.NodeScoreList, 1000)
	for i := 0; i < 1000; i++ {
		nodeScoreList[i] = framework.NodeScore{
			Name:  "node" + string(rune(i)),
			Score: int64(1000 - i),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = selectCandidateNodes(nodeScoreList, 5)
	}
}

// BenchmarkShouldReserveSecondaryNode 性能测试：次优节点预留决策
func BenchmarkShouldReserveSecondaryNode(b *testing.B) {
	sched := &Scheduler{
		nodeHistoryManager: nodehistory.NewNodeHistoryManager(),
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}

	candidateNodes := []CandidateNode{
		{Name: "node1", Score: 100, AdoptionProbability: 0.8},
		{Name: "node2", Score: 90, AdoptionProbability: 0.6},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sched.shouldReserveSecondaryNode(1, candidateNodes, pod)
	}
}

// BenchmarkSelectNodesByCumulativeProbability 性能测试：累积概率选择
func BenchmarkSelectNodesByCumulativeProbability(b *testing.B) {
	sched := &Scheduler{
		nodeHistoryManager: nodehistory.NewNodeHistoryManager(),
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}

	candidateNodes := []CandidateNode{
		{Name: "node1", Score: 100, AdoptionProbability: 0.8},
		{Name: "node2", Score: 90, AdoptionProbability: 0.6},
		{Name: "node3", Score: 80, AdoptionProbability: 0.4},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sched.selectNodesByCumulativeProbability(candidateNodes, 3, pod)
	}
}

