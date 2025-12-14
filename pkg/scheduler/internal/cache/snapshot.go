/*
Copyright 2019 The Kubernetes Authors.

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

package cache

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// Snapshot is a snapshot of cache NodeInfo and NodeTree order. The scheduler takes a
// snapshot at the beginning of each scheduling cycle and uses it for its operations in that cycle.
type Snapshot struct {
	// nodeInfoMap a map of node name to a snapshot of its NodeInfo.
	nodeInfoMap map[string]*framework.NodeInfo
	// nodeInfoList is the list of nodes as ordered in the cache's nodeTree.
	nodeInfoList []*framework.NodeInfo
	// havePodsWithAffinityNodeInfoList is the list of nodes with at least one pod declaring affinity terms.
	havePodsWithAffinityNodeInfoList []*framework.NodeInfo
	// havePodsWithRequiredAntiAffinityNodeInfoList is the list of nodes with at least one pod declaring
	// required anti-affinity terms.
	havePodsWithRequiredAntiAffinityNodeInfoList []*framework.NodeInfo
	generation                                   int64

	// ========== 分区同步相关字段 ==========
	// numPartitions 分区数量
	numPartitions int
	// nodeToPartition 节点名称到分区 ID 的映射
	nodeToPartition map[string]int
	// partitionGenerations 每个分区的 generation，用于增量更新
	partitionGenerations []int64
	// partitionNodeLists 每个分区的节点列表
	partitionNodeLists [][]*framework.NodeInfo
}

var _ framework.SharedLister = &Snapshot{}

// NewEmptySnapshot initializes a Snapshot struct and returns it.
func NewEmptySnapshot() *Snapshot {
	return &Snapshot{
		nodeInfoMap: make(map[string]*framework.NodeInfo),
	}
}

// NewSnapshot initializes a Snapshot struct and returns it.
func NewSnapshot(pods []*v1.Pod, nodes []*v1.Node) *Snapshot {
	nodeInfoMap := createNodeInfoMap(pods, nodes)
	nodeInfoList := make([]*framework.NodeInfo, 0, len(nodeInfoMap))
	havePodsWithAffinityNodeInfoList := make([]*framework.NodeInfo, 0, len(nodeInfoMap))
	havePodsWithRequiredAntiAffinityNodeInfoList := make([]*framework.NodeInfo, 0, len(nodeInfoMap))
	for _, v := range nodeInfoMap {
		nodeInfoList = append(nodeInfoList, v)
		if len(v.PodsWithAffinity) > 0 {
			havePodsWithAffinityNodeInfoList = append(havePodsWithAffinityNodeInfoList, v)
		}
		if len(v.PodsWithRequiredAntiAffinity) > 0 {
			havePodsWithRequiredAntiAffinityNodeInfoList = append(havePodsWithRequiredAntiAffinityNodeInfoList, v)
		}
	}

	s := NewEmptySnapshot()
	s.nodeInfoMap = nodeInfoMap
	s.nodeInfoList = nodeInfoList
	s.havePodsWithAffinityNodeInfoList = havePodsWithAffinityNodeInfoList
	s.havePodsWithRequiredAntiAffinityNodeInfoList = havePodsWithRequiredAntiAffinityNodeInfoList

	return s
}

// createNodeInfoMap obtains a list of pods and pivots that list into a map
// where the keys are node names and the values are the aggregated information
// for that node.
func createNodeInfoMap(pods []*v1.Pod, nodes []*v1.Node) map[string]*framework.NodeInfo {
	nodeNameToInfo := make(map[string]*framework.NodeInfo)
	for _, pod := range pods {
		nodeName := pod.Spec.NodeName
		if _, ok := nodeNameToInfo[nodeName]; !ok {
			nodeNameToInfo[nodeName] = framework.NewNodeInfo()
		}
		nodeNameToInfo[nodeName].AddPod(pod)
	}
	imageExistenceMap := createImageExistenceMap(nodes)

	for _, node := range nodes {
		if _, ok := nodeNameToInfo[node.Name]; !ok {
			nodeNameToInfo[node.Name] = framework.NewNodeInfo()
		}
		nodeInfo := nodeNameToInfo[node.Name]
		nodeInfo.SetNode(node)
		nodeInfo.ImageStates = getNodeImageStates(node, imageExistenceMap)
	}
	return nodeNameToInfo
}

// getNodeImageStates returns the given node's image states based on the given imageExistence map.
func getNodeImageStates(node *v1.Node, imageExistenceMap map[string]sets.String) map[string]*framework.ImageStateSummary {
	imageStates := make(map[string]*framework.ImageStateSummary)

	for _, image := range node.Status.Images {
		for _, name := range image.Names {
			imageStates[name] = &framework.ImageStateSummary{
				Size:     image.SizeBytes,
				NumNodes: len(imageExistenceMap[name]),
			}
		}
	}
	return imageStates
}

// createImageExistenceMap returns a map recording on which nodes the images exist, keyed by the images' names.
func createImageExistenceMap(nodes []*v1.Node) map[string]sets.String {
	imageExistenceMap := make(map[string]sets.String)
	for _, node := range nodes {
		for _, image := range node.Status.Images {
			for _, name := range image.Names {
				if _, ok := imageExistenceMap[name]; !ok {
					imageExistenceMap[name] = sets.NewString(node.Name)
				} else {
					imageExistenceMap[name].Insert(node.Name)
				}
			}
		}
	}
	return imageExistenceMap
}

// NodeInfos returns a NodeInfoLister.
func (s *Snapshot) NodeInfos() framework.NodeInfoLister {
	return s
}

// NumNodes returns the number of nodes in the snapshot.
func (s *Snapshot) NumNodes() int {
	return len(s.nodeInfoList)
}

// List returns the list of nodes in the snapshot.
func (s *Snapshot) List() ([]*framework.NodeInfo, error) {
	return s.nodeInfoList, nil
}

// HavePodsWithAffinityList returns the list of nodes with at least one pod with inter-pod affinity
func (s *Snapshot) HavePodsWithAffinityList() ([]*framework.NodeInfo, error) {
	return s.havePodsWithAffinityNodeInfoList, nil
}

// HavePodsWithRequiredAntiAffinityList returns the list of nodes with at least one pod with
// required inter-pod anti-affinity
func (s *Snapshot) HavePodsWithRequiredAntiAffinityList() ([]*framework.NodeInfo, error) {
	return s.havePodsWithRequiredAntiAffinityNodeInfoList, nil
}

// Get returns the NodeInfo of the given node name.
func (s *Snapshot) Get(nodeName string) (*framework.NodeInfo, error) {
	if v, ok := s.nodeInfoMap[nodeName]; ok && v.Node() != nil {
		return v, nil
	}
	return nil, fmt.Errorf("nodeinfo not found for node name %q", nodeName)
}

// ========== 分区同步相关方法 ==========

// InitPartitions 初始化分区信息
// numPartitions: 分区数量
func (s *Snapshot) InitPartitions(numPartitions int) {
	// 确保分区数量至少为1，防止无效的分区数量导致后续逻辑出错
	if numPartitions < 1 {
		numPartitions = 1
	}
	
	// 设置快照的分区总数
	s.numPartitions = numPartitions
	
	// 初始化节点到分区的映射表
	// 用于快速查找指定节点属于哪个分区
	s.nodeToPartition = make(map[string]int)
	
	// 初始化分区代数切片
	// 每个分区维护自己的代数，用于跟踪分区的更新版本
	s.partitionGenerations = make([]int64, numPartitions)
	
	// 初始化分区节点列表切片
	// 每个分区存储对应的节点信息列表
	s.partitionNodeLists = make([][]*framework.NodeInfo, numPartitions)
	
	// 为每个分区预分配空的节点信息切片
	// 避免后续追加节点时频繁扩容，提高性能
	for i := 0; i < numPartitions; i++ {
		s.partitionNodeLists[i] = make([]*framework.NodeInfo, 0)
	}
}
// AssignNodeToPartition 将节点分配到分区
// 使用节点名称的哈希值来确定分区
func (s *Snapshot) AssignNodeToPartition(nodeName string) int {
	if s.numPartitions <= 1 {
		return 0
	}

	// 如果已经分配过，直接返回
	if partitionID, exists := s.nodeToPartition[nodeName]; exists {
		return partitionID
	}

	// 使用简单的哈希函数将节点分配到分区
	var hash int
	for _, c := range nodeName {
		hash += int(c)
	}
	partitionID := hash % s.numPartitions
	s.nodeToPartition[nodeName] = partitionID
	return partitionID
}

// GetNodePartition 获取节点所属的分区 ID
// 该方法首先检查节点是否已存在于分区映射中，如果存在则直接返回分区ID，
// 否则调用分配函数为节点分配分区并更新映射
func (s *Snapshot) GetNodePartition(nodeName string) int {
	// 首先从节点到分区的映射表中查找节点的分区ID
	// 如果节点已经在映射表中存在，直接返回缓存的分区ID
	if partitionID, exists := s.nodeToPartition[nodeName]; exists {
		return partitionID
	}
	
	// 如果节点不存在于映射表中，则为其分配一个新的分区ID
	// AssignNodeToPartition方法使用一致性哈希或其他分区算法来确定节点归属
	return s.AssignNodeToPartition(nodeName)
}

// GetPartitionGeneration 获取指定分区的 generation
func (s *Snapshot) GetPartitionGeneration(partitionID int) int64 {
	if partitionID >= 0 && partitionID < len(s.partitionGenerations) {
		return s.partitionGenerations[partitionID]
	}
	return 0
}

// SetPartitionGeneration 设置指定分区的 generation
func (s *Snapshot) SetPartitionGeneration(partitionID int, generation int64) {
	if partitionID >= 0 && partitionID < len(s.partitionGenerations) {
		s.partitionGenerations[partitionID] = generation
	}
}

// GetPartitionNodeList 获取指定分区的节点列表
func (s *Snapshot) GetPartitionNodeList(partitionID int) []*framework.NodeInfo {
	if partitionID >= 0 && partitionID < len(s.partitionNodeLists) {
		return s.partitionNodeLists[partitionID]
	}
	return nil
}

// RebuildPartitionNodeLists 根据 nodeInfoList 重建分区节点列表
// 该方法重新将所有的节点按照分区策略分配到对应的分区中
func (s *Snapshot) RebuildPartitionNodeLists() {
	// 如果分区数量小于等于1，表示不需要分区，直接返回
	// 单一分区或无分区的情况下无需进行节点重新分配
	if s.numPartitions <= 1 {
		return
	}

	// 清空现有的分区节点列表
	// 为每个分区创建新的空切片，准备重新分配节点
	for i := range s.partitionNodeLists {
		s.partitionNodeLists[i] = make([]*framework.NodeInfo, 0)
	}

	// 遍历快照中的所有节点信息，将它们分配到相应的分区
	for _, nodeInfo := range s.nodeInfoList {
		// 检查节点是否存在，避免空指针异常
		if nodeInfo.Node() == nil {
			continue
		}
		
		// 根据节点名称获取其所属的分区ID
		// GetNodePartition方法使用一致性哈希或其他分区算法确定节点归属
		partitionID := s.GetNodePartition(nodeInfo.Node().Name)
		
		// 验证分区ID的有效性，确保不会越界访问分区列表
		if partitionID >= 0 && partitionID < len(s.partitionNodeLists) {
			// 将节点信息添加到对应的分区列表中
			s.partitionNodeLists[partitionID] = append(s.partitionNodeLists[partitionID], nodeInfo)
		}
	}
}

// GetNumPartitions 获取分区数量
func (s *Snapshot) GetNumPartitions() int {
	return s.numPartitions
}

// IsPartitioned 返回是否启用了分区
func (s *Snapshot) IsPartitioned() bool {
	return s.numPartitions > 1
}
