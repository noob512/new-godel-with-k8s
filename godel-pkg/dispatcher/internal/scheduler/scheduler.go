/*
Copyright 2023 The Godel Scheduler Authors.

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
	schedulerapi "github.com/kubewharf/godel-scheduler-api/pkg/apis/scheduling/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GodelScheduler stores all necessary metrics about one godel scheduler.
// We do not create a lock for GodelScheduler here, it is only used by scheduler maintainer and maintainer's lock will protect this too.
// TODO: if GodelScheduler is used by multiple users, we may need to revisit this to see if we need a lock here.
type GodelScheduler struct {
// schedulerName 是该调度器实例的唯一标识名称。
	schedulerName string

	// scheduler 是 Kubernetes 调度器 API 的核心实例，负责执行实际的调度逻辑（预选、优选等）。
	scheduler *schedulerapi.Scheduler

	// active 表示该调度器当前是否处于活跃状态。
	// 如果为 true，则调度器可以接收和处理调度请求；如果为 false，则不会被调度分发器选中。
	active bool

	// nodePartitionType 指示该调度器管理的节点分区类型（物理或逻辑）。
	// 这与 SchedulerMaintainer 中的 NodePartitionType 相关，用于确定调度器的作用域。
	nodePartitionType string

	// taskSelector 是一个标签选择器，定义了哪些 Pod（任务）可以由这个调度器来调度。
	// 调度分发器（dispatcher）在将任务分发给调度器时，会检查 Pod 的标签是否与此选择器匹配。
	// 只有满足此 TaskSelector 的 Pod 才会由这个调度器负责调度。
	taskSelector metav1.LabelSelector

	// nodeSelector 是一个标签选择器，定义了哪些节点可以由这个调度器来管理。
	// 该调度器只会考虑满足此 NodeSelector 的节点来调度 Pod。
	// 这有助于实现节点隔离或特定调度策略。
	nodeSelector metav1.LabelSelector

	// TODO: Extract more fields from Scheduler CRD if necessary

	// there may be some latency to update this map after pods are scheduled
	// schedulers need to filter out scheduled pods too after dispatcher dispatching pods to them
	// TODO: if this scheduler dies, we need to get the latest state of the unscheduled pods here first and then re-dispatch still-real-unscheduled pods
	// UnscheduledPods map[string]*v1.Pod

	// nodes in this scheduler's partition
	// TODO: enrich the value of Nodes map if necessary, for example set the type to *node.NodeInfo or something like that
	// nodes 是一个映射，存储属于该调度器分区的所有节点名称。
	// 键是节点名称，值是一个空结构体（struct{}），用于实现集合（Set）的功能，表示节点的存在。
	// 该调度器只会考虑这些节点来调度 Pod。
	// TODO: enrich the value of Nodes map if necessary, for example set the type to *node.NodeInfo or something like that
	// 将来如果需要存储更丰富的节点信息（如资源容量、已分配资源等），可以将值类型改为 *node.NodeInfo 或类似结构
	nodes map[string]struct{}

	// TODO: track dispatched pods here
}

// NewGodelSchedulerWithSchedulerName create a GodelScheduler with scheduler name
func NewGodelSchedulerWithSchedulerName(schedulerName string) *GodelScheduler {
	return &GodelScheduler{
		schedulerName: schedulerName,
		// active field defaulting to true
		active: true,
		nodes:  make(map[string]struct{}),
	}
}

// NewGodelSchedulerWithSchedulerCRD creates a GodelScheduler with a scheduler CRD
func NewGodelSchedulerWithSchedulerCRD(scheduler *schedulerapi.Scheduler) *GodelScheduler {
	// TODO: initialize more fields for GodelScheduler
	// NodePartitionType ...
	return &GodelScheduler{
		schedulerName: scheduler.Name,
		// active field defaulting to true
		active:    true,
		scheduler: scheduler,
		nodes:     make(map[string]struct{}),
	}
}

func (gs *GodelScheduler) IsSchedulerActive() bool {
	return gs.active
}

func (gs *GodelScheduler) SetSchedulerActive() {
	gs.active = true
}

func (gs *GodelScheduler) SetSchedulerInActive() {
	gs.active = false
}

func (gs *GodelScheduler) SetScheduler(scheduler *schedulerapi.Scheduler) {
	gs.scheduler = scheduler
}

func (gs *GodelScheduler) GetScheduler() *schedulerapi.Scheduler {
	return gs.scheduler
}

func (gs *GodelScheduler) Clone() *GodelScheduler {
	gsClone := &GodelScheduler{
		schedulerName:     gs.schedulerName,
		scheduler:         gs.scheduler.DeepCopy(),
		nodePartitionType: gs.nodePartitionType,
		taskSelector:      gs.taskSelector,
		nodeSelector:      gs.nodeSelector,
	}
	gsClone.nodes = make(map[string]struct{})
	for nodeName := range gs.nodes {
		gsClone.nodes[nodeName] = struct{}{}
	}
	return gsClone
}

func (gs *GodelScheduler) AddNode(nodeName string) {
	gs.nodes[nodeName] = struct{}{}
}

func (gs *GodelScheduler) RemoveNode(nodeName string) {
	delete(gs.nodes, nodeName)
}

func (gs *GodelScheduler) NodeExists(nodeName string) bool {
	_, found := gs.nodes[nodeName]
	return found
}

func (gs *GodelScheduler) GetNodes() map[string]struct{} {
	return gs.nodes
}
