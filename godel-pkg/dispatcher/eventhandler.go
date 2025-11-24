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

package dispatcher

import (
	nodev1alpha1 "github.com/kubewharf/godel-scheduler-api/pkg/apis/node/v1alpha1"
	scheduling "github.com/kubewharf/godel-scheduler-api/pkg/apis/scheduling/v1alpha1"
	//nodeinformer "github.com/kubewharf/godel-scheduler-api/pkg/client/informers/externalversions/node/v1alpha1"
	schedulinginformer "github.com/kubewharf/godel-scheduler-api/pkg/client/informers/externalversions/scheduling/v1alpha1"
	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/kubewharf/godel-scheduler/pkg/dispatcher/internal/queue"
	"github.com/kubewharf/godel-scheduler/pkg/dispatcher/metrics"
	"github.com/kubewharf/godel-scheduler/pkg/features"
	frwkutils "github.com/kubewharf/godel-scheduler/pkg/framework/utils"
	podutil "github.com/kubewharf/godel-scheduler/pkg/util/pod"
	"github.com/kubewharf/godel-scheduler/pkg/util/tracing"
)

func generateUnitKeyFromPod(pod *v1.Pod) string {
	if pod.Annotations != nil && len(pod.Annotations[podutil.PodGroupNameAnnotationKey]) > 0 {
		return pod.Namespace + "/" + pod.Annotations[podutil.PodGroupNameAnnotationKey]
	}
	return ""
}

// addPodToPendingOrSortedQueue 是一个事件处理器函数，当 Informer 检测到有新的 Pending Pod 添加时被调用
// 该函数负责将 Pod 信息转换为调度队列所需的格式，并根据 Pod 是否属于某个调度单元 (Unit) 将其加入不同的队列
func (d *Dispatcher) addPodToPendingOrSortedQueue(obj interface{}) {
	// 将接收到的通用接口对象转换为 *v1.Pod 类型
	pod, err := podutil.ConvertToPod(obj)
	if err != nil {
		// 如果转换失败，记录错误日志并退出
		klog.InfoS("Failed to convert the object to *v1.Pod", "object", obj, "err", err)
		return
	}

	// 将 Pod 转换为队列内部使用的 QueuedPodInfo 结构体
	// QueuedPodInfo 包含了 Pod 本身以及调度队列管理所需的一些元数据
	podInfo, err := queue.NewQueuedPodInfo(pod)
	if err != nil {
		// 如果创建 QueuedPodInfo 失败，记录错误日志并退出
		klog.InfoS("Error occurred during NewQueuedPodInfo for pod", "pod", klog.KObj(pod), "err", err)
		return
	}

	// 检查 Pod 是否属于一个调度单元 (Unit)，例如 PodGroup
	if frwkutils.PodBelongToUnit(pod) {
		// 如果 Pod 属于某个单元
		klog.V(5).InfoS("DEBUG: added unsorted pod to unit infos", "pod", klog.KObj(pod))
		// 将 Pod 信息添加到 UnitInfos 中，标记为 "未排序" (unsorted)
		// 这意味着该 Pod 需要等待其所属单元中的其他 Pod 到达，以满足单元的调度条件（如 MinMember）
		// UnitInfos 会管理这些未排序的 Pod，并在单元就绪时将其移动到就绪队列
		d.UnitInfos.AddUnSortedPodInfo(generateUnitKeyFromPod(pod), podInfo)
		return // 处理完成，直接返回，不加入普通的待处理队列
	}
	
	// 如果 Pod 不属于任何单元，则直接将其添加到 FIFO 待处理 Pod 队列中
	// 这个队列是调度器的主要输入队列，用于存放可以被立即调度的 Pod
	d.FIFOPendingPodsQueue.AddPodInfo(podInfo)
}

func (d *Dispatcher) updatePodInPendingOrSortedQueue(old, new interface{}) {
	oldPod, ok := old.(*v1.Pod)
	if !ok {
		klog.InfoS("Failed to convert the oldObject to *v1.Pod", "oldObject", old)
		return
	}
	newPod, ok := new.(*v1.Pod)
	if !ok {
		klog.InfoS("Failed to convert the newObject to *v1.Pod", "newObject", new)
		return
	}

	newPodInfo, err := queue.NewQueuedPodInfo(newPod)
	if err != nil {
		klog.InfoS("Error occurred for NewQueuedPodInfo for pod", "pod", klog.KObj(newPod), "err", err)
		return
	}

	parentSpanContext := tracing.GetSpanContextFromPod(newPod)
	podProperty := newPodInfo.GetPodProperty()
	traceContext, _ := tracing.StartSpanForPodWithParentSpan(
		podutil.GetPodKey(newPod),
		"dispatcher::updatePodInPendingOrSortedQueue",
		parentSpanContext,
		tracing.WithDispatcherOption(),
		podProperty.ConvertToTracingTags(),
	)

	defer func() {
		go traceContext.Finish()
	}()
	klog.V(3).InfoS("Detected an Update event for the pending pod", "podResourceType", newPodInfo.PodResourceType, "pod", klog.KObj(newPod))

	oldPodInfo := &queue.QueuedPodInfo{
		PodKey:          podutil.GetPodKey(oldPod),
		PodResourceType: newPodInfo.PodResourceType,
		SpanContext:     newPodInfo.SpanContext,
		PodProperty:     podProperty,
	}

	// if the pod has been inserted into the Sorted Queue, remove the
	// pod from it
	if exist := d.SortedPodsQueue.PodInfoExist(newPodInfo); exist {
		d.SortedPodsQueue.UpdatePodInfo(newPodInfo)
		return
	}

	// always try to delete the old pod from all queues
	// the delete operation must be idempotent
	d.FIFOPendingPodsQueue.RemovePodInfo(oldPodInfo)
	klog.V(5).InfoS("DEBUG: removed unsorted pod from unit info first", "pod", klog.KObj(oldPod))
	d.UnitInfos.DeleteUnSortedPodInfo(generateUnitKeyFromPod(oldPod), oldPodInfo)

	if frwkutils.PodBelongToUnit(newPod) {
		klog.V(5).InfoS("DEBUG: added unsorted pod back to unit info", "pod", klog.KObj(newPod))
		d.UnitInfos.AddUnSortedPodInfo(generateUnitKeyFromPod(newPod), newPodInfo)
		return
	}

	d.FIFOPendingPodsQueue.AddPodInfo(newPodInfo)
}

func (d *Dispatcher) deletePodFromPendingOrSortedQueue(obj interface{}) {
	pod, err := podutil.ConvertToPod(obj)
	if err != nil {
		klog.InfoS("Failed to delete pod from pending or sorted queue", "err", err)
		return
	}

	podInfo, err := queue.NewQueuedPodInfo(pod)
	if err != nil {
		klog.InfoS("Error occurred for NewQueuedPodInfo for pod", "pod", klog.KObj(pod), "err", err)
		return
	}
	klog.V(3).InfoS("Detected a Delete event for the pending pod", "podResourceType", podInfo.PodResourceType, "pod", klog.KObj(pod))

	// if the pod has been inserted into the Sorted Queue, remove the
	// pod from it
	if exist := d.SortedPodsQueue.PodInfoExist(podInfo); exist {
		d.SortedPodsQueue.RemovePodInfo(podInfo)
		return
	}

	// if the pod is associated with a unit, which is not ready, remove it
	// from the corresponding pending unit.
	if frwkutils.PodBelongToUnit(pod) {
		d.UnitInfos.DeleteUnSortedPodInfo(generateUnitKeyFromPod(pod), podInfo)
		return
	}

	d.FIFOPendingPodsQueue.RemovePodInfo(podInfo)
}

func (d *Dispatcher) addPodToDispatchedInfo(obj interface{}) {
	pod, err := podutil.ConvertToPod(obj)
	if err != nil {
		klog.InfoS("Failed to add pod to dispatched", "err", err)
		return
	}
	klog.V(3).InfoS("Detected an Add event for the dispatched pod", "pod", klog.KObj(pod))

	d.DispatchInfo.AddPod(pod)

	if podutil.DispatchedPodOfGodel(pod, d.SchedulerName) {
		schedulerName := pod.Annotations[podutil.SchedulerAnnotationKey]
		metrics.PodsInPartitionSizeInc(schedulerName, string(podutil.PodDispatched))
	}
}

func (d *Dispatcher) deletePodFromDispatchedInfo(obj interface{}) {
	pod, err := podutil.ConvertToPod(obj)
	if err != nil {
		klog.InfoS("Failed to delete the dispatched pod", "err", err)
		return
	}

	klog.V(3).InfoS("Detected a Delete event for the dispatched pod", "pod", klog.KObj(pod))
	d.DispatchInfo.RemovePod(pod)

	if podutil.DispatchedPodOfGodel(pod, d.SchedulerName) {
		schedulerName := pod.Annotations[podutil.SchedulerAnnotationKey]
		metrics.PodsInPartitionSizeDec(schedulerName, string(podutil.PodDispatched))
	}
}

func (d *Dispatcher) addPodToOwnerInfo(obj interface{}) {
	pod, err := podutil.ConvertToPod(obj)
	if err != nil {
		klog.InfoS("Failed to add pod to owner info", "err", err)
		return
	}
	schedulerName := podutil.GetSchedulerNameForPod(pod)
	d.OwnerInfos.AddDispatchedUnboundPod(pod, schedulerName)
}

func (d *Dispatcher) deletePodFromOwnerInfo(obj interface{}) {
	pod, err := podutil.ConvertToPod(obj)
	if err != nil {
		klog.InfoS("Failed to delete pod from owner info", "err", err)
		return
	}
	d.OwnerInfos.DeleteDispatchedUnboundPod(pod)
}

// AddAllEventHandlers 为 Dispatcher 注册所有必要的 Kubernetes 资源事件处理器。
// 这些处理器监听 Pod、Scheduler、Node、NMNode、PodGroup 等资源的变化，
// 并调用 Dispatcher 内部的相应方法来更新其内部状态、队列或信息存储。
//
// 参数:
//   - dispatcher: 需要添加事件处理器的 Dispatcher 实例指针。
//   - podInformer: Pod Informer，用于监听 Pod 资源变化。
//   - schedulerInformer: Scheduler Informer，用于监听 Scheduler CRD 资源变化。
//   - nodeInformer: Node Informer，用于监听 Node 资源变化。
//   - nmNodeInformer: NMNode Informer，用于监听 NMNode CRD 资源变化。
//   - podGroupInformer: PodGroup Informer，用于监听 PodGroup CRD 资源变化。
func AddAllEventHandlers(
	dispatcher *Dispatcher,
	podInformer coreinformers.PodInformer,
	schedulerInformer schedulinginformer.SchedulerInformer,
	nodeInformer coreinformers.NodeInformer,
) {
	// 1. 为 Pod Informer 添加事件处理器：处理 "等待调度" (Pending) 的 Pod
	// 这个处理器过滤出属于当前 Godel 调度器且状态为 Pending 的 Pod。
	podInformer.Informer().AddEventHandler(
		// 使用 FilteringResourceEventHandler 来先过滤对象，再处理。
		cache.FilteringResourceEventHandler{
			// FilterFunc 定义了哪些对象需要被处理。
			FilterFunc: func(obj interface{}) bool {
				klog.Info("触发过滤器-1")
				switch t := obj.(type) {
				case *v1.Pod:
					// 检查 Pod 是否是当前调度器负责的 Pending Pod。
					return podutil.PendingPodOfGodel(t, dispatcher.SchedulerName)
				case cache.DeletedFinalStateUnknown:
					// 处理删除事件时可能遇到的 DeletedFinalStateUnknown 类型。
					// 尝试从中提取 Pod 对象并进行检查。
					if pod, ok := t.Obj.(*v1.Pod); ok {
						return podutil.PendingPodOfGodel(pod, dispatcher.SchedulerName)
					}
					// 如果无法转换，记录日志并返回 false（不处理）。
					klog.InfoS("Failed to convert object to *v1.Pod", "object", obj, "component", dispatcher)
					return false
				default:
					// 处理意外的类型，记录日志并返回 false。
					klog.InfoS("Failed to handle object", "component", dispatcher, "object", obj)
					return false
				}
			},
			// Handler 定义了具体的事件处理逻辑。
			Handler: cache.ResourceEventHandlerFuncs{
				// Pod 添加事件：将 Pod 添加到待调度队列。
				AddFunc: dispatcher.addPodToPendingOrSortedQueue,
				// Pod 更新事件：更新待调度队列中的 Pod 信息。
				UpdateFunc: dispatcher.updatePodInPendingOrSortedQueue,
				// Pod 删除事件：从待调度队列中移除 Pod。
				DeleteFunc: dispatcher.deletePodFromPendingOrSortedQueue,
			},
		},
	)

	// 2. 为 Pod Informer 添加事件处理器：处理 "已调度" (Dispatched) 的 Pod
	// 这个处理器过滤出属于当前 Godel 调度器且状态为 Dispatched 的 Pod。
	podInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				klog.Info("触发过滤器-2")
				switch t := obj.(type) {
				case *v1.Pod:
					// 检查 Pod 是否是当前调度器负责的 Dispatched Pod。
					return podutil.DispatchedPodOfGodel(t, dispatcher.SchedulerName)
				case cache.DeletedFinalStateUnknown:
					if pod, ok := t.Obj.(*v1.Pod); ok {
						return podutil.DispatchedPodOfGodel(pod, dispatcher.SchedulerName)
					}
					klog.InfoS("Failed to convert object to *v1.Pod", "object", obj, "component", dispatcher)
					return false
				default:
					klog.InfoS("Failed to handle object", "component", dispatcher, "object", obj)
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				// Pod 添加事件：将 Pod 信息添加到调度信息存储中。
				AddFunc: dispatcher.addPodToDispatchedInfo,
				// Pod 删除事件：从调度信息存储中移除 Pod 信息。
				DeleteFunc: dispatcher.deletePodFromDispatchedInfo,
			},
		},
	)

	// 4. 为 Pod Informer 添加事件处理器：处理 "异常状态" 的 Pod
	// 这个处理器不使用过滤器，它会处理所有 Pod 的 Add 和 Update 事件，
	// 用于检测和管理处于异常状态的 Pod。
	podInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			// Pod 添加事件：尝试将新 Pod 添加到异常队列（如果它处于异常状态）。
			AddFunc: dispatcher.addPodToAbnormalQueue,
			// Pod 更新事件：检查 Pod 状态变化，更新异常队列。
			UpdateFunc: dispatcher.updatePodInAbnormalQueue,
		},
	)

	// 5. 为 Scheduler Informer 添加事件处理器：处理 Scheduler CRD 资源变化
	// 监听 Scheduler 资源的添加、更新和删除事件。
	schedulerInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			// Scheduler 添加事件。
			AddFunc: dispatcher.addScheduler,
			// Scheduler 更新事件。
			UpdateFunc: dispatcher.updateScheduler,
			// Scheduler 删除事件。
			DeleteFunc: dispatcher.deleteScheduler,
		},
	)

	// // 6. 为 Pod Informer 添加事件处理器：更新 "Pod 单元信息" (UnitInfos)
	// // 监听 Pod 的添加和删除事件，以更新 Pod 单元信息。
	// podInformer.Informer().AddEventHandler(
	// 	cache.ResourceEventHandlerFuncs{
	// 		// Pod 添加事件：更新 UnitInfos。
	// 		AddFunc: dispatcher.addPodToUnitInfos,
	// 		// Pod 删除事件：更新 UnitInfos。
	// 		DeleteFunc: dispatcher.deletePodFromUnitInfos,
	// 	},
	// )

	// 8. 为 Node 和 NMNode Informer 添加事件处理器：处理 "节点洗牌" 特性相关的事件
	// 这些处理器仅在 "DispatcherNodeShuffle" 特性门控启用时才添加。
	if utilfeature.DefaultFeatureGate.Enabled(features.DispatcherNodeShuffle) {
		// 监听 Node 资源的添加、更新和删除事件。
		nodeInformer.Informer().AddEventHandler(
			cache.ResourceEventHandlerFuncs{
				// Node 添加事件。
				AddFunc: dispatcher.addNode,
				// Node 更新事件。
				UpdateFunc: dispatcher.updateNode,
				// Node 删除事件。
				DeleteFunc: dispatcher.deleteNode,
			},
		)
	}
}

// addScheduler adds the info of a scheduler instance to the dispatcher cache.
func (d *Dispatcher) addScheduler(obj interface{}) {
	scheduler, ok := obj.(*scheduling.Scheduler)
	if !ok {
		klog.InfoS("Failed to convert object to *scheduling.Scheduler", "object", obj)
		return
	}

	klog.V(3).InfoS("Started to add scheduler", "schedulerName", scheduler.Name)

	d.DispatchInfo.AddScheduler(scheduler.Name)
	d.maintainer.AddScheduler(scheduler)
}

func (d *Dispatcher) updateScheduler(oldObj, newObj interface{}) {
	oldScheduler, ok := oldObj.(*scheduling.Scheduler)
	if !ok {
		klog.InfoS("Failed to convert oldObj to *scheduling.Scheduler", "oldObject", oldObj)
		return
	}
	newScheduler, ok := newObj.(*scheduling.Scheduler)
	if !ok {
		klog.InfoS("Failed to convert newObj to *scheduling.Scheduler", "newObject", newObj)
		return
	}

	d.maintainer.UpdateScheduler(oldScheduler, newScheduler)

	klog.V(3).InfoS("Updated scheduler", "schedulerName", oldScheduler.Name)
}

func (d *Dispatcher) deleteScheduler(obj interface{}) {
	var scheduler *scheduling.Scheduler
	switch t := obj.(type) {
	case *scheduling.Scheduler:
		scheduler = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		scheduler, ok = t.Obj.(*scheduling.Scheduler)
		if !ok {
			klog.InfoS("Failed to convert to *scheduling.Scheduler", "object", t.Obj)
			return
		}
	default:
		klog.InfoS("Failed to convert to *scheduling.Scheduler", "object", t)
		return
	}

	klog.V(3).InfoS("Started to delete scheduler", "schedulerName", scheduler.Name)

	d.DispatchInfo.DeleteScheduler(scheduler.Name)
	d.maintainer.DeleteScheduler(scheduler)
	d.reconciler.DeleteScheduler(scheduler)
}

func (d *Dispatcher) addPodToUnitInfos(obj interface{}) {
	pod, err := podutil.ConvertToPod(obj)
	if err != nil {
		klog.InfoS("Failed to add pod to unit info", "err", err)
		return
	}
	klog.V(5).InfoS("Added pod to unit infos", "pod", klog.KObj(pod))
	d.UnitInfos.AddPod(generateUnitKeyFromPod(pod), podutil.GetPodKey(pod))
}

func (d *Dispatcher) deletePodFromUnitInfos(obj interface{}) {
	pod, err := podutil.ConvertToPod(obj)
	if err != nil {
		klog.InfoS("Failed to delete pod from unit info", "err", err)
		return
	}
	d.UnitInfos.DeletePod(generateUnitKeyFromPod(pod), podutil.GetPodKey(pod))
}

func (d *Dispatcher) addPodGroupToUnitInfos(obj interface{}) {
	pg, ok := obj.(*scheduling.PodGroup)
	if !ok {
		klog.InfoS("Failed to convert obj to *scheduling.PodGroup", "object", obj)
		return
	}

	klog.V(4).InfoS("Started to handle Add event for PodGroup",
		"podGroup", klog.KObj(pg))
	d.UnitInfos.AddPodGroup(pg)
}

func (d *Dispatcher) updatePodGroupInUnitInfos(oldObj, newObj interface{}) {
	old, ok := oldObj.(*scheduling.PodGroup)
	if !ok {
		klog.InfoS("Failed to convert oldObj to *scheduling.PodGroup", "oldObject", oldObj)
		return
	}
	new, ok := newObj.(*scheduling.PodGroup)
	if !ok {
		klog.InfoS("Failed to convert newObj to *scheduling.PodGroup", "newObject", newObj)
		return
	}
	klog.V(4).InfoS("Started to handle Update event for PodGroup",
		"podGroup", klog.KObj(new))
	d.UnitInfos.UpdatePodGroup(old, new)
}

func (d *Dispatcher) deletePodGroupFromUnitInfos(obj interface{}) {
	var podGroup *scheduling.PodGroup
	switch t := obj.(type) {
	case *scheduling.PodGroup:
		podGroup = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		podGroup, ok = t.Obj.(*scheduling.PodGroup)
		if !ok {
			klog.InfoS("Failed to convert object to *scheduling.PodGroup", "object", t.Obj)
			return
		}
	default:
		klog.InfoS("Failed to convert to *scheduling.PodGroup", "object", t)
		return
	}

	klog.V(4).InfoS("Started to handle the Delete event for the PodGroup",
		"podGroup", klog.KObj(podGroup))
	d.UnitInfos.DeletePodGroup(podGroup)
}

func (d *Dispatcher) addNode(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		klog.InfoS("Failed to convert to *v1.Node", "object", obj)
		return
	}

	klog.V(4).InfoS("Started to add the node", "node", klog.KObj(node))
	d.maintainer.AddNodeToGodelSchedulerIfNotPresent(node)
	if utilfeature.DefaultFeatureGate.Enabled(features.DispatcherNodeShuffle) {
		d.shuffler.AddNode(node)
	}
}

func (d *Dispatcher) updateNode(oldObj, newObj interface{}) {
	oldNode, ok := oldObj.(*v1.Node)
	if !ok {
		klog.InfoS("Failed to convert oldObj to *v1.Node", "oldObject", oldObj)
		return
	}
	newNode, ok := newObj.(*v1.Node)
	if !ok {
		klog.InfoS("Failed to convert newObj to *v1.Node", "newObject", newObj)
		return
	}

	klog.V(4).InfoS("Started to update node", "node", klog.KObj(oldNode))
	d.maintainer.UpdateNodeInGodelSchedulerIfNecessary(oldNode, newNode)
	if utilfeature.DefaultFeatureGate.Enabled(features.DispatcherNodeShuffle) {
		d.shuffler.UpdateNode(oldNode, newNode)
	}
}

func (d *Dispatcher) deleteNode(obj interface{}) {
	var node *v1.Node
	switch t := obj.(type) {
	case *v1.Node:
		node = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		node, ok = t.Obj.(*v1.Node)
		if !ok {
			klog.InfoS("Failed to convert to *v1.Node", "object", t.Obj)
			return
		}
	default:
		klog.InfoS("Failed to convert to *v1.Node", "object", t)
		return
	}

	klog.V(4).InfoS("Started to delete node", "node", klog.KObj(node))
	d.maintainer.DeleteNodeFromGodelScheduler(node)
}

func (d *Dispatcher) addNMNode(obj interface{}) {
	nmNode, ok := obj.(*nodev1alpha1.NMNode)
	if !ok {
		klog.InfoS("Failed to convert to *nodev1alpha1.NMNode", "object", obj)
		return
	}

	klog.V(3).InfoS("Started to add nmNode", "nmNode", nmNode.Name)
	d.maintainer.AddNMNodeToGodelSchedulerIfNotPresent(nmNode)
	if utilfeature.DefaultFeatureGate.Enabled(features.DispatcherNodeShuffle) {
		d.shuffler.AddNMNode(nmNode)
	}
}

func (d *Dispatcher) updateNMNode(oldObj, newObj interface{}) {
	oldNMNode, ok := oldObj.(*nodev1alpha1.NMNode)
	if !ok {
		klog.InfoS("Failed to convert oldObj to *nodev1alpha1.NMNode", "oldObject", oldObj)
		return
	}
	newNMNode, ok := newObj.(*nodev1alpha1.NMNode)
	if !ok {
		klog.InfoS("Failed to convert newObj to *nodev1alpha1.NMNode", "newObject", newObj)
		return
	}

	klog.V(3).InfoS("Started to update nmNode", "nmNode", oldNMNode.Name)
	d.maintainer.UpdateNMNodeInGodelSchedulerIfNecessary(oldNMNode, newNMNode)
	if utilfeature.DefaultFeatureGate.Enabled(features.DispatcherNodeShuffle) {
		d.shuffler.UpdateNMNode(oldNMNode, newNMNode)
	}
}

func (d *Dispatcher) deleteNMNode(obj interface{}) {
	var nmNode *nodev1alpha1.NMNode
	switch t := obj.(type) {
	case *nodev1alpha1.NMNode:
		nmNode = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		nmNode, ok = t.Obj.(*nodev1alpha1.NMNode)
		if !ok {
			klog.InfoS("Failed to convert to *nodev1alpha1.NMNode", "object", t.Obj)
			return
		}
	default:
		klog.InfoS("Failed to convert to *nodev1alpha1.NMNode", "object", t)
		return
	}

	klog.V(3).InfoS("Started to delete nmNode", "nmNode", nmNode.Name)
	d.maintainer.DeleteNMNodeFromGodelScheduler(nmNode)
}

func (d *Dispatcher) addPodToAbnormalQueue(obj interface{}) {
	pod, err := podutil.ConvertToPod(obj)
	if err != nil {
		klog.InfoS("Failed to add pod to abnormal queue", "err", err)
		return
	}

	if abnormal := podutil.AbnormalPodStateOfGodel(pod, d.SchedulerName); abnormal {
		podKey, err := cache.MetaNamespaceKeyFunc(pod)
		if err == nil {
			d.reconciler.AbnormalPodsEnqueue(podKey)
		} else {
			klog.InfoS("Failed to get key for pod", "pod", klog.KObj(pod), "err", err)
		}
	}
}

func (d *Dispatcher) updatePodInAbnormalQueue(_, newObj interface{}) {
	newPod, err := podutil.ConvertToPod(newObj)
	if err != nil {
		klog.InfoS("Failed to add pod to dispatched", "err", err)
		return
	}
	if abnormal := podutil.AbnormalPodStateOfGodel(newPod, d.SchedulerName); abnormal {
		podKey, err := cache.MetaNamespaceKeyFunc(newPod)
		if err == nil {
			d.reconciler.AbnormalPodsEnqueue(podKey)
		} else {
			klog.InfoS("Failed to get key for pod", "pod", klog.KObj(newPod), "err", err)
		}
	}
}
