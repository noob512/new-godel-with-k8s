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

package scheduler

import (
	"fmt"
	"reflect"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	//utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	nodev1alpha1 "github.com/kubewharf/godel-scheduler-api/pkg/apis/node/v1alpha1"
	schedulingv1a1 "github.com/kubewharf/godel-scheduler-api/pkg/apis/scheduling/v1alpha1"
	crdinformers "github.com/kubewharf/godel-scheduler-api/pkg/client/informers/externalversions"
	//godelfeatures "github.com/kubewharf/godel-scheduler/pkg/features"
	framework "github.com/kubewharf/godel-scheduler/pkg/framework/api"
	"github.com/kubewharf/godel-scheduler/pkg/util"
	//"github.com/kubewharf/godel-scheduler/pkg/util/features"
	podutil "github.com/kubewharf/godel-scheduler/pkg/util/pod"
	unitutil "github.com/kubewharf/godel-scheduler/pkg/util/unit"
	katalystv1alpha1 "github.com/kubewharf/katalyst-api/pkg/apis/node/v1alpha1"
	katalystinformers "github.com/kubewharf/katalyst-api/pkg/client/informers/externalversions"
)

func (sched *Scheduler) onPvAdd(obj interface{}) {
	// Pods created when there are no PVs available will be stuck in
	// unschedulable queue. But unbound PVs created for static provisioning and
	// delay binding storage class are skipped in PV controller dynamic
	// provisioning and binding process, will not trigger events to schedule pod
	// again. So we need to move pods to active queue on PV add for this
	// scenario.
	sched.ScheduleSwitch.Process(
		// TODO: Parse SwitchType for PV
		framework.SwitchTypeAll,
		func(dataSet ScheduleDataSet) {
			dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.PvAdd)
		},
	)
}

func (sched *Scheduler) onPvUpdate(old, new interface{}) {
	// Scheduler.bindVolumesWorker may fail to update assumed pod volume
	// bindings due to conflicts if PVs are updated by PV controller or other
	// parties, then scheduler will add pod back to unschedulable queue. We
	// need to move pods to active queue on PV update for this scenario.
	sched.ScheduleSwitch.Process(
		// TODO: Parse SwitchType for PV
		framework.SwitchTypeAll,
		func(dataSet ScheduleDataSet) {
			dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.PvUpdate)
		},
	)
}

func (sched *Scheduler) onPvcAdd(obj interface{}) {
	sched.ScheduleSwitch.Process(
		// TODO: Parse SwitchType for PV
		framework.SwitchTypeAll,
		func(dataSet ScheduleDataSet) {
			dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.PvcAdd)
		},
	)
}

func (sched *Scheduler) onPvcUpdate(old, new interface{}) {
	sched.ScheduleSwitch.Process(
		// TODO: Parse SwitchType for PVC
		framework.SwitchTypeAll,
		func(dataSet ScheduleDataSet) {
			dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.PvcUpdate)
		},
	)
}

func (sched *Scheduler) onStorageClassAdd(obj interface{}) {
	sc, ok := obj.(*storagev1.StorageClass)
	if !ok {
		klog.InfoS("Failed to convert to *storagev1.StorageClass", "object", obj)
		return
	}

	// CheckVolumeBindingPred fails if pod has unbound immediate PVCs. If these
	// PVCs have specified StorageClass name, creating StorageClass objects
	// with late binding will cause predicates to pass, so we need to move pods
	// to active queue.
	// We don't need to invalidate cached results because results will not be
	// cached for pod that has unbound immediate PVCs.
	if sc.VolumeBindingMode != nil && *sc.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer {
		sched.ScheduleSwitch.Process(
			// TODO: Parse SwitchType for StorageClass
			framework.SwitchTypeAll,
			func(dataSet ScheduleDataSet) {
				dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.StorageClassAdd)
			},
		)
	}
}

func (sched *Scheduler) onServiceAdd(obj interface{}) {
	sched.ScheduleSwitch.Process(
		// TODO: Parse SwitchType for Service
		framework.SwitchTypeAll,
		func(dataSet ScheduleDataSet) {
			dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.ServiceAdd)
		},
	)
}

func (sched *Scheduler) onServiceUpdate(oldObj interface{}, newObj interface{}) {
	sched.ScheduleSwitch.Process(
		// TODO: Parse SwitchType for Service
		framework.SwitchTypeAll,
		func(dataSet ScheduleDataSet) {
			dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.ServiceUpdate)
		},
	)
}

func (sched *Scheduler) onServiceDelete(obj interface{}) {
	sched.ScheduleSwitch.Process(
		// TODO: Parse SwitchType for Service
		framework.SwitchTypeAll,
		func(dataSet ScheduleDataSet) {
			dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.ServiceDelete)
		},
	)
}

func (sched *Scheduler) onCSINodeAdd(obj interface{}) {
	sched.ScheduleSwitch.Process(
		// TODO: Parse SwitchType for CSI
		framework.SwitchTypeAll,
		func(dataSet ScheduleDataSet) {
			dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.CSINodeAdd)
		},
	)
}

func (sched *Scheduler) onCSINodeUpdate(oldObj, newObj interface{}) {
	sched.ScheduleSwitch.Process(
		// TODO: Parse SwitchType for CSI
		framework.SwitchTypeAll,
		func(dataSet ScheduleDataSet) {
			dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.CSINodeUpdate)
		},
	)
}

// update nodes within scheduler api
func (sched *Scheduler) onSchedulerUpdate(_, _ interface{}) {
	sched.ScheduleSwitch.Process(
		// TODO: Parse SwitchType for Scheduler
		framework.SwitchTypeAll,
		func(dataSet ScheduleDataSet) {},
	)
}

func (sched *Scheduler) addNodeToCache(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		klog.InfoS("Failed to convert to *v1.Node", "object", obj)
		return
	}

	klog.V(3).InfoS("Detected an Add event for node", "node", node.Name)

	if err := sched.commonCache.AddNode(node); err != nil {
		klog.InfoS("Failed to add node to scheduler cache", "err", err)
		return
	}

	sched.ScheduleSwitch.Process(
		ParseSwitchTypeForNode(node),
		func(dataSet ScheduleDataSet) {
			// TODO: revisit this.
			// Comment out this if-condition for now and remove this logic when the physical is completely removed.
			// if sched.nodeManagedByThisScheduler(node.Name) {
			dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.NodeAdd)
			// }
		},
	)
}

func (sched *Scheduler) updateNodeInCache(oldObj, newObj interface{}) {
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

	klog.V(3).InfoS("Detected an update event for node", "node", oldNode.Name)

	if err := sched.commonCache.UpdateNode(oldNode, newNode); err != nil {
		klog.InfoS("Failed to update node in scheduler cache", "err", err)
	}

	sched.ScheduleSwitch.Process(
		ParseSwitchTypeForNode(newNode),
		func(dataSet ScheduleDataSet) {
			// Only activate unschedulable pods if the node became more schedulable.
			// We skip the node property comparison when there is no unschedulable pods in the queue
			// to save processing cycles. We still trigger a move to active queue to cover the case
			// that a pod being processed by the scheduler is determined unschedulable. We want this
			// pod to be reevaluated when a change in the cluster happens.
			// Because pod preemption among all nodes, we should trigger a move as well.
			if dataSet.SchedulingQueue().NumUnschedulableUnits() == 0 {
				return
			} else if event := nodeSchedulingPropertiesChange(newNode, oldNode); event != "" {
				klog.V(3).InfoS("Detected an Update event for node", "node", newNode.Name, "type", dataSet.Type())
				// TODO: revisit this.
				// Comment out this if-condition for now and remove this logic when the physical is completely removed.
				// if sched.nodeManagedByThisScheduler(newNode.Name) {
				dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(event)
				// }
			}
		},
	)
}

func (sched *Scheduler) deleteNodeFromCache(obj interface{}) {
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
		klog.InfoS("Failed to convert to *v1.Node", "type", t)
		return
	}
	klog.V(3).InfoS("Detected a Delete event for node", "node", node.Name)

	if err := sched.commonCache.DeleteNode(node); err != nil {
		klog.InfoS("Failed to remove node from Scheduler cache", "err", err)
	}
}

func (sched *Scheduler) addNMNodeToCache(obj interface{}) {
	nmNode, ok := obj.(*nodev1alpha1.NMNode)
	if !ok {
		klog.InfoS("Failed to convert to *nodev1alpha1.NMNode", "object", obj)
		return
	}

	klog.V(3).InfoS("Detected an Add event for nmNode", "node", nmNode.Name)

	if err := sched.commonCache.AddNMNode(nmNode); err != nil {
		klog.InfoS("Failed to add NMNode to Scheduler cache", "err", err)
		return
	}

	sched.ScheduleSwitch.Process(
		ParseSwitchTypeForNMNode(nmNode),
		func(dataSet ScheduleDataSet) {
			// TODO: revisit this.
			// Comment out this if-condition for now and remove this logic when the physical is completely removed.
			// if sched.nodeManagedByThisScheduler(nmNode.Name) {
			dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.NMNodeAdd)
			// }
		},
	)
}

func (sched *Scheduler) updateNMNodeInCache(oldObj, newObj interface{}) {
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

	klog.V(3).InfoS("Detected an update event for node", "nmnode", oldNMNode.Name)

	if err := sched.commonCache.UpdateNMNode(oldNMNode, newNMNode); err != nil {
		klog.InfoS("Failed to update NMNode in Scheduler cache", "err", err)
	}

	sched.ScheduleSwitch.Process(
		ParseSwitchTypeForNMNode(newNMNode),
		func(dataSet ScheduleDataSet) {
			// Only activate unschedulable pods if the node became more schedulable.
			// We skip the node property comparison when there is no unschedulable pods in the queue
			// to save processing cycles. We still trigger a move to active queue to cover the case
			// that a pod being processed by the scheduler is determined unschedulable. We want this
			// pod to be reevaluated when a change in the cluster happens.
			// Because pod preemption among all nodes, we should trigger a move as well.
			if dataSet.SchedulingQueue().NumUnschedulableUnits() == 0 {
				return
			} else if event := nmNodeSchedulingPropertiesChange(newNMNode, oldNMNode); event != "" {
				klog.V(3).InfoS("Detected an Update event for nmNode", "nmNode", newNMNode.Name, "type", dataSet.Type())
				// TODO: revisit this.
				// Comment out this if-condition for now and remove this logic when the physical is completely removed.
				// if sched.nodeManagedByThisScheduler(newNMNode.Name) {
				dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(event)
				// }
			}
		},
	)
}

func (sched *Scheduler) deleteNMNodeFromCache(obj interface{}) {
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
		klog.InfoS("Failed to convert to *nodev1alpha1.NMNode", "type", t)
		return
	}
	klog.V(3).InfoS("Detected a Delete event for nmNode", "nmNode", nmNode.Name)

	if err := sched.commonCache.DeleteNMNode(nmNode); err != nil {
		klog.InfoS("Failed to remove NMNode from Scheduler cache", "err", err)
	}
}

func (sched *Scheduler) addCNRToCache(obj interface{}) {
	cnr, ok := obj.(*katalystv1alpha1.CustomNodeResource)
	if !ok {
		klog.InfoS("Failed to convert to *katalystv1alpha1.CustomNodeResource", "object", obj)
		return
	}

	klog.V(3).InfoS("Detected an add event", "cnr", cnr.Name)

	if err := sched.commonCache.AddCNR(cnr); err != nil {
		klog.InfoS("Failed to add CNR to Scheduler cache", "err", err)
		return
	}

	sched.ScheduleSwitch.Process(
		ParseSwitchTypeForCNR(cnr),
		func(dataSet ScheduleDataSet) {
			// TODO: revisit this.
			// Comment out this if-condition for now and remove this logic when the physical is completely removed.
			// if sched.nodeManagedByThisScheduler(cnr.Name) {
			klog.V(3).InfoS("Detected an Add event for cnr", "cnr", cnr.Name, "type", dataSet.Type())
			dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.CNRAdd)
			// }
		},
	)
}

func (sched *Scheduler) updateCNRInCache(oldObj, newObj interface{}) {
	oldCNR, ok := oldObj.(*katalystv1alpha1.CustomNodeResource)
	if !ok {
		klog.InfoS("Failed to convert oldObj to *katalystv1alpha1.CustomNodeResource", "oldObject", oldObj)
		return
	}
	newCNR, ok := newObj.(*katalystv1alpha1.CustomNodeResource)
	if !ok {
		klog.InfoS("Failed to convert newObj to *katalystv1alpha1.CustomNodeResource", "newObject", newObj)
		return
	}

	klog.V(3).InfoS("Detected an update event for cnr", "cnr", oldCNR.Name)

	if err := sched.commonCache.UpdateCNR(oldCNR, newCNR); err != nil {
		klog.InfoS("Failed to update CNR in scheduler cache", "err", err)
	}

	sched.ScheduleSwitch.Process(
		ParseSwitchTypeForCNR(newCNR),
		func(dataSet ScheduleDataSet) {
			// Only activate unschedulable pods if the node became more schedulable.
			// We skip the node property comparison when there is no unschedulable pods in the queue
			// to save processing cycles. We still trigger a move to active queue to cover the case
			// that a pod being processed by the scheduler is determined unschedulable. We want this
			// pod to be reevaluated when a change in the cluster happens.
			// Because pod preemption among all nodes, we should trigger a move as well.
			if dataSet.SchedulingQueue().NumUnschedulableUnits() == 0 {
				return
			} else if event := cnrSchedulingPropertiesChanged(newCNR, oldCNR); event != "" {
				klog.V(3).InfoS("Detected an Update event for cnr", "cnr", newCNR.Name, "type", dataSet.Type())
				// TODO: revisit this.
				// Comment out this if-condition for now and remove this logic when the physical is completely removed.
				// if sched.nodeManagedByThisScheduler(newCNR.Name) {
				dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(event)
				// }
			}
		},
	)
}

func (sched *Scheduler) deleteCNRFromCache(obj interface{}) {
	var cnr *katalystv1alpha1.CustomNodeResource
	switch t := obj.(type) {
	case *katalystv1alpha1.CustomNodeResource:
		cnr = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		cnr, ok = t.Obj.(*katalystv1alpha1.CustomNodeResource)
		if !ok {
			klog.InfoS("Failed to convert to *katalystv1alpha1.CustomNodeResource", "object", t.Obj)
			return
		}
	default:
		klog.InfoS("Failed to convert to *katalystv1alpha1.CustomNodeResource", "type", t)
		return
	}
	klog.V(3).InfoS("Detected a Delete event for cnr", "cnr", cnr.Name)

	if err := sched.commonCache.DeleteCNR(cnr); err != nil {
		klog.InfoS("Failed to remove CNR from scheduler cache", "err", err)
	}
}

func (sched *Scheduler) addPodGroupToCache(obj interface{}) {
	podGroup, ok := obj.(*schedulingv1a1.PodGroup)
	if !ok {
		klog.InfoS("Failed to convert obj to *v1alpha1.PodGroup", "object", obj)
		return
	}

	klog.V(3).InfoS("Detected an Add event for pod group", "podGroup", klog.KObj(podGroup))

	if err := sched.commonCache.AddPodGroup(podGroup); err != nil {
		klog.InfoS("Failed to add pod group to scheduler cache", "err", err)
		return
	}

	sched.ScheduleSwitch.Process(
		// TODO: Parse SwitchType for PodGroup
		framework.SwitchTypeAll,
		func(dataSet ScheduleDataSet) {
			// Pods created when there are no PodGroup available will be stuck in
			// unschedulable queue. Since job controller almost create pod group and pods at the same time,
			// it will not trigger events to schedule pod again if they are failed at PreFilter phase.
			// So we need to move pods to active queue on PodGroupUpdate for this scenario.
			dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.PodGroupAdd)
			dataSet.SchedulingQueue().ActivePodGroupUnit(unitutil.GetPodGroupKey(podGroup))
		},
	)
}

func (sched *Scheduler) updatePodGroupToCache(oldObj interface{}, newObj interface{}) {
	oldPodGroup, ok := oldObj.(*schedulingv1a1.PodGroup)
	if !ok {
		klog.InfoS("Failed to convert oldObj to *v1alpha1.PodGroup", "oldObject", oldObj)
		return
	}
	newPodGroup, ok := newObj.(*schedulingv1a1.PodGroup)
	if !ok {
		klog.InfoS("Failed to convert newObj to *v1alpha1.PodGroup", "newObject", newObj)
		return
	}

	if oldPodGroup.UID != newPodGroup.UID {
		sched.deletePodGroupFromCache(oldPodGroup)
		sched.addPodGroupToCache(oldPodGroup)
	}

	klog.V(3).InfoS("Detected an update event for pod group", "podGroup", klog.KObj(newPodGroup))

	if err := sched.commonCache.UpdatePodGroup(oldPodGroup, newPodGroup); err != nil {
		klog.InfoS("Failed to update pod group in scheduler cache", "err", err)
		return
	}

	sched.ScheduleSwitch.Process(
		// TODO: Parse SwitchType for PodGroup
		framework.SwitchTypeAll,
		func(dataSet ScheduleDataSet) {
			// Pods created with the associated timeout PodGroup will be stuck in
			// unschedulable queue. Since owner may change pod group status later,
			// it will not trigger events to schedule pod again if they are failed at PreFilter phase.
			// So we need to move pods to active queue on PodGroupUpdate for this scenario.
			dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.PodGroupUpdate)
			dataSet.SchedulingQueue().ActivePodGroupUnit(unitutil.GetPodGroupKey(newPodGroup))
		},
	)
}

func (sched *Scheduler) deletePodGroupFromCache(obj interface{}) {
	var podGroup *schedulingv1a1.PodGroup
	switch t := obj.(type) {
	case *schedulingv1a1.PodGroup:
		podGroup = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		podGroup, ok = t.Obj.(*schedulingv1a1.PodGroup)
		if !ok {
			klog.InfoS("Failed to convert to *v1.PodGroup", "object", t.Obj)
			return
		}
	default:
		klog.InfoS("Failed to convert to *v1.PodGroup", "type", t)
		return
	}

	klog.V(3).InfoS("Detected a Delete event for pod group", "podGroup", klog.KObj(podGroup))

	if err := sched.commonCache.DeletePodGroup(podGroup); err != nil {
		klog.InfoS("Failed to remove pod group from scheduler cache", "err", err)
	}
}

func nodeAllocatableChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	return !reflect.DeepEqual(oldNode.Status.Allocatable, newNode.Status.Allocatable)
}

func nodeLabelsChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	return !reflect.DeepEqual(oldNode.GetLabels(), newNode.GetLabels())
}

func nodeTaintsChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	return !reflect.DeepEqual(newNode.Spec.Taints, oldNode.Spec.Taints)
}

func nodeSchedulableChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	return newNode.Spec.Unschedulable != oldNode.Spec.Unschedulable && !newNode.Spec.Unschedulable
}

func nodeConditionsChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	strip := func(conditions []v1.NodeCondition) map[v1.NodeConditionType]v1.ConditionStatus {
		conditionStatuses := make(map[v1.NodeConditionType]v1.ConditionStatus, len(conditions))
		for i := range conditions {
			conditionStatuses[conditions[i].Type] = conditions[i].Status
		}
		return conditionStatuses
	}
	return !reflect.DeepEqual(strip(oldNode.Status.Conditions), strip(newNode.Status.Conditions))
}

func nmNodeAllocatableChanged(newNMNode, oldNMNode *nodev1alpha1.NMNode) bool {
	return !reflect.DeepEqual(newNMNode.Status.ResourceAllocatable, oldNMNode.Status.ResourceAllocatable)
}

func nmNodeLabelsChanged(newNMNode, oldNMNode *nodev1alpha1.NMNode) bool {
	return !reflect.DeepEqual(oldNMNode.GetLabels(), newNMNode.GetLabels())
}

func nmNodeConditionsChanged(newNMNode, oldNMNode *nodev1alpha1.NMNode) bool {
	strip := func(conditions []*v1.NodeCondition) map[v1.NodeConditionType]v1.ConditionStatus {
		conditionStatuses := make(map[v1.NodeConditionType]v1.ConditionStatus, len(conditions))
		for i := range conditions {
			conditionStatuses[conditions[i].Type] = conditions[i].Status
		}
		return conditionStatuses
	}
	return !reflect.DeepEqual(strip(oldNMNode.Status.NodeCondition),
		strip(newNMNode.Status.NodeCondition))
}

// TODO: may be necessary in the future
/*func CNRConditionsChanged(newCNR *v1alpha1.CNR, oldCNR *v1alpha1.CNR) bool {
	strip := func(condition *v1.NodeCondition) map[v1.NodeConditionType]v1.ConditionStatus {
		conditionStatuses := map[v1.NodeConditionType]v1.ConditionStatus{}
		if condition != nil {
			conditionStatuses[condition.Type] = condition.Status
		}
		return conditionStatuses
	}
	return !reflect.DeepEqual(strip(oldCNR.Status.NMPerspectiveNodeCondition),
		strip(newCNR.Status.NMPerspectiveNodeCondition))
}*/

// nodeManagedByThisScheduler finds whether the scheduler can schedule pod to node
// if node partition type is logical, nodeManagedByThisScheduler always return true;
// if node partition type is physical and node is in node partition according to schedulercache, nodeManagedByThisScheduler return true, else return false
// This is called after cache operation, so it is ok to check whether this node is in scheduler's partition based on cache
// TODO: if this is called in other places, revisit this
func (sched *Scheduler) nodeManagedByThisScheduler(nodeName string) bool {
	return sched.commonCache.NodeInThisPartition(nodeName)
}

func nodeSchedulingPropertiesChange(newNode *v1.Node, oldNode *v1.Node) string {
	if nodeSchedulableChanged(newNode, oldNode) {
		return util.NodeSpecUnschedulableChange
	}
	if nodeAllocatableChanged(newNode, oldNode) {
		return util.NodeAllocatableChange
	}
	if nodeLabelsChanged(newNode, oldNode) {
		return util.NodeLabelChange
	}
	if nodeTaintsChanged(newNode, oldNode) {
		return util.NodeTaintChange
	}
	if nodeConditionsChanged(newNode, oldNode) {
		return util.NodeConditionChange
	}

	return ""
}

func nmNodeSchedulingPropertiesChange(newNMNode, oldNMNode *nodev1alpha1.NMNode) string {
	if nmNodeAllocatableChanged(newNMNode, oldNMNode) {
		return util.NodeAllocatableChange
	}
	if nmNodeLabelsChanged(newNMNode, oldNMNode) {
		return util.NodeLabelChange
	}
	if nmNodeConditionsChanged(newNMNode, oldNMNode) {
		return util.NodeConditionChange
	}

	return ""
}

func cnrAllocatableChanged(newCNR *katalystv1alpha1.CustomNodeResource, oldCNR *katalystv1alpha1.CustomNodeResource) bool {
	return !reflect.DeepEqual(oldCNR.Status.Resources.Allocatable, newCNR.Status.Resources.Allocatable) ||
		!reflect.DeepEqual(oldCNR.Spec.NodeResourceProperties, newCNR.Spec.NodeResourceProperties)
}

// TODO: find more properties which may change scheduling decisions
func cnrSchedulingPropertiesChanged(newCNR *katalystv1alpha1.CustomNodeResource, oldCNR *katalystv1alpha1.CustomNodeResource) string {
	if cnrAllocatableChanged(newCNR, oldCNR) {
		return util.NodeAllocatableChange
	}

	return ""
}

// skipPodUpdate checks whether the specified pod update should be ignored.
// This function will return true if
//   - The pod has already been assumed: pod is already in assumed cache or pod is set to assumed or preempted by annotation, AND
//   - The pod has only its ResourceVersion, Spec.NodeName, Annotations, ManagedFields, Finalizers and/or Conditions updated.
func (sched *Scheduler) skipPodUpdate(pod *v1.Pod) bool {
	// Non-assumed pods should never be skipped.
	isAssumed, err := sched.commonCache.IsAssumedPod(pod)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to check whether pod %s/%s is assumed: %v", pod.Namespace, pod.Name, err))
		return false
	}
	isAssumed = isAssumed || podutil.AssumedPod(pod) || podutil.BoundPod(pod)
	if isAssumed {
		klog.V(3).InfoS("Skipping assumed pod update", "pod", klog.KObj(pod))
		return true
	}
	return false
}

func (sched *Scheduler) onPdbAdd(obj interface{}) {
	pdb, ok := obj.(*policy.PodDisruptionBudget)
	if !ok {
		klog.InfoS("Failed to convert to *policy.PodDisruptionBudget", "object", obj)
		return
	}

	klog.V(3).InfoS("Detected an Add event for pdb, disruptions allowed", "pdb", klog.KObj(pdb), "disruptionsAllowed", pdb.Status.DisruptionsAllowed)

	if err := sched.commonCache.AddPDB(pdb); err != nil {
		klog.InfoS("Failed to add pdb", "pdb", klog.KObj(pdb), "err", err)
	}
}

func (sched *Scheduler) onPdbUpdate(oldObj, newObj interface{}) {
	oldPdb, ok := oldObj.(*policy.PodDisruptionBudget)
	if !ok {
		klog.InfoS("Failed to convert to *policy.PodDisruptionBudget", "oldObject", oldObj)
		return
	}
	newPdb, ok := newObj.(*policy.PodDisruptionBudget)
	if !ok {
		klog.InfoS("Failed to convert to *policy.PodDisruptionBudget", "newObject", newObj)
		return
	}
	klog.V(3).InfoS("Detected an Update event for pdb, disruptions allowed status changed", "pdb", klog.KObj(newPdb), "oldPdbDisruptionsAllowed", oldPdb.Status.DisruptionsAllowed, "newPdbDisruptionsAllowed", newPdb.Status.DisruptionsAllowed)

	if err := sched.commonCache.UpdatePDB(oldPdb, newPdb); err != nil {
		klog.InfoS("Failed to update pdb", "oldPdb", klog.KObj(oldPdb), "newPdb", klog.KObj(newPdb), "err", err)
	}
}

func (sched *Scheduler) onPdbDelete(obj interface{}) {
	pdb, ok := obj.(*policy.PodDisruptionBudget)
	if !ok {
		klog.InfoS("Failed to convert to *policy.PodDisruptionBudget", "object", obj)
		return
	}
	klog.V(3).InfoS("Detected a Delete event for pdb with disruptions allowed status", "pdb", klog.KObj(pdb), "disruptionsAllowed", pdb.Status.DisruptionsAllowed)

	if err := sched.commonCache.DeletePDB(pdb); err != nil {
		klog.InfoS("Failed to delete pdb", "pdb", klog.KObj(pdb))
	}
}

func (sched *Scheduler) onReplicaSetAdd(obj interface{}) {
	rs, ok := obj.(*appsv1.ReplicaSet)
	if !ok {
		klog.InfoS("Failed to convert to *appsv1.ReplicaSet", "object", obj)
		return
	}
	klog.V(3).InfoS("Detected an Add event for replicaset", "replicaSet", klog.KObj(rs))

	sched.commonCache.AddOwner(util.OwnerTypeReplicaSet, util.GetReplicaSetKey(rs), rs.GetLabels())
}

func (sched *Scheduler) onReplicaSetUpdate(oldObj, newObj interface{}) {
	oldRS, ok := oldObj.(*appsv1.ReplicaSet)
	if !ok {
		klog.InfoS("Failed to convert to *appsv1.ReplicaSet", "oldObject", oldObj)
		return
	}
	newRS, ok := newObj.(*appsv1.ReplicaSet)
	if !ok {
		klog.InfoS("Failed to convert to *appsv1.ReplicaSet", "newObject", newObj)
		return
	}
	klog.V(3).InfoS("Detected an Update event for replicaset", "replicaSet", klog.KObj(newRS))

	sched.commonCache.UpdateOwner(util.OwnerTypeReplicaSet, util.GetReplicaSetKey(newRS), oldRS.GetLabels(), newRS.GetLabels())
}

func (sched *Scheduler) onReplicaSetDelete(obj interface{}) {
	rs, ok := obj.(*appsv1.ReplicaSet)
	if !ok {
		klog.InfoS("Failed to convert to *appsv1.ReplicaSet", "object", obj)
		return
	}
	klog.V(3).InfoS("Detected a Delete event for replicaset", "replicaSet", klog.KObj(rs))

	sched.commonCache.DeleteOwner(util.OwnerTypeReplicaSet, util.GetReplicaSetKey(rs))
}

func (sched *Scheduler) onDaemonSetAdd(obj interface{}) {
	ds, ok := obj.(*appsv1.DaemonSet)
	if !ok {
		klog.InfoS("Failed to convert to *appsv1.DaemonSet", "object", obj)
		return
	}
	klog.V(3).InfoS("Detected an Add event for daemonset", "daemonSet", klog.KObj(ds))

	sched.commonCache.AddOwner(util.OwnerTypeDaemonSet, util.GetDaemonSetKey(ds), ds.GetLabels())
}

func (sched *Scheduler) onDaemonSetUpdate(oldObj, newObj interface{}) {
	oldDS, ok := oldObj.(*appsv1.DaemonSet)
	if !ok {
		klog.InfoS("Failed to convert to *appsv1.DaemonSet", "oldObject", oldObj)
		return
	}
	newDS, ok := newObj.(*appsv1.DaemonSet)
	if !ok {
		klog.InfoS("Failed to convert to *appsv1.DaemonSet", "newObject", newObj)
		return
	}
	klog.V(3).InfoS("Detected an Update event for daemonset", "daemonSet", klog.KObj(newDS))

	sched.commonCache.UpdateOwner(util.OwnerTypeDaemonSet, util.GetDaemonSetKey(newDS), oldDS.GetLabels(), newDS.GetLabels())
}

func (sched *Scheduler) onDaemonSetDelete(obj interface{}) {
	ds, ok := obj.(*appsv1.DaemonSet)
	if !ok {
		klog.InfoS("Failed to convert to *appsv1.DaemonSet", "object", obj)
		return
	}
	klog.V(3).InfoS("Detected a Delete event for daemonset", "daemonSet", klog.KObj(ds))

	sched.commonCache.DeleteOwner(util.OwnerTypeDaemonSet, util.GetDaemonSetKey(ds))
}

func (sched *Scheduler) addMovement(obj interface{}) {
	movement, ok := obj.(*schedulingv1a1.Movement)
	if !ok {
		klog.InfoS("Failed to convert to *scheduling.Movement", "object", obj)
		return
	}

	klog.V(5).InfoS("Detected an add event for movement", "movement", util.GetMovementName(movement))
	sched.commonCache.AddMovement(movement)
	sched.movementController.AddMovement(movement)
}

func (sched *Scheduler) updateMovement(oldObj, newObj interface{}) {
	oldMovement, ok := oldObj.(*schedulingv1a1.Movement)
	if !ok {
		klog.InfoS("Failed to convert to *scheduling.Movement", "oldObject", oldObj)
		return
	}
	newMovement, ok := newObj.(*schedulingv1a1.Movement)
	if !ok {
		klog.InfoS("Failed to convert to *scheduling.Movement", "newObject", newObj)
		return
	}
	klog.V(5).InfoS("Detected an update event for movement", "movement", util.GetMovementName(newMovement))
	sched.commonCache.UpdateMovement(oldMovement, newMovement)
	sched.movementController.AddMovement(newMovement)
}

func (sched *Scheduler) deleteMovement(obj interface{}) {
	movement, ok := obj.(*schedulingv1a1.Movement)
	if !ok {
		klog.InfoS("Failed to convert to *scheduling.Movement", "object", obj)
		return
	}
	klog.V(5).InfoS("Detected a delete event for movement", "movement", util.GetMovementName(movement))
	sched.commonCache.DeleteMovement(movement)
}

func (sched *Scheduler) addReservation(obj interface{}) {
	req, ok := obj.(*schedulingv1a1.Reservation)
	if !ok {
		klog.InfoS("cannot convert to *scheduling.Reservation", "object", obj)
		return
	}
	klog.V(4).InfoS("add event for Reservation", "reservation", klog.KObj(req))
	if err := sched.commonCache.AddReservation(req); err != nil {
		klog.ErrorS(err, "failed to add reservation to cache")
	}
}

func (sched *Scheduler) updateReservation(oldObj, newObj interface{}) {
	oldReq, ok := oldObj.(*schedulingv1a1.Reservation)
	if !ok {
		klog.InfoS("cannot convert to *scheduling.Reservation", "oldObj", oldObj)
		return
	}

	newReq, ok := newObj.(*schedulingv1a1.Reservation)
	if !ok {
		klog.InfoS("cannot convert to *scheduling.Reservation", "newObj", newObj)
		return
	}
	klog.V(4).InfoS("update event for Reservation", "reservation", klog.KObj(newReq))

	err := sched.commonCache.UpdateReservation(oldReq, newReq)
	if err != nil {
		klog.ErrorS(err, "scheduler cache UpdateReservation failed")
	}

	if podutil.ShouldOccupyResources(oldReq) && !podutil.ShouldOccupyResources(newReq) {
		if err != nil {
			oldPod := podutil.ConvertReservationToPod(oldReq)
			sched.ScheduleSwitch.Process(
				ParseSwitchTypeForPod(oldPod),
				func(dataSet ScheduleDataSet) {
					dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.ReservationDelete)
				},
			)
		}
	}
}

func (sched *Scheduler) deleteReservation(obj interface{}) {
	req, ok := obj.(*schedulingv1a1.Reservation)
	if !ok {
		klog.InfoS("cannot convert to *scheduling.Reservation", "object", obj)
		return
	}

	klog.V(4).InfoS("delete event for Reservation", "reservation", klog.KObj(req))
	if err := sched.commonCache.DeleteReservation(req); err != nil {
		klog.ErrorS(err, "scheduler cache delete Reservation failed")
	} else {
		pod := podutil.ConvertReservationToPod(req)
		sched.ScheduleSwitch.Process(
			ParseSwitchTypeForPod(pod),
			func(dataSet ScheduleDataSet) {
				dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.AssignedPodDelete)
			},
		)
	}
}

// addAllEventHandlers is a helper function used in tests and in Scheduler
// to add event handlers for various informers.
// addAllEventHandlers 为调度器注册所有必要的资源事件处理器（Event Handlers），
// 用于监听 Kubernetes 核心资源（Pod、Node、Service 等）和 Godel 特有 CRD（PodGroup、Reservation、Movement 等）
// 的增删改事件，并同步更新调度器内部缓存（commonCache）和队列（SchedulingQueue）。
// 这是实现调度器“实时感知集群状态变化”的关键机制。
func addAllEventHandlers(
	sched *Scheduler,
	informerFactory informers.SharedInformerFactory,
	crdInformerFactory crdinformers.SharedInformerFactory,
	katalystCrdInformerFactory katalystinformers.SharedInformerFactory,
) {
	// ==================== Pod 事件处理器 ====================
	// 监听 Pod 生命周期变化，触发调度队列和缓存的更新
	podInformer := informerFactory.Core().V1().Pods().Informer()
	podInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    sched.addPod,    // Pod 创建时调用（可能需要入队调度）
			UpdateFunc: sched.updatePod, // Pod 状态更新时调用（如节点绑定、删除）
			DeleteFunc: sched.deletePod, // Pod 删除时调用（清理缓存）
		},
	)

	// ==================== NMNode 事件处理器 ====================
	// NMNode 是 Godel 中表示 Node Manager 节点状态的 CRD（用于节点资源管理）
	// crdInformerFactory.Node().V1alpha1().NMNodes().Informer().AddEventHandler(
	// 	cache.ResourceEventHandlerFuncs{
	// 		AddFunc:    sched.addNMNodeToCache,    // 添加 NMNode 信息到缓存
	// 		UpdateFunc: sched.updateNMNodeInCache, // 更新 NMNode 信息
	// 		DeleteFunc: sched.deleteNMNodeFromCache, // 删除 NMNode 信息
	// 	},
	// )

	// ==================== CustomNodeResource (CNR) 事件处理器 ====================
	// CNR 是 Katalyst 项目中用于描述节点自定义资源（如 GPU、FPGA 等）的 CRD
	// katalystCrdInformerFactory.Node().V1alpha1().CustomNodeResources().Informer().AddEventHandler(
	// 	cache.ResourceEventHandlerFuncs{
	// 		AddFunc:    sched.addCNRToCache,    // 添加节点自定义资源信息
	// 		UpdateFunc: sched.updateCNRInCache, // 更新自定义资源信息
	// 		DeleteFunc: sched.deleteCNRFromCache, // 删除自定义资源信息
	// 	},
	// )

	// ==================== Node 事件处理器 ====================
	// 监听集群节点变化，更新节点资源池
	informerFactory.Core().V1().Nodes().Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    sched.addNodeToCache,    // 节点加入集群
			UpdateFunc: sched.updateNodeInCache, // 节点状态更新（如资源变化、污点更新）
			DeleteFunc: sched.deleteNodeFromCache, // 节点离开集群（如被删除、失联）
		},
	)

	// ==================== Scheduler CRD 事件处理器 ====================
	// 监听 Godel Scheduler 自身配置的变更（如调度策略更新）
	crdInformerFactory.Scheduling().V1alpha1().Schedulers().Informer().AddEventHandler(
		// 使用 FilteringResourceEventHandler 只处理与当前调度器实例相关的事件
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *schedulingv1a1.Scheduler:
					// 添加日志打印 Scheduler CRD 的名称和当前调度器的名称
					klog.InfoS("比较调度器名称", "schedulerCRDName", t.Name, "currentSchedulerName", sched.Name)
					return t.Name == sched.Name // 只处理名称匹配的 Scheduler CRD
				case cache.DeletedFinalStateUnknown:
					// 处理删除事件中缓存可能不一致的情况
					if scheduler, ok := t.Obj.(*schedulingv1a1.Scheduler); ok {
						// 添加日志打印 DeletedFinalStateUnknown 中的 Scheduler 名称和当前调度器的名称
						klog.InfoS("比较调度器名称", "schedulerCRDName", scheduler.Name, "currentSchedulerName", sched.Name)
						return scheduler.Name == sched.Name
					}
					utilruntime.HandleError(fmt.Errorf("unable to convert object %T to *v1alpha1.Scheduler in %T", obj, sched))
					return false
				default:
					utilruntime.HandleError(fmt.Errorf("unable to handle object in %T: %T", sched, obj))
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				UpdateFunc: sched.onSchedulerUpdate, // Scheduler CRD 更新时触发配置重载
			},
		},
	)

	// ==================== CSI Node 事件处理器（条件启用）====================
	// 监听 CSI 驱动节点信息变化，用于存储调度
	// if utilfeature.DefaultFeatureGate.Enabled(features.CSINodeInfo) {
	// 	informerFactory.Storage().V1().CSINodes().Informer().AddEventHandler(
	// 		cache.ResourceEventHandlerFuncs{
	// 			AddFunc:    sched.onCSINodeAdd,    // CSI Node 信息新增
	// 			UpdateFunc: sched.onCSINodeUpdate, // CSI Node 信息更新
	// 		},
	// 	)
	// }

	// ==================== PersistentVolume (PV) 事件处理器 ====================
	// 监听 PV 变化，影响 MaxPDVolumeCountPredicate（限制节点上 PV 数量）的计算
	// informerFactory.Core().V1().PersistentVolumes().Informer().AddEventHandler(
	// 	cache.ResourceEventHandlerFuncs{
	// 		AddFunc:    sched.onPvAdd,    // PV 创建（可能影响节点 PV 限额）
	// 		UpdateFunc: sched.onPvUpdate, // PV 状态更新（如绑定状态）
	// 	},
	// )

	// ==================== PersistentVolumeClaim (PVC) 事件处理器 ====================
	// 监听 PVC 变化，PVC 绑定 PV 时会影响节点 PV 计数
	// informerFactory.Core().V1().PersistentVolumeClaims().Informer().AddEventHandler(
	// 	cache.ResourceEventHandlerFuncs{
	// 		AddFunc:    sched.onPvcAdd,    // PVC 创建
	// 		UpdateFunc: sched.onPvcUpdate, // PVC 状态更新（如绑定 PV）
	// 	},
	// )

	// ==================== Service 事件处理器 ====================
	// 监听 Service 变化，影响 ServiceAffinity 调度谓词（确保 Pod 与 Service 所需节点的亲和性）
	// informerFactory.Core().V1().Services().Informer().AddEventHandler(
	// 	cache.ResourceEventHandlerFuncs{
	// 		AddFunc:    sched.onServiceAdd,    // Service 创建（可能影响 Pod 分布）
	// 		UpdateFunc: sched.onServiceUpdate, // Service 更新（如 Selector 变化）
	// 		DeleteFunc: sched.onServiceDelete, // Service 删除
	// 	},
	// )

	// ==================== 抢占相关资源事件处理器（条件启用）====================
	// 仅在启用了抢占功能时注册以下事件处理器
	// if sched.mayHasPreemption {
	// 	// PodDisruptionBudget (PDB) 事件：影响抢占决策（不能驱逐 PDB 保护的 Pod）
	// 	informerFactory.Policy().V1().PodDisruptionBudgets().Informer().AddEventHandler(
	// 		cache.ResourceEventHandlerFuncs{
	// 			AddFunc:    sched.onPdbAdd,
	// 			UpdateFunc: sched.onPdbUpdate,
	// 			DeleteFunc: sched.onPdbDelete,
	// 		},
	// 	)

	// 	// ReplicaSet 事件：影响抢占目标选择（如副本数、控制器状态）
	// 	informerFactory.Apps().V1().ReplicaSets().Informer().AddEventHandler(
	// 		cache.ResourceEventHandlerFuncs{
	// 			AddFunc:    sched.onReplicaSetAdd,
	// 			UpdateFunc: sched.onReplicaSetUpdate,
	// 			DeleteFunc: sched.onReplicaSetDelete,
	// 		},
	// 	)

	// 	// DaemonSet 事件：DaemonSet Pod 通常不可抢占
	// 	informerFactory.Apps().V1().DaemonSets().Informer().AddEventHandler(
	// 		cache.ResourceEventHandlerFuncs{
	// 			AddFunc:    sched.onDaemonSetAdd,
	// 			UpdateFunc: sched.onDaemonSetUpdate,
	// 			DeleteFunc: sched.onDaemonSetDelete,
	// 		},
	// 	)
	// }

	// ==================== StorageClass 事件处理器 ====================
	// 监听 StorageClass 变化（目前只处理新增，可能用于动态 PV 调度）
	// informerFactory.Storage().V1().StorageClasses().Informer().AddEventHandler(
	// 	cache.ResourceEventHandlerFuncs{
	// 		AddFunc: sched.onStorageClassAdd,
	// 	},
	// )

	// ==================== PodGroup CRD 事件处理器 ====================
	// 监听 PodGroup（Gang Scheduling）变化，管理 Pod 组的调度状态
	// crdInformerFactory.Scheduling().V1alpha1().PodGroups().Informer().AddEventHandler(
	// 	cache.ResourceEventHandlerFuncs{
	// 		AddFunc:    sched.addPodGroupToCache,    // PodGroup 创建
	// 		UpdateFunc: sched.updatePodGroupToCache, // PodGroup 状态更新（如最小成员数变化）
	// 		DeleteFunc: sched.deletePodGroupFromCache, // PodGroup 删除
	// 	},
	// )

	// ==================== Movement CRD 事件处理器（条件启用）====================
	// Movement 是 Godel 中表示“Pod 迁移/重调度请求”的 CRD
	// if utilfeature.DefaultFeatureGate.Enabled(godelfeatures.SupportRescheduling) {
	// 	crdInformerFactory.Scheduling().V1alpha1().Movements().Informer().AddEventHandler(
	// 		cache.ResourceEventHandlerFuncs{
	// 			AddFunc:    sched.addMovement,    // 新增迁移请求
	// 			UpdateFunc: sched.updateMovement, // 迁移请求状态更新
	// 			DeleteFunc: sched.deleteMovement, // 迁移请求删除
	// 		})
	// }

	// ==================== Reservation CRD 事件处理器（条件启用）====================
	// Reservation 是 Godel 中表示“资源预留”的 CRD，用于 Gang Scheduling 和资源预占
	// if utilfeature.DefaultFeatureGate.Enabled(godelfeatures.ResourceReservation) {
	// 	crdInformerFactory.Scheduling().V1alpha1().Reservations().Informer().AddEventHandler(
	// 		cache.ResourceEventHandlerFuncs{
	// 			AddFunc:    sched.addReservation,    // 新增资源预留
	// 			UpdateFunc: sched.updateReservation, // 预留状态更新（如绑定节点）
	// 			DeleteFunc: sched.deleteReservation, // 预留取消
	// 		},
	// 	)
	// }
}