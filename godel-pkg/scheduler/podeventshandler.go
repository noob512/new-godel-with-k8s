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
	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"

	"github.com/kubewharf/godel-scheduler/pkg/features"
	framework "github.com/kubewharf/godel-scheduler/pkg/framework/api"
	"github.com/kubewharf/godel-scheduler/pkg/util"
	podutil "github.com/kubewharf/godel-scheduler/pkg/util/pod"
	"github.com/kubewharf/godel-scheduler/pkg/util/tracing"
)

func (sched *Scheduler) assumedOrBoundPod(pod *v1.Pod) bool {
	return podutil.BoundPod(pod) || podutil.AssumedPodOfGodel(pod, *sched.SchedulerName)
}

// dispatchedPodOfThisScheduler 是一个判断函数，用于检查一个 Pod 是否由当前的 Godel 调度器实例进行调度。
//
// 参数:
// - pod: 需要检查的 v1.Pod 对象指针。
//
// 返回值:
// - bool: 如果 Pod 被标记为由 Godel 调度框架调度，并且明确指定由当前调度器实例 (sched.Name) 调度，则返回 true；
//         否则返回 false。
func (sched *Scheduler) dispatchedPodOfThisScheduler(pod *v1.Pod) bool {
	// 此函数执行双重检查：
	// 1. podutil.DispatchedPodOfGodel(pod, *sched.SchedulerName):
	//    检查 Pod 的标签或注解是否表明它应该由 Godel 调度框架处理。
	//    它会验证 Pod 上是否存在特定的调度器名称标签 (SchedulerName)，并与传入的 *sched.SchedulerName 进行比较。
	//    这确保了 Pod 是 Godel 调度框架的管辖范围。
	//
	// 2. podutil.DispatchedPodOfThisScheduler(pod, sched.Name):
	//    检查 Pod 的注解是否表明它已经被（或应该被）当前这个具体的调度器实例 (sched.Name) 选中。
	//    它会查看 Pod 的注解 (Annotations) 中是否包含 "godel-scheduler.k8s.io/scheduler" 键，
	//    并验证其值是否与当前调度器实例的名称 (sched.Name) 匹配。
	//
	// 只有当两个条件都满足时，才认为该 Pod 是由当前调度器实例负责的。
	return podutil.DispatchedPodOfGodel(pod, *sched.SchedulerName) && podutil.DispatchedPodOfThisScheduler(pod, sched.Name)
}

func (sched *Scheduler) triggerQueueOnAssumedOrBoundPodAdd(pod *v1.Pod) error {
	sched.ScheduleSwitch.Process(
		ParseSwitchTypeForPod(pod),
		func(dataSet ScheduleDataSet) {
			dataSet.SchedulingQueue().AssignedPodAdded(pod)
		},
	)
	return nil
}

func (sched *Scheduler) triggerQueueOnAssumedOrBoundPodUpdate(oldPod, newPod *v1.Pod) error {
	sched.ScheduleSwitch.Process(
		ParseSwitchTypeForPod(newPod),
		func(dataSet ScheduleDataSet) {
			dataSet.SchedulingQueue().AssignedPodUpdated(newPod)
		},
	)
	return nil
}

func (sched *Scheduler) triggerQueueOnAssumedOrBoundPodDelete(pod *v1.Pod) error {
	sched.ScheduleSwitch.Process(
		ParseSwitchTypeForPod(pod),
		func(dataSet ScheduleDataSet) {
			dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.AssignedPodDelete)
		},
	)
	return nil
}

func (sched *Scheduler) addDispatchedPodToQueue(pod *v1.Pod) error {
	podProperty := framework.ExtractPodProperty(pod)
	parentSpanContext := tracing.GetSpanContextFromPod(pod)
	traceContext, _ := tracing.StartSpanForPodWithParentSpan(
		podutil.GeneratePodKey(pod),
		"scheduler::addPodToSchedulingQueue",
		parentSpanContext,
		tracing.WithComponent("scheduler"),
		tracing.WithScheduler(sched.Name),
		podProperty.ConvertToTracingTags(),
	)
	defer func() {
		go traceContext.Finish()
	}()

	if utilfeature.DefaultFeatureGate.Enabled(features.SchedulerSubClusterConcurrentScheduling) {
		subCluster := pod.Spec.NodeSelector[framework.GetGlobalSubClusterKey()]
		idx, exist := framework.GetOrCreateClusterIndex(subCluster)
		if !exist {
			// ATTENTION: It is possible to be called before `sched.Run`, so we don't run workflow immediately.
			sched.createSubClusterWorkflow(idx, subCluster)
		}
	}

	sched.ScheduleSwitch.Process(
		ParseSwitchTypeForPod(pod),
		func(dataSet ScheduleDataSet) {
			if err := dataSet.SchedulingQueue().Add(pod); err != nil {
				klog.InfoS("Failed to add pod to scheduler queue", "err", err)
			}
		},
	)
	return nil
}

func (sched *Scheduler) updateDispatchedPodInQueue(oldPod, newPod *v1.Pod) error {
	if sched.skipPodUpdate(newPod) {
		return nil
	}
	podProperty := framework.ExtractPodProperty(newPod)
	parentSpanContext := tracing.GetSpanContextFromPod(newPod)
	traceContext, _ := tracing.StartSpanForPodWithParentSpan(
		podutil.GetPodKey(newPod),
		"scheduler::updatePodInSchedulingQueue",
		parentSpanContext,
		tracing.WithComponent("scheduler"),
		tracing.WithScheduler(sched.Name),
		podProperty.ConvertToTracingTags(),
	)
	defer func() {
		go traceContext.Finish()
	}()

	if utilfeature.DefaultFeatureGate.Enabled(features.SchedulerSubClusterConcurrentScheduling) {
		// If the sub-cluster changes, it should be removed from the old sub-cluster.
		// ATTENTION: This should not happen, but be careful in case
		{
			oldSt, newSt := ParseSwitchTypeForPod(oldPod), ParseSwitchTypeForPod(newPod)
			if oldSt != newSt {
				sched.ScheduleSwitch.Process(
					oldSt,
					func(dataSet ScheduleDataSet) {
						if err := dataSet.SchedulingQueue().Delete(oldPod); err != nil {
							klog.InfoS("Failed to update new pod: deleted oldPod from another workflow", "newPod", klog.KObj(newPod), "oldPod", klog.KObj(oldPod), "err", err)
						}
					},
				)
			}
		}

		subCluster := newPod.Spec.NodeSelector[framework.GetGlobalSubClusterKey()]
		idx, exist := framework.GetOrCreateClusterIndex(subCluster)
		if !exist {
			// ATTENTION: It is possible to be called before `sched.Run`, so we don't run workflow immediately.
			sched.createSubClusterWorkflow(idx, subCluster)
		}
	}

	sched.ScheduleSwitch.Process(
		ParseSwitchTypeForPod(newPod),
		func(dataSet ScheduleDataSet) {
			// TODO: handle special case: newPod belongs to unit and oldPod belongs to Pod. vice versa.
			if err := dataSet.SchedulingQueue().Update(oldPod, newPod); err != nil {
				klog.InfoS("Failed to update new pod", "newPod", klog.KObj(newPod), "err", err)
			}
		},
	)
	return nil
}

func (sched *Scheduler) deleteDispatchedPodFromQueue(pod *v1.Pod) error {
	podProperty := framework.ExtractPodProperty(pod)
	parentSpanContext := tracing.GetSpanContextFromPod(pod)
	traceContext, _ := tracing.StartSpanForPodWithParentSpan(
		podutil.GetPodKey(pod),
		"scheduler::deletePodFromSchedulingQueue",
		parentSpanContext,
		tracing.WithComponent("scheduler"),
		tracing.WithScheduler(sched.Name),
		podProperty.ConvertToTracingTags(),
	)
	defer func() {
		go traceContext.Finish()
	}()

	sched.ScheduleSwitch.Process(
		ParseSwitchTypeForPod(pod),
		func(dataSet ScheduleDataSet) {
			// TODO: handle special case: newPod belongs to unit and oldPod belongs to Pod. vice versa.
			if err := dataSet.SchedulingQueue().Delete(pod); err != nil {
				klog.InfoS("Failed to dequeue", "pod", klog.KObj(pod), "err", err)
			}
		},
	)
	return nil
}

// addPod 是 Pod 添加事件的处理函数，负责将新创建的 Pod 添加到调度器的缓存和队列中。
// 该函数首先将传入的对象转换为 Pod 类型，然后将其添加到共享缓存中。
// 根据 Pod 的当前状态（假设/绑定 或 调度中），决定将其加入相应的处理队列。
// 这是调度器响应 Pod 创建事件的核心逻辑之一。
func (sched *Scheduler) addPod(obj interface{}) {
	// 将传入的对象（可能是 *v1.Pod 或 cache.DeletedFinalStateUnknown）转换为 *v1.Pod 类型
	pod, err := podutil.ConvertToPod(obj)
	if err != nil {
		// 转换失败时记录错误日志（注意：此处 pod 可能为 nil，klog.KObj 会处理 nil 情况）
		klog.InfoS("Failed to add pod", "err", err, "pod", klog.KObj(pod))
		return
	}

	// 记录 Pod 添加事件的详细信息，包括 Pod 状态和键（namespace/name）
	klog.InfoS("检测到一个pod事件", "state", podutil.GetPodState(pod.Annotations), "pod", podutil.GetPodKey(pod))

	// 将 Pod 添加到调度器的共享缓存中（该缓存被调度器核心逻辑和队列共享）
	if err := sched.commonCache.AddPod(pod); err != nil {
		// 添加到缓存失败时记录错误日志
		klog.InfoS("Failed to add pod to scheduler cache", "err", err)
	}
	
	// 根据 Pod 的状态决定后续处理逻辑：
	// 1. 如果 Pod 是假设状态或已绑定状态（即已调度）
	if sched.assumedOrBoundPod(pod) {
		// 触发队列相关的处理逻辑（可能更新队列中的相关信息）
		sched.triggerQueueOnAssumedOrBoundPodAdd(pod)
	// 2. 如果 Pod 是由当前调度器调度的（dispatched 状态）
	} else if sched.dispatchedPodOfThisScheduler(pod) {
		// 将 Pod 添加到调度队列中，准备进行调度
		sched.addDispatchedPodToQueue(pod)
	}
	// 如果 Pod 不属于以上两种状态，则不进行额外的队列操作
}

// updatePod 是 Pod 更新事件的处理函数，负责处理 Pod 对象的变更事件。
// 该函数接收旧的 Pod 对象和新的 Pod 对象，执行以下操作：
// 1. 更新调度器缓存中的 Pod 信息
// 2. 根据 Pod 的调度状态更新相应的调度队列
// 这是调度器响应 Pod 状态变更（如资源更新、标签变化等）的核心逻辑之一。
func (sched *Scheduler) updatePod(oldObj, newObj interface{}) {
	// 将旧的对象转换为 Pod 类型
	oldPod, err := podutil.ConvertToPod(oldObj)
	if err != nil {
		// 转换失败时记录错误日志，返回不继续处理
		klog.InfoS("Failed to update pod with oldObj", "err", err)
		return
	}
	// 将新的对象转换为 Pod 类型
	newPod, err := podutil.ConvertToPod(newObj)
	if err != nil {
		// 转换失败时记录错误日志，返回不继续处理
		klog.InfoS("Failed to update pod with newObj", "err", err)
		return
	}

	// 记录 Pod 更新事件的详细信息，包括 Pod 的完整键（namespace/name）
	klog.InfoS("检测到pod更新事件", "pod", klog.KObj(newPod))

	// 执行缓存更新操作：根据新旧 Pod 的差异更新调度器的共享缓存
	if err := sched.updatePodInCache(oldPod, newPod); err != nil {
		// 缓存更新失败时记录错误日志
		klog.InfoS("Failed to update pod in scheduler cache", "err", err)
	}

	// 执行队列更新操作：根据 Pod 的调度状态（Dispatched）决定如何处理队列中的 Pod
	{
		// 使用 FilteringUpdate 工具函数来智能处理 Pod 更新：
		// - 如果 Pod 由当前调度器调度（dispatched），则根据变更情况执行相应的队列操作
		// - 可能的操作包括：添加到队列、更新队列中的 Pod 信息、或从队列中删除 Pod
		// - 该函数会根据 oldPod 和 newPod 的状态差异，自动选择合适的处理方式
		podutil.FilteringUpdate(sched.dispatchedPodOfThisScheduler, sched.addDispatchedPodToQueue, sched.updateDispatchedPodInQueue,
			sched.deleteDispatchedPodFromQueue, oldPod, newPod)
	}
}

// updatePodInCache 更新调度器缓存中的 Pod 信息，并根据 Pod 状态变化触发相关的队列更新操作。
// 该函数执行两步操作：
// 1. 更新调度器共享缓存中的 Pod 对象信息
// 2. 根据 Pod 状态变化更新调度队列，因为 Pod 的变化可能影响其他未调度 Pod 的调度结果
// 这是确保调度器缓存与实际集群状态保持一致的关键函数。
func (sched *Scheduler) updatePodInCache(oldPod *v1.Pod, newPod *v1.Pod) error {
	// 第一步：更新调度器的共享缓存
	// 将旧的 Pod 信息替换为新的 Pod 信息，更新节点资源统计和 Pod 列表
	if err := sched.commonCache.UpdatePod(oldPod, newPod); err != nil {
		// 缓存更新失败时直接返回错误
		return err
	}

	// 第二步：作为级联操作，需要更新调度队列
	// 因为 Pod 状态的变化可能会使一些不可调度的 Pod 变得可调度
	// 例如：由于 Pod 亲和性相关的原因，一个在不可调度队列中的 Pod 可能变得可调度，
	// 那么这个不可调度的 Pod 就可以从不可调度队列移动到就绪队列或退避队列中进行重新调度尝试
	// FilteringUpdate 根据 Pod 是否为假设或已绑定状态，执行相应的队列触发操作：
	// - 如果 Pod 是假设/绑定状态，则可能触发队列的添加、更新或删除逻辑
	podutil.FilteringUpdate(sched.assumedOrBoundPod, sched.triggerQueueOnAssumedOrBoundPodAdd, sched.triggerQueueOnAssumedOrBoundPodUpdate,
		sched.triggerQueueOnAssumedOrBoundPodDelete, oldPod, newPod)

	return nil
}

func (sched *Scheduler) deletePod(obj interface{}) {
	pod, err := podutil.ConvertToPod(obj)
	if err != nil {
		klog.InfoS("Failed to delete pod", "err", err)
		return
	}

	klog.V(3).InfoS("Detected a Delete event for pod", "state", podutil.GetPodState(pod.Annotations), "pod", podutil.GetPodKey(pod))

	if err = sched.commonCache.DeletePod(pod); err != nil {
		klog.InfoS("Scheduler cache RemovePod failed", "err", err)
	}
	if sched.assumedOrBoundPod(pod) {
		sched.triggerQueueOnAssumedOrBoundPodDelete(pod)
	} else {
		// just in case
		if err == nil {
			sched.ScheduleSwitch.Process(
				ParseSwitchTypeForPod(pod),
				func(dataSet ScheduleDataSet) {
					dataSet.SchedulingQueue().MoveAllToActiveOrBackoffQueue(util.AssignedPodDelete)
				},
			)
		}
	}

	if sched.dispatchedPodOfThisScheduler(pod) {
		sched.deleteDispatchedPodFromQueue(pod)
	}

	// TODO: we may need to take victims throttle into account here
}
