/*
Copyright 2014 The Kubernetes Authors.

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
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/internal/queue"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/util"
	utiltrace "k8s.io/utils/trace"
)

const (
	// SchedulerError is the reason recorded for events when an error occurs during scheduling a pod.
	SchedulerError = "SchedulerError"
	// Percentage of plugin metrics to be sampled.
	pluginMetricsSamplePercent = 10
	// minFeasibleNodesToFind is the minimum number of nodes that would be scored
	// in each scheduling cycle. This is a semi-arbitrary value to ensure that a
	// certain minimum of nodes are checked for feasibility. This in turn helps
	// ensure a minimum level of spreading.
	minFeasibleNodesToFind = 100
	// minFeasibleNodesPercentageToFind is the minimum percentage of nodes that
	// would be scored in each scheduling cycle. This is a semi-arbitrary value
	// to ensure that a certain minimum of nodes are checked for feasibility.
	// This in turn helps ensure a minimum level of spreading.
	minFeasibleNodesPercentageToFind = 5
)

var clearNominatedNode = &framework.NominatingInfo{NominatingMode: framework.ModeOverride, NominatedNodeName: ""}

// scheduleOne 对单个 Pod 执行完整的调度工作流程。
// 这个函数在调度算法的主机适配（host fitting）阶段是串行化的。
func (sched *Scheduler) scheduleOne(ctx context.Context) {
	// 从调度队列中获取下一个待调度的 Pod 信息。
	podInfo := sched.NextPod()
	// 如果获取到的 Pod 信息为 nil，或者 Pod 本身为 nil（例如调度队列已关闭），
	// 则直接返回，结束本次调度尝试。
	if podInfo == nil || podInfo.Pod == nil {
		return
	}
	pod := podInfo.Pod

	// --- Godel 调度器兼容性处理 ---
	// 检查 Pod 是否指定了 "godel-scheduler" 作为其调度器。
	// 如果是，则将其修改为 "default-scheduler"。
	// 这通常是为了兼容或测试目的，将 Godel 调度器的 Pod 重定向到标准调度器。
	if pod.Spec.SchedulerName == "godel-scheduler" {
		pod.Spec.SchedulerName = "default-scheduler"
	}

	// 根据 Pod 的调度器名称获取对应的调度框架（Framework）。
	fwk, err := sched.frameworkForPod(pod)
	// 如果获取框架失败（例如没有匹配的配置文件），记录错误并返回。
	// 这种情况理论上不应该发生，因为我们只接受调度器名称匹配的 Pod。
	if err != nil {
		klog.ErrorS(err, "Error occurred")
		return
	}

	// 检查是否应该跳过此 Pod 的调度。
	// 例如，如果 Pod 已经被标记为不可调度或有其他原因需要跳过。
	if sched.skipPodSchedule(fwk, pod) {
		return
	}

	// 记录日志，表示开始尝试调度此 Pod。
	klog.V(3).InfoS("Attempting to schedule pod", "pod", klog.KObj(pod))

	// --- 同步尝试为 Pod 寻找合适的节点 ---
	// 记录调度开始时间，用于后续性能指标统计。
	start := time.Now()
	// 创建一个新的调度周期状态对象。
	// 这个对象在整个调度周期中被插件用来共享和传递信息。
	state := framework.NewCycleState()
	// 根据一个随机数决定是否为本次调度周期记录插件指标。
	// 这有助于控制指标收集的开销。
	state.SetRecordPluginMetrics(rand.Intn(100) < pluginMetricsSamplePercent)
	// 初始化一个空的 PodsToActivate 结构体。
	// 这个结构体可能会被某些插件填充，用于在调度失败后激活其他 Pod。
	// 如果没有插件使用它，它将保持为空。
	podsToActivate := framework.NewPodsToActivate()
	// 将 PodsToActivate 对象写入到调度周期状态中，以便插件可以访问。
	state.Write(framework.PodsToActivateKey, podsToActivate)

	// 创建一个可取消的调度周期上下文，用于控制调度过程。
	// 当函数返回时，确保取消这个上下文，以释放相关资源。
	schedulingCycleCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 调用核心调度逻辑，尝试为 Pod 找到一个合适的节点。
	scheduleResult, err := sched.SchedulePod(schedulingCycleCtx, fwk, state, pod)
	// 如果调度失败（err 不为 nil）。
	if err != nil {
		// SchedulePod() 可能因为 Pod 在任何主机上都不适合而失败，所以我们尝试抢占（preemption），
		// 期望下一次调度时，由于抢占的发生，Pod 将能够适合。
		// 当然，也有可能是另一个不同的 Pod 占用了被抢占的资源，但这是无害的。

		// 初始化 NominatingInfo，用于指定在抢占后应该提名哪个节点。
		var nominatingInfo *framework.NominatingInfo
		// 初始化失败原因。
		reason := v1.PodReasonUnschedulable

		// 检查错误类型是否为 FitError（表示 Pod 不适合任何节点）。
		if fitError, ok := err.(*framework.FitError); ok {
			// 检查是否有 PostFilter 插件注册。
			if !fwk.HasPostFilterPlugins() {
				// 如果没有 PostFilter 插件，则无法执行抢占。
				klog.V(3).InfoS("No PostFilter plugins are registered, so no preemption will be performed")
			} else {
				// 运行 PostFilter 插件，尝试使 Pod 在未来的调度周期中变得可调度。
				// 通常，这涉及执行抢占逻辑。
				result, status := fwk.RunPostFilterPlugins(ctx, state, pod, fitError.Diagnosis.NodeToStatusMap)
				// 检查 PostFilter 插件的运行状态。
				if status.Code() == framework.Error {
					// 如果 PostFilter 插件内部发生错误，记录详细错误信息。
					klog.ErrorS(nil, "Status after running PostFilter plugins for pod", "pod", klog.KObj(pod), "status", status)
				} else {
					// 如果 PostFilter 插件运行成功或有其他状态，记录状态信息。
					fitError.Diagnosis.PostFilterMsg = status.Message()
					klog.V(5).InfoS("Status after running PostFilter plugins for pod", "pod", klog.KObj(pod), "status", status)
				}
				// 如果 PostFilter 插件返回了结果，并且结果中包含 NominatingInfo，
				// 则更新本次调度失败的 NominatingInfo。
				if result != nil {
					nominatingInfo = result.NominatingInfo
				}
			}
			// Pod 在任何地方都不适合，因此计为失败。
			// 如果抢占成功，下次尝试调度此 Pod 时，它应该会因为抢占而被计为成功。（希望如此）
			metrics.PodUnschedulable(fwk.ProfileName(), metrics.SinceInSeconds(start))
		} else if err == ErrNoNodesAvailable {
			// 如果错误是 "没有可用节点"，则清除提名节点。
			nominatingInfo = clearNominatedNode
			// "没有可用节点" 被计为不可调度，而不是错误。
			metrics.PodUnschedulable(fwk.ProfileName(), metrics.SinceInSeconds(start))
		} else {
			// 如果是其他类型的错误，则清除提名节点。
			nominatingInfo = clearNominatedNode
			// 记录调度节点选择过程中的错误。
			klog.ErrorS(err, "Error selecting node for pod", "pod", klog.KObj(pod))
			// 记录调度错误指标。
			metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
			// 更新失败原因。
			reason = SchedulerError
		}
		// 调用处理调度失败的函数，执行清理操作，并将 Pod 放回队列等待重试。
		sched.handleSchedulingFailure(fwk, podInfo, err, reason, nominatingInfo)
		return
	}

	// 如果调度成功，记录调度算法的执行延迟。
	metrics.SchedulingAlgorithmLatency.Observe(metrics.SinceInSeconds(start))

	// --- 假设 Pod 已被调度到选定的节点 ---
	// 告诉缓存（Cache）假设一个 Pod 现在正在给定的节点上运行，即使它还没有被实际绑定。
	// 这允许我们在不等待绑定发生的情况下继续调度其他 Pod。
	assumedPodInfo := podInfo.DeepCopy() // 深拷贝 PodInfo，避免修改原始对象。
	assumedPod := assumedPodInfo.Pod     // 获取深拷贝后的 Pod 对象。

	// 调用 assume 函数，修改 `assumedPod`，将其 `NodeName` 设置为 `scheduleResult.SuggestedHost`。
	err = sched.assume(assumedPod, scheduleResult.SuggestedHost)
	// 如果假设失败。
	if err != nil {
		// Assume失败，记录冲突
		sched.nodeHistoryManager.RecordConflict(scheduleResult.SuggestedHost)
		// 记录调度错误指标。
		metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
		// 这很可能是重试逻辑中的 BUG。
		// 我们在这里报告一个错误，以便 Pod 调度可以被重试。
		// 这依赖于这样一个事实：Error 函数会检查 Pod 是否已被绑定到节点，
		// 如果是，则不会将其添加回未调度的 Pod 队列（否则会导致无限循环）。
		sched.handleSchedulingFailure(fwk, assumedPodInfo, err, SchedulerError, clearNominatedNode)
		return
	}
	nextNum := 0

	// --- 运行 Reserve 插件（首选节点）---
	// 总是为首选节点预留资源
	// 运行 Reserve 插件的 Reserve 方法。
	// 这些插件通常用于为 Pod 预留节点上的资源（如 Volume、网络资源等）。
	if sts := fwk.RunReservePluginsReserve(schedulingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost); !sts.IsSuccess() {
		// 首选节点 Reserve 失败，记录冲突信息到历史管理器
		sched.nodeHistoryManager.RecordConflict(scheduleResult.SuggestedHost)

		// 如果次优节点已经被预留了，需要清理其预留状态
		// 这是为了避免资源泄漏，确保次优节点的资源可以被其他 Pod 使用
		if scheduleResult.SecondaryReservedNode != "" {
			// 创建次优节点的 Pod 副本用于取消预留操作
			secondaryPod := assumedPod.DeepCopy()
			secondaryPod.Spec.NodeName = scheduleResult.SecondaryReservedNode
			// 运行 Reserve 插件的 Unreserve 方法，释放次优节点上预留的资源
			fwk.RunReservePluginsUnreserve(schedulingCycleCtx, state, secondaryPod, scheduleResult.SecondaryReservedNode)
			// 记录次优节点清理日志
			klog.V(4).InfoS("Cleaned up secondary node reservation due to primary node reserve failure",
				"pod", klog.KObj(assumedPod),
				"secondaryNode", scheduleResult.SecondaryReservedNode)
		}

		// 如果调度结果中包含候选节点（说明使用了候选节点调度策略）
		if len(scheduleResult.CandidateNodes) > 1 {
			// 记录首选节点 Reserve 失败，准备尝试候选节点的日志
			klog.V(4).InfoS("Reserve failed for primary node, trying candidate nodes",
				"pod", klog.KObj(assumedPod),
				"primaryNode", scheduleResult.SuggestedHost)

			// 取消任何可能存在的 Permit Wait 状态
			// 这是为了确保在尝试新节点前，没有挂起的等待状态
			sched.cancelPermitWait(fwk, assumedPod)

			// 清理首选节点上已经预留的资源状态
			fwk.RunReservePluginsUnreserve(schedulingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
			
			// 从调度器缓存中移除 Pod 的假设状态
			if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
				klog.ErrorS(forgetErr, "Scheduler cache ForgetPod failed")
			}

			// 尝试候选节点（从第二个开始，因为第一个是首选节点）
			// 参数说明：
			// - schedulingCycleCtx: 调度周期上下文
			// - fwk: 调度框架
			// - state: 调度周期状态
			// - assumedPod: 假设的 Pod 对象
			// - assumedPodInfo: 假设的 Pod 信息
			// - scheduleResult.CandidateNodes[1:]: 从第二个候选节点开始的所有候选节点
			// - start: 调度开始时间
			success, candidateNode, curNextNum, permitStatus := sched.tryCandidateNodesForReserve(
				schedulingCycleCtx, fwk, state, assumedPod, assumedPodInfo,
				scheduleResult.CandidateNodes[1:], start)

			// 更新下一个节点的计数器
			nextNum = curNextNum

			// 如果在候选节点上 Reserve 成功
			if success {
				// 检查 Permit 阶段的状态
				if permitStatus != nil && !permitStatus.IsSuccess() {
					// Permit 阶段失败，需要清理当前节点的预留资源
					fwk.RunReservePluginsUnreserve(schedulingCycleCtx, state, assumedPod, candidateNode)
					
					// 从调度器缓存中移除 Pod 的假设状态
					if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
						klog.ErrorS(forgetErr, "Scheduler cache ForgetPod failed")
					}
					
					// 记录该候选节点的冲突信息
					sched.nodeHistoryManager.RecordConflict(candidateNode)
					
					// 如果还有其他候选节点，可以继续尝试；否则处理调度失败
					metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
					sched.handleSchedulingFailure(fwk, assumedPodInfo, permitStatus.AsError(), SchedulerError, clearNominatedNode)
					return
				}

				// Reserve 和 Permit 都成功，更新调度结果并继续后续流程
				scheduleResult.SuggestedHost = candidateNode
				// 如果 Permit 返回 Wait 状态，会在绑定阶段进行相应的等待处理
				klog.V(4).InfoS("Successfully reserved on candidate node",
					"pod", klog.KObj(assumedPod),
					"node", candidateNode,
					"permitWait", permitStatus != nil && permitStatus.Code() == framework.Wait)
				// 继续后续流程（如果 Permit 返回 Wait，会在绑定阶段等待）
			} else {
				// 所有候选节点都尝试失败，执行调度失败处理逻辑
				metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
				sched.handleSchedulingFailure(fwk, assumedPodInfo, sts.AsError(), SchedulerError, clearNominatedNode)
				return
			}
		} else {
			// 没有候选节点可用，执行原有的调度失败处理逻辑
			metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
			// 清理首选节点上已经预留的资源
			fwk.RunReservePluginsUnreserve(schedulingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
			// 从调度器缓存中移除 Pod 的假设状态
			if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
				klog.ErrorS(forgetErr, "Scheduler cache ForgetPod failed")
			}
			// 处理调度失败
			sched.handleSchedulingFailure(fwk, assumedPodInfo, sts.AsError(), SchedulerError, clearNominatedNode)
			return
		}
	}

	// --- 概率性为次优节点预留资源 ---
	// 以 (1-p1)*p2 的概率同时为次优节点也预留资源，防止其他调度器占用
	// 其中 p1 是首选节点的采纳概率，p2 是次优节点的采纳概率
	if len(scheduleResult.CandidateNodes) >= 2 {
		shouldReserveSecondary := sched.shouldReserveSecondaryNode(
			scheduleResult.CandidateNodes,
			assumedPod,
		)

		if shouldReserveSecondary {
			secondaryNode := scheduleResult.CandidateNodes[1].Name
			klog.V(4).InfoS("Attempting to reserve secondary node by probability",
				"pod", klog.KObj(assumedPod),
				"primaryNode", scheduleResult.SuggestedHost,
				"secondaryNode", secondaryNode)

			// 为次优节点执行 Reserve
			// 注意：创建一个临时的 Pod 副本，因为 Reserve 插件可能需要 Pod 的 NodeName
			secondaryPod := assumedPod.DeepCopy()
			secondaryPod.Spec.NodeName = secondaryNode

			// 尝试为次优节点执行 Reserve
			if sts := fwk.RunReservePluginsReserve(schedulingCycleCtx, state, secondaryPod, secondaryNode); sts.IsSuccess() {
				// 记录次优节点被预留
				scheduleResult.SecondaryReservedNode = secondaryNode
				klog.V(4).InfoS("Successfully reserved secondary node",
					"pod", klog.KObj(assumedPod),
					"primaryNode", scheduleResult.SuggestedHost,
					"secondaryNode", secondaryNode)
			} else {
				// 次优节点 Reserve 失败，记录但不影响主流程
				klog.V(4).InfoS("Failed to reserve secondary node, continuing with primary node only",
					"pod", klog.KObj(assumedPod),
					"secondaryNode", secondaryNode,
					"status", sts)
			}
		}
	}

	// --- 运行 Permit 插件 ---
	// 运行 Permit 插件。
	// 这些插件可以批准、拒绝或延迟 Pod 的绑定。
	// 注意：如果 Reserve 阶段成功切换到候选节点并重新执行了 Permit，这里会再次执行 Permit
	// 这是必要的，因为 Permit 可能依赖节点特定信息，需要为最终选定的节点执行
	runPermitStatus := fwk.RunPermitPlugins(schedulingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
	// 如果 Permit 插件返回的状态不是 "Wait" 且不是成功。
	if runPermitStatus.Code() != framework.Wait && !runPermitStatus.IsSuccess() {
		// Permit失败，记录冲突
		sched.nodeHistoryManager.RecordConflict(scheduleResult.SuggestedHost)
		// 初始化失败原因。
		var reason string
		// 检查 Permit 插件返回的状态是否为 "Unschedulable"。
		if runPermitStatus.IsUnschedulable() {
			// 记录不可调度指标。
			metrics.PodUnschedulable(fwk.ProfileName(), metrics.SinceInSeconds(start))
			reason = v1.PodReasonUnschedulable
		} else {
			// 记录调度错误指标。
			metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
			reason = SchedulerError
		}
		// 其中一个插件返回了非成功或非等待的状态。
		// 触发 Unreserve 插件来清理状态。
		fwk.RunReservePluginsUnreserve(schedulingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
		// 从缓存中移除假设的 Pod。
		if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
			klog.ErrorS(forgetErr, "Scheduler cache ForgetPod failed")
		}
		// 处理调度失败。
		sched.handleSchedulingFailure(fwk, assumedPodInfo, runPermitStatus.AsError(), reason, clearNominatedNode)
		return
	}

	// --- 激活等待的 Pod（如果需要） ---
	// 在成功的调度周期结束时，如果需要，弹出并移动 Pod。
	// 检查是否有需要激活的 Pod。
	if len(podsToActivate.Map) != 0 {
		// 激活调度队列中指定的 Pod。
		sched.SchedulingQueue.Activate(podsToActivate.Map)
		// 激活后清除条目。
		podsToActivate.Map = make(map[string]*v1.Pod)
	}

	// --- 异步绑定 Pod ---
	// 异步地将 Pod 绑定到其主机（我们之所以可以这样做，是因为上面的假设步骤）。
	go func() {
		// 为绑定周期创建一个可取消的上下文。
		bindingCycleCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		// 增加绑定 goroutine 的计数。
		metrics.SchedulerGoroutines.WithLabelValues(metrics.Binding).Inc()
		defer metrics.SchedulerGoroutines.WithLabelValues(metrics.Binding).Dec()

		// 等待 Permit 插件的许可。
		waitOnPermitStatus := fwk.WaitOnPermit(bindingCycleCtx, assumedPod)
		// 如果等待许可失败。
		if !waitOnPermitStatus.IsSuccess() {
			// WaitOnPermit失败，记录冲突
			sched.nodeHistoryManager.RecordConflict(scheduleResult.SuggestedHost)
			// 初始化失败原因。
			var reason string
			// 检查等待许可的状态是否为 "Unschedulable"。
			if waitOnPermitStatus.IsUnschedulable() {
				// 记录不可调度指标。
				metrics.PodUnschedulable(fwk.ProfileName(), metrics.SinceInSeconds(start))
				reason = v1.PodReasonUnschedulable
			} else {
				// 记录调度错误指标。
				metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
				reason = SchedulerError
			}
			// 触发 Unreserve 插件来清理与预留 Pod 相关的状态。
			fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
			// 从缓存中移除假设的 Pod。
			if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
				klog.ErrorS(forgetErr, "scheduler cache ForgetPod failed")
			} else {
				// 在绑定周期中 "Forget" 一个假设的 Pod 应被视为 PodDelete 事件，
				// 因为假设的 Pod 在调度器缓存中占用了一定量的资源。
				// TODO(#103853): 去除重复逻辑。
				// 避免移动假设的 Pod 本身，因为它始终是 Unschedulable 的。
				// 故意 "defer" 这个操作；否则 MoveAllToActiveOrBackoffQueue() 会
				// 更新 `q.moveRequest`，从而将假设的 Pod 移动到 backoffQ。
				defer sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(internalqueue.AssignedPodDelete, func(pod *v1.Pod) bool {
					return assumedPod.UID != pod.UID
				})
			}
			// 处理调度失败。
			sched.handleSchedulingFailure(fwk, assumedPodInfo, waitOnPermitStatus.AsError(), reason, clearNominatedNode)
			return
		}

		// --- 运行 PreBind 插件 ---
		// 运行 "prebind" 插件。
		// 这些插件在 Pod 实际绑定到节点之前执行一些准备操作。
		preBindStatus := fwk.RunPreBindPlugins(bindingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
		// 如果 PreBind 插件执行失败。
		if !preBindStatus.IsSuccess() {
			// PreBind失败，记录冲突
			sched.nodeHistoryManager.RecordConflict(scheduleResult.SuggestedHost)

			// 如果有候选节点，尝试候选节点
			if len(scheduleResult.CandidateNodes) > 1 && nextNum <= 2 {
				klog.V(4).InfoS("PreBind failed for primary node, trying candidate nodes",
					"pod", klog.KObj(assumedPod),
					"primaryNode", scheduleResult.SuggestedHost)

				// 取消 Permit Wait（如果存在）
				sched.cancelPermitWait(fwk, assumedPod)

				// 清理首选节点的状态
				fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
				if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
					klog.ErrorS(forgetErr, "scheduler cache ForgetPod failed")
				}

				// 尝试候选节点（从第二个开始，因为第一个是首选节点）
				success, _, _ := sched.tryCandidateNodesForPreBind(
					bindingCycleCtx, fwk, state, assumedPod, assumedPodInfo,
					scheduleResult.CandidateNodes[nextNum:], start)

				if success {
					// 成功在候选节点上绑定，直接返回
					// 注意：这里已经完成了完整的绑定流程（包括 PostBind），所以直接返回
					return
				}
			}

			// 没有候选节点或所有候选节点都失败，执行原有失败处理逻辑
			metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
			fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
			if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
				klog.ErrorS(forgetErr, "scheduler cache ForgetPod failed")
			} else {
				// 在绑定周期中 "Forget" 一个假设的 Pod 应被视为 PodDelete 事件，
				// 因为假设的 Pod 在调度器缓存中占用了一定量的资源。
				// TODO(#103853): 去除重复逻辑。
				sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(internalqueue.AssignedPodDelete, nil)
			}
			// 处理调度失败。
			sched.handleSchedulingFailure(fwk, assumedPodInfo, preBindStatus.AsError(), SchedulerError, clearNominatedNode)
			return
		}

		// --- 尝试绑定到首选节点 ---
		// 尝试绑定到首选节点
		bindingNode := scheduleResult.SuggestedHost                             // 获取首选节点。
		err := sched.bind(bindingCycleCtx, fwk, assumedPod, bindingNode, state) // 尝试绑定。
		// 如果首选节点绑定失败。
		if err != nil {
			// 如果次优节点被预留了，需要清理
			if scheduleResult.SecondaryReservedNode != "" {
				secondaryPod := assumedPod.DeepCopy()
				secondaryPod.Spec.NodeName = scheduleResult.SecondaryReservedNode
				fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, secondaryPod, scheduleResult.SecondaryReservedNode)
				klog.V(4).InfoS("Cleaned up secondary node reservation due to primary node bind failure",
					"pod", klog.KObj(assumedPod),
					"primaryNode", bindingNode,
					"secondaryNode", scheduleResult.SecondaryReservedNode)
			}
			// 绑定失败，记录冲突
			sched.nodeHistoryManager.RecordConflict(bindingNode)
			// 记录日志，表示首选节点绑定失败，将尝试候选节点。
			klog.InfoS("Binding failed for primary node, trying candidate nodes",
				"pod", klog.KObj(assumedPod),
				"primaryNode", bindingNode,
				"error", err)

			// 如果首选节点绑定失败，尝试使用候选节点列表中的其他节点
			bound := false // 标记是否成功绑定到某个节点。
			founded := false
			// 取消 Permit Wait（如果存在）
			sched.cancelPermitWait(fwk, assumedPod)
			// 遍历调度结果中的候选节点列表。
			for i, candidate := range scheduleResult.CandidateNodes {
				if candidate.Name == bindingNode {
					founded = true
					continue
				}
				if i == 0 {
					// 第一个节点（首选节点）已经尝试过了，跳过。
					continue
				}
				if !founded {
					continue
				}
				// 取消之前的预留（在首选节点上）。
				fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, assumedPod, bindingNode)
				// 从缓存中移除假设的 Pod（从首选节点）。
				if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
					klog.ErrorS(forgetErr, "scheduler cache ForgetPod failed")
				}

				// 尝试绑定到候选节点
				bindingNode = candidate.Name // 更新绑定节点为目标候选节点。
				// 记录日志，表示正在尝试绑定到候选节点。
				klog.V(4).InfoS("Trying to bind to candidate node",
					"pod", klog.KObj(assumedPod),
					"candidateNode", bindingNode,
					"candidateIndex", i)

				// 重新假设pod到新节点
				assumedPod.Spec.NodeName = bindingNode // 更新 Pod 的 NodeName。
				// 将 Pod 假设到新的候选节点上。
				if assumeErr := sched.Cache.AssumePod(assumedPod); assumeErr != nil {
					// 如果假设失败，记录错误并记录冲突。
					klog.ErrorS(assumeErr, "Scheduler cache AssumePod failed for candidate node", "node", bindingNode)
					sched.nodeHistoryManager.RecordConflict(bindingNode)
					continue // 继续尝试下一个候选节点。
				}

				// 重新运行Reserve插件
				// 为新的候选节点运行 Reserve 插件。
				if sts := fwk.RunReservePluginsReserve(bindingCycleCtx, state, assumedPod, bindingNode); !sts.IsSuccess() {
					// 如果 Reserve 失败，运行 Unreserve 清理，移除 Pod，记录冲突，然后继续。
					fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, assumedPod, bindingNode)
					if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
						klog.ErrorS(forgetErr, "scheduler cache ForgetPod failed")
					}
					sched.nodeHistoryManager.RecordConflict(bindingNode)
					continue
				}

				// 重新执行 Permit 插件（因为节点变了）
				permitStatus, needWait := sched.reexecutePermitForCandidate(bindingCycleCtx, fwk, state, assumedPod, bindingNode)
				if permitStatus != nil && !permitStatus.IsSuccess() && permitStatus.Code() != framework.Wait {
					// Permit 失败或拒绝，清理状态
					fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, assumedPod, bindingNode)
					if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
						klog.ErrorS(forgetErr, "scheduler cache ForgetPod failed")
					}
					sched.nodeHistoryManager.RecordConflict(bindingNode)
					continue
				}

				// 如果 Permit 返回 Wait，需要等待
				if needWait {
					waitOnPermitStatus := fwk.WaitOnPermit(bindingCycleCtx, assumedPod)
					if !waitOnPermitStatus.IsSuccess() {
						// WaitOnPermit 失败，清理状态
						fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, assumedPod, bindingNode)
						if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
							klog.ErrorS(forgetErr, "scheduler cache ForgetPod failed")
						}
						sched.nodeHistoryManager.RecordConflict(bindingNode)
						continue
					}
				}

				// 尝试 PreBind
				if preBindStatus := fwk.RunPreBindPlugins(bindingCycleCtx, state, assumedPod, bindingNode); !preBindStatus.IsSuccess() {
					fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, assumedPod, bindingNode)
					if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
						klog.ErrorS(forgetErr, "scheduler cache ForgetPod failed")
					}
					sched.nodeHistoryManager.RecordConflict(bindingNode)
					continue
				}

				// 尝试绑定
				// 尝试将 Pod 绑定到当前候选节点。
				if bindErr := sched.bind(bindingCycleCtx, fwk, assumedPod, bindingNode, state); bindErr != nil {
					// 如果绑定失败，记录冲突，运行 Unreserve 清理，移除 Pod，然后继续尝试下一个候选节点。
					sched.nodeHistoryManager.RecordConflict(bindingNode)
					fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, assumedPod, bindingNode)
					if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
						klog.ErrorS(forgetErr, "scheduler cache ForgetPod failed")
					}
					continue
				}

				// 绑定成功
				bound = true // 标记为已成功绑定。
				// 记录日志，表示成功绑定到候选节点。
				klog.V(2).InfoS("Successfully bound pod to candidate node",
					"pod", klog.KObj(assumedPod),
					"node", bindingNode,
					"candidateIndex", i)
				break // 退出循环，因为已经成功绑定。
			}

			// 检查是否在任何节点上成功绑定。
			if !bound {
				// 所有候选节点都失败了
				// 记录调度错误指标。
				metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
				// 运行 Unreserve 清理最后尝试的节点。
				fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, assumedPod, bindingNode)
				// 从缓存中移除 Pod。
				if err := sched.Cache.ForgetPod(assumedPod); err != nil {
					klog.ErrorS(err, "scheduler cache ForgetPod failed")
				} else {
					// 将队列中的 Pod 移动到活动或回退队列。
					sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(internalqueue.AssignedPodDelete, nil)
				}
				// 处理调度失败，原因是所有候选节点都绑定失败。
				sched.handleSchedulingFailure(fwk, assumedPodInfo, fmt.Errorf("binding rejected for all candidate nodes: %w", err), SchedulerError, clearNominatedNode)
				return
			}
		}

		// 绑定成功，记录成功统计
		sched.nodeHistoryManager.RecordSuccess(bindingNode)

		// 清理次优节点的预留（如果存在）
		if scheduleResult.SecondaryReservedNode != "" {
			secondaryPod := assumedPod.DeepCopy()
			secondaryPod.Spec.NodeName = scheduleResult.SecondaryReservedNode
			fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, secondaryPod, scheduleResult.SecondaryReservedNode)
			klog.V(4).InfoS("Cleaned up secondary node reservation after successful binding",
				"pod", klog.KObj(assumedPod),
				"primaryNode", bindingNode,
				"secondaryNode", scheduleResult.SecondaryReservedNode)
		}

		// 计算 nodeResourceString 可能比较耗时。如果 klog 详细程度低于 2，则避免计算。
		klog.V(2).InfoS("Successfully bound pod to node", "pod", klog.KObj(pod), "node", bindingNode, "evaluatedNodes", scheduleResult.EvaluatedNodes, "feasibleNodes", scheduleResult.FeasibleNodes)
		// 记录 Pod 成功调度的指标。
		metrics.PodScheduled(fwk.ProfileName(), metrics.SinceInSeconds(start))
		// 记录 Pod 的调度尝试次数。
		metrics.PodSchedulingAttempts.Observe(float64(podInfo.Attempts))
		// 记录 Pod 的调度总持续时间。
		metrics.PodSchedulingDuration.WithLabelValues(getAttemptsLabel(podInfo)).Observe(metrics.SinceInSeconds(podInfo.InitialAttemptTimestamp))

		// --- 运行 PostBind 插件 ---
		// 运行 "postbind" 插件。
		// 这些插件在 Pod 成功绑定到节点后执行一些清理或通知操作。
		fwk.RunPostBindPlugins(bindingCycleCtx, state, assumedPod, bindingNode)

		// --- 激活等待的 Pod（如果需要） ---
		// 在成功的绑定周期结束时，如果需要，移动 Pod。
		// 检查是否有需要激活的 Pod。
		if len(podsToActivate.Map) != 0 {
			// 激活调度队列中指定的 Pod。
			sched.SchedulingQueue.Activate(podsToActivate.Map)
			// 与调度周期中的逻辑不同，我们不费心删除条目
			// 因为 `podsToActivate.Map` 不再被消费。
		}
	}()
}

func (sched *Scheduler) frameworkForPod(pod *v1.Pod) (framework.Framework, error) {

	fwk, ok := sched.Profiles[pod.Spec.SchedulerName]
	if !ok {
		return nil, fmt.Errorf("profile not found for scheduler name %q", pod.Spec.SchedulerName)
	}
	return fwk, nil
}

// skipPodSchedule returns true if we could skip scheduling the pod for specified cases.
func (sched *Scheduler) skipPodSchedule(fwk framework.Framework, pod *v1.Pod) bool {
	// Case 1: pod is being deleted.
	if pod.DeletionTimestamp != nil {
		fwk.EventRecorder().Eventf(pod, nil, v1.EventTypeWarning, "FailedScheduling", "Scheduling", "skip schedule deleting pod: %v/%v", pod.Namespace, pod.Name)
		klog.V(3).InfoS("Skip schedule deleting pod", "pod", klog.KObj(pod))
		return true
	}

	// Case 2: pod that has been assumed could be skipped.
	// An assumed pod can be added again to the scheduling queue if it got an update event
	// during its previous scheduling cycle but before getting assumed.
	isAssumed, err := sched.Cache.IsAssumedPod(pod)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to check whether pod %s/%s is assumed: %v", pod.Namespace, pod.Name, err))
		return false
	}
	return isAssumed
}

// schedulePod 尝试将给定的 Pod 调度到节点列表中的一个节点上
// 如果成功，将返回节点名称
// 如果失败，将返回包含原因的 FitError
func (sched *Scheduler) schedulePod(ctx context.Context, fwk framework.Framework, state *framework.CycleState, pod *v1.Pod) (result ScheduleResult, err error) {
	// 创建调度跟踪，记录调度过程中的性能数据
	trace := utiltrace.New("Scheduling", utiltrace.Field{Key: "namespace", Value: pod.Namespace}, utiltrace.Field{Key: "name", Value: pod.Name})
	defer trace.LogIfLong(100 * time.Millisecond)

	// 更新调度器缓存快照，确保节点信息是最新的
	if err := sched.Cache.UpdateSnapshot(sched.nodeInfoSnapshot); err != nil {
		return result, err
	}
	trace.Step("Snapshotting scheduler cache and node infos done")

	// 检查是否有可用节点，如果没有则返回错误
	if sched.nodeInfoSnapshot.NumNodes() == 0 {
		return result, ErrNoNodesAvailable
	}

	// 找到适合 Pod 的节点列表（过滤阶段）
	feasibleNodes, diagnosis, err := sched.findNodesThatFitPod(ctx, fwk, state, pod)
	if err != nil {
		return result, err
	}
	trace.Step("Computing predicates done")

	// 如果没有找到合适的节点，返回调度失败的错误信息
	if len(feasibleNodes) == 0 {
		return result, &framework.FitError{
			Pod:         pod,  // 待调度的 Pod
			NumAllNodes: sched.nodeInfoSnapshot.NumNodes(), // 总节点数
			Diagnosis:   diagnosis, // 诊断信息，包含哪些节点不满足条件及原因
		}
	}

	// 当只有一个节点满足条件时，直接使用该节点，无需进行优先级排序
	if len(feasibleNodes) == 1 {
		return ScheduleResult{
			SuggestedHost:  feasibleNodes[0].Name,     // 建议的主机名
			EvaluatedNodes: 1 + len(diagnosis.NodeToStatusMap), // 评估的节点数
			FeasibleNodes:  1,                       // 可行的节点数
		}, nil
	}

	// 对可行节点进行优先级排序（评分阶段）
	priorityList, err := prioritizeNodes(ctx, sched.Extenders, fwk, state, pod, feasibleNodes)
	if err != nil {
		return result, err
	}

	// 选择多个候选节点（最多保留前5个）
	maxCandidates := 5
	candidateNodes, err := selectCandidateNodes(priorityList, maxCandidates)
	if err != nil {
		return result, err
	}

	// 计算每个候选节点的采纳概率
	// 采纳概率基于节点的历史调度成功率和当前评分
	for i := range candidateNodes {
		candidateNodes[i].AdoptionProbability = sched.nodeHistoryManager.CalculateAdoptionProbability(
			candidateNodes[i].Name,   // 节点名称
			candidateNodes[i].Score,  // 节点评分
		)
	}

	// 根据累积概率选择主节点和备选节点
	// 逻辑：
	// - 第一个节点的采纳率为 p1，有 p1 的概率直接 reserve 第一个节点
	// - 如果第一个节点没有被选中，第二个节点的采纳率为 p2，有 (1-p1)*p2 的概率 reserve 第二个节点
	// - 如果前两个都没被选中，第三个节点的采纳率为 p3，有 (1-p1)*(1-p2)*p3 的概率 reserve 第三个节点
	// - 总共会选取三个备选节点（包括主节点）
	const maxSelectedCandidates = 3
	selectedCandidates := sched.selectNodesByCumulativeProbability(candidateNodes, maxSelectedCandidates, pod)

	// 确定主节点（第一个被选中的节点）
	selectedHost := selectedCandidates[0].Name
	candidateNodes = selectedCandidates

	// 构建候选节点名称列表用于日志记录
	candidateNames := make([]string, len(candidateNodes))
	for i, c := range candidateNodes {
		candidateNames[i] = c.Name
	}
	// 记录选中的候选节点信息，便于调试和监控
	klog.V(4).InfoS("Selected candidate nodes based on cumulative adoption probability",
		"pod", klog.KObj(pod),        // Pod 对象信息
		"selectedNode", selectedHost, // 选中的节点
		"candidateCount", len(candidateNodes), // 候选节点数量
		"candidates", candidateNames)  // 候选节点名称列表

	trace.Step("Prioritizing done")

	// 返回调度结果，包括建议的主机、候选节点列表和统计信息
	return ScheduleResult{
		SuggestedHost:  selectedHost,      // 建议调度到的主机名（主节点）
		CandidateNodes: candidateNodes,    // 候选节点列表（包括主节点和备选节点）
		EvaluatedNodes: len(feasibleNodes) + len(diagnosis.NodeToStatusMap), // 评估的节点总数
		FeasibleNodes:  len(feasibleNodes), // 符合条件的节点数量
	}, nil
}

// Filters the nodes to find the ones that fit the pod based on the framework
// filter plugins and filter extenders.
func (sched *Scheduler) findNodesThatFitPod(ctx context.Context, fwk framework.Framework, state *framework.CycleState, pod *v1.Pod) ([]*v1.Node, framework.Diagnosis, error) {
	diagnosis := framework.Diagnosis{
		NodeToStatusMap:      make(framework.NodeToStatusMap),
		UnschedulablePlugins: sets.NewString(),
	}

	// Run "prefilter" plugins.
	preRes, s := fwk.RunPreFilterPlugins(ctx, state, pod)
	allNodes, err := sched.nodeInfoSnapshot.NodeInfos().List()
	if err != nil {
		return nil, diagnosis, err
	}
	if !s.IsSuccess() {
		if !s.IsUnschedulable() {
			return nil, diagnosis, s.AsError()
		}
		// All nodes will have the same status. Some non trivial refactoring is
		// needed to avoid this copy.
		for _, n := range allNodes {
			diagnosis.NodeToStatusMap[n.Node().Name] = s
		}
		// Status satisfying IsUnschedulable() gets injected into diagnosis.UnschedulablePlugins.
		if s.FailedPlugin() != "" {
			diagnosis.UnschedulablePlugins.Insert(s.FailedPlugin())
		}
		return nil, diagnosis, nil
	}

	// "NominatedNodeName" can potentially be set in a previous scheduling cycle as a result of preemption.
	// This node is likely the only candidate that will fit the pod, and hence we try it first before iterating over all nodes.
	if len(pod.Status.NominatedNodeName) > 0 {
		feasibleNodes, err := sched.evaluateNominatedNode(ctx, pod, fwk, state, diagnosis)
		if err != nil {
			klog.ErrorS(err, "Evaluation failed on nominated node", "pod", klog.KObj(pod), "node", pod.Status.NominatedNodeName)
		}
		// Nominated node passes all the filters, scheduler is good to assign this node to the pod.
		if len(feasibleNodes) != 0 {
			return feasibleNodes, diagnosis, nil
		}
	}

	nodes := allNodes
	if !preRes.AllNodes() {
		nodes = make([]*framework.NodeInfo, 0, len(preRes.NodeNames))
		for n := range preRes.NodeNames {
			nInfo, err := sched.nodeInfoSnapshot.NodeInfos().Get(n)
			if err != nil {
				return nil, diagnosis, err
			}
			nodes = append(nodes, nInfo)
		}
	}
	feasibleNodes, err := sched.findNodesThatPassFilters(ctx, fwk, state, pod, diagnosis, nodes)
	if err != nil {
		return nil, diagnosis, err
	}

	feasibleNodes, err = findNodesThatPassExtenders(sched.Extenders, pod, feasibleNodes, diagnosis.NodeToStatusMap)
	if err != nil {
		return nil, diagnosis, err
	}
	return feasibleNodes, diagnosis, nil
}

func (sched *Scheduler) evaluateNominatedNode(ctx context.Context, pod *v1.Pod, fwk framework.Framework, state *framework.CycleState, diagnosis framework.Diagnosis) ([]*v1.Node, error) {
	nnn := pod.Status.NominatedNodeName
	nodeInfo, err := sched.nodeInfoSnapshot.Get(nnn)
	if err != nil {
		return nil, err
	}
	node := []*framework.NodeInfo{nodeInfo}
	feasibleNodes, err := sched.findNodesThatPassFilters(ctx, fwk, state, pod, diagnosis, node)
	if err != nil {
		return nil, err
	}

	feasibleNodes, err = findNodesThatPassExtenders(sched.Extenders, pod, feasibleNodes, diagnosis.NodeToStatusMap)
	if err != nil {
		return nil, err
	}

	return feasibleNodes, nil
}

// findNodesThatPassFilters finds the nodes that fit the filter plugins.
func (sched *Scheduler) findNodesThatPassFilters(
	ctx context.Context,
	fwk framework.Framework,
	state *framework.CycleState,
	pod *v1.Pod,
	diagnosis framework.Diagnosis,
	nodes []*framework.NodeInfo) ([]*v1.Node, error) {
	numNodesToFind := sched.numFeasibleNodesToFind(int32(len(nodes)))

	// Create feasible list with enough space to avoid growing it
	// and allow assigning.
	feasibleNodes := make([]*v1.Node, numNodesToFind)

	if !fwk.HasFilterPlugins() {
		length := len(nodes)
		for i := range feasibleNodes {
			feasibleNodes[i] = nodes[(sched.nextStartNodeIndex+i)%length].Node()
		}
		sched.nextStartNodeIndex = (sched.nextStartNodeIndex + len(feasibleNodes)) % length
		return feasibleNodes, nil
	}

	errCh := parallelize.NewErrorChannel()
	var statusesLock sync.Mutex
	var feasibleNodesLen int32
	ctx, cancel := context.WithCancel(ctx)
	checkNode := func(i int) {
		// We check the nodes starting from where we left off in the previous scheduling cycle,
		// this is to make sure all nodes have the same chance of being examined across pods.
		nodeInfo := nodes[(sched.nextStartNodeIndex+i)%len(nodes)]
		status := fwk.RunFilterPluginsWithNominatedPods(ctx, state, pod, nodeInfo)
		if status.Code() == framework.Error {
			errCh.SendErrorWithCancel(status.AsError(), cancel)
			return
		}
		if status.IsSuccess() {
			length := atomic.AddInt32(&feasibleNodesLen, 1)
			if length > numNodesToFind {
				cancel()
				atomic.AddInt32(&feasibleNodesLen, -1)
			} else {
				feasibleNodes[length-1] = nodeInfo.Node()
			}
		} else {
			statusesLock.Lock()
			diagnosis.NodeToStatusMap[nodeInfo.Node().Name] = status
			diagnosis.UnschedulablePlugins.Insert(status.FailedPlugin())
			statusesLock.Unlock()
		}
	}

	beginCheckNode := time.Now()
	statusCode := framework.Success
	defer func() {
		// We record Filter extension point latency here instead of in framework.go because framework.RunFilterPlugins
		// function is called for each node, whereas we want to have an overall latency for all nodes per scheduling cycle.
		// Note that this latency also includes latency for `addNominatedPods`, which calls framework.RunPreFilterAddPod.
		metrics.FrameworkExtensionPointDuration.WithLabelValues(frameworkruntime.Filter, statusCode.String(), fwk.ProfileName()).Observe(metrics.SinceInSeconds(beginCheckNode))
	}()

	// Stops searching for more nodes once the configured number of feasible nodes
	// are found.
	fwk.Parallelizer().Until(ctx, len(nodes), checkNode)
	processedNodes := int(feasibleNodesLen) + len(diagnosis.NodeToStatusMap)
	sched.nextStartNodeIndex = (sched.nextStartNodeIndex + processedNodes) % len(nodes)

	feasibleNodes = feasibleNodes[:feasibleNodesLen]
	if err := errCh.ReceiveError(); err != nil {
		statusCode = framework.Error
		return nil, err
	}
	return feasibleNodes, nil
}

// numFeasibleNodesToFind returns the number of feasible nodes that once found, the scheduler stops
// its search for more feasible nodes.
func (sched *Scheduler) numFeasibleNodesToFind(numAllNodes int32) (numNodes int32) {
	if numAllNodes < minFeasibleNodesToFind || sched.percentageOfNodesToScore >= 100 {
		return numAllNodes
	}

	adaptivePercentage := sched.percentageOfNodesToScore
	if adaptivePercentage <= 0 {
		basePercentageOfNodesToScore := int32(50)
		adaptivePercentage = basePercentageOfNodesToScore - numAllNodes/125
		if adaptivePercentage < minFeasibleNodesPercentageToFind {
			adaptivePercentage = minFeasibleNodesPercentageToFind
		}
	}

	numNodes = numAllNodes * adaptivePercentage / 100
	if numNodes < minFeasibleNodesToFind {
		return minFeasibleNodesToFind
	}

	return numNodes
}

func findNodesThatPassExtenders(extenders []framework.Extender, pod *v1.Pod, feasibleNodes []*v1.Node, statuses framework.NodeToStatusMap) ([]*v1.Node, error) {
	// Extenders are called sequentially.
	// Nodes in original feasibleNodes can be excluded in one extender, and pass on to the next
	// extender in a decreasing manner.
	for _, extender := range extenders {
		if len(feasibleNodes) == 0 {
			break
		}
		if !extender.IsInterested(pod) {
			continue
		}

		// Status of failed nodes in failedAndUnresolvableMap will be added or overwritten in <statuses>,
		// so that the scheduler framework can respect the UnschedulableAndUnresolvable status for
		// particular nodes, and this may eventually improve preemption efficiency.
		// Note: users are recommended to configure the extenders that may return UnschedulableAndUnresolvable
		// status ahead of others.
		feasibleList, failedMap, failedAndUnresolvableMap, err := extender.Filter(pod, feasibleNodes)
		if err != nil {
			if extender.IsIgnorable() {
				klog.InfoS("Skipping extender as it returned error and has ignorable flag set", "extender", extender, "err", err)
				continue
			}
			return nil, err
		}

		for failedNodeName, failedMsg := range failedAndUnresolvableMap {
			var aggregatedReasons []string
			if _, found := statuses[failedNodeName]; found {
				aggregatedReasons = statuses[failedNodeName].Reasons()
			}
			aggregatedReasons = append(aggregatedReasons, failedMsg)
			statuses[failedNodeName] = framework.NewStatus(framework.UnschedulableAndUnresolvable, aggregatedReasons...)
		}

		for failedNodeName, failedMsg := range failedMap {
			if _, found := failedAndUnresolvableMap[failedNodeName]; found {
				// failedAndUnresolvableMap takes precedence over failedMap
				// note that this only happens if the extender returns the node in both maps
				continue
			}
			if _, found := statuses[failedNodeName]; !found {
				statuses[failedNodeName] = framework.NewStatus(framework.Unschedulable, failedMsg)
			} else {
				statuses[failedNodeName].AppendReason(failedMsg)
			}
		}

		feasibleNodes = feasibleList
	}
	return feasibleNodes, nil
}

// prioritizeNodes prioritizes the nodes by running the score plugins,
// which return a score for each node from the call to RunScorePlugins().
// The scores from each plugin are added together to make the score for that node, then
// any extenders are run as well.
// All scores are finally combined (added) to get the total weighted scores of all nodes
func prioritizeNodes(
	ctx context.Context,
	extenders []framework.Extender,
	fwk framework.Framework,
	state *framework.CycleState,
	pod *v1.Pod,
	nodes []*v1.Node,
) (framework.NodeScoreList, error) {
	// If no priority configs are provided, then all nodes will have a score of one.
	// This is required to generate the priority list in the required format
	if len(extenders) == 0 && !fwk.HasScorePlugins() {
		result := make(framework.NodeScoreList, 0, len(nodes))
		for i := range nodes {
			result = append(result, framework.NodeScore{
				Name:  nodes[i].Name,
				Score: 1,
			})
		}
		return result, nil
	}

	// Run PreScore plugins.
	preScoreStatus := fwk.RunPreScorePlugins(ctx, state, pod, nodes)
	if !preScoreStatus.IsSuccess() {
		return nil, preScoreStatus.AsError()
	}

	// Run the Score plugins.
	scoresMap, scoreStatus := fwk.RunScorePlugins(ctx, state, pod, nodes)
	if !scoreStatus.IsSuccess() {
		return nil, scoreStatus.AsError()
	}

	// Additional details logged at level 10 if enabled.
	klogV := klog.V(10)
	if klogV.Enabled() {
		for plugin, nodeScoreList := range scoresMap {
			for _, nodeScore := range nodeScoreList {
				klogV.InfoS("Plugin scored node for pod", "pod", klog.KObj(pod), "plugin", plugin, "node", nodeScore.Name, "score", nodeScore.Score)
			}
		}
	}

	// Summarize all scores.
	result := make(framework.NodeScoreList, 0, len(nodes))

	for i := range nodes {
		result = append(result, framework.NodeScore{Name: nodes[i].Name, Score: 0})
		for j := range scoresMap {
			result[i].Score += scoresMap[j][i].Score
		}
	}

	if len(extenders) != 0 && nodes != nil {
		var mu sync.Mutex
		var wg sync.WaitGroup
		combinedScores := make(map[string]int64, len(nodes))
		for i := range extenders {
			if !extenders[i].IsInterested(pod) {
				continue
			}
			wg.Add(1)
			go func(extIndex int) {
				metrics.SchedulerGoroutines.WithLabelValues(metrics.PrioritizingExtender).Inc()
				defer func() {
					metrics.SchedulerGoroutines.WithLabelValues(metrics.PrioritizingExtender).Dec()
					wg.Done()
				}()
				prioritizedList, weight, err := extenders[extIndex].Prioritize(pod, nodes)
				if err != nil {
					// Prioritization errors from extender can be ignored, let k8s/other extenders determine the priorities
					klog.V(5).InfoS("Failed to run extender's priority function. No score given by this extender.", "error", err, "pod", klog.KObj(pod), "extender", extenders[extIndex].Name())
					return
				}
				mu.Lock()
				for i := range *prioritizedList {
					host, score := (*prioritizedList)[i].Host, (*prioritizedList)[i].Score
					if klogV.Enabled() {
						klogV.InfoS("Extender scored node for pod", "pod", klog.KObj(pod), "extender", extenders[extIndex].Name(), "node", host, "score", score)
					}
					combinedScores[host] += score * weight
				}
				mu.Unlock()
			}(i)
		}
		// wait for all go routines to finish
		wg.Wait()
		for i := range result {
			// MaxExtenderPriority may diverge from the max priority used in the scheduler and defined by MaxNodeScore,
			// therefore we need to scale the score returned by extenders to the score range used by the scheduler.
			result[i].Score += combinedScores[result[i].Name] * (framework.MaxNodeScore / extenderv1.MaxExtenderPriority)
		}
	}

	if klogV.Enabled() {
		for i := range result {
			klogV.InfoS("Calculated node's final score for pod", "pod", klog.KObj(pod), "node", result[i].Name, "score", result[i].Score)
		}
	}
	return result, nil
}

// selectHost takes a prioritized list of nodes and then picks one
// in a reservoir sampling manner from the nodes that had the highest score.
// 保留原有函数以保持向后兼容
// selectHost 从节点评分列表中根据分数选择一个节点。
// 如果有多个节点具有相同的最高分，则随机选择其中一个。
// 这确保了在分数相同的情况下，所有最高分节点都有相等的被选中概率。
func selectHost(nodeScoreList framework.NodeScoreList) (string, error) {
	// 检查输入的节点评分列表是否为空。
	if len(nodeScoreList) == 0 {
		// 如果列表为空，则返回错误。
		return "", fmt.Errorf("empty priorityList")
	}

	// 初始化最高分和选中节点名称为列表中的第一个节点。
	maxScore := nodeScoreList[0].Score
	selected := nodeScoreList[0].Name
	// 记录当前最高分的节点数量，初始为1（因为第一个节点就是当前最高分）。
	cntOfMaxScore := 1

	// 遍历列表中剩余的节点（从第二个开始）。
	for _, ns := range nodeScoreList[1:] {
		// 如果当前节点的分数高于已知的最高分。
		if ns.Score > maxScore {
			// 更新最高分、选中节点名称，并将最高分节点计数重置为1。
			maxScore = ns.Score
			selected = ns.Name
			cntOfMaxScore = 1
		} else if ns.Score == maxScore { // 如果当前节点的分数等于已知的最高分。
			// 增加最高分节点的计数。
			cntOfMaxScore++
			// 使用 reservoir sampling 算法来公平地随机选择。
			// 生成一个 [0, cntOfMaxScore) 范围内的随机整数。
			// 如果这个随机数恰好是 0，我们就用当前节点替换之前选中的节点。
			// 这样可以保证在所有最高分节点中，每个节点被选中的概率都是 1 / cntOfMaxScore。
			if rand.Intn(cntOfMaxScore) == 0 {
				// 替换候选节点的概率为 1/cntOfMaxScore。
				selected = ns.Name
			}
		}
	}

	// 返回选中的节点名称。
	return selected, nil
}

// selectCandidateNodes 选择多个候选节点（按得分从高到低排序）
// maxCandidates 指定最多返回多少个候选节点
func selectCandidateNodes(nodeScoreList framework.NodeScoreList, maxCandidates int) ([]CandidateNode, error) {
	// 检查输入的节点评分列表是否为空。
	if len(nodeScoreList) == 0 {
		// 如果列表为空，则返回错误。
		return nil, fmt.Errorf("empty priorityList")
	}

	// 创建一个新切片来存储排序后的节点列表，避免修改原始列表。
	// 切片的长度与原始列表相同。
	sortedList := make(framework.NodeScoreList, len(nodeScoreList))
	// 将原始列表的内容复制到新的切片中。
	copy(sortedList, nodeScoreList)
	// 使用 sort.Slice 对新切片进行排序。
	// 排序规则是按 Score 从高到低排序（i.Score > j.Score）。
	sort.Slice(sortedList, func(i, j int) bool {
		return sortedList[i].Score > sortedList[j].Score
	})

	// 确定要返回的候选节点数量。
	// 默认情况下，候选数量是排序后列表的长度。
	candidateCount := len(sortedList)
	// 如果指定了 maxCandidates（大于0），并且排序后列表的长度超过了 maxCandidates，
	// 则将候选数量限制为 maxCandidates。
	if maxCandidates > 0 && candidateCount > maxCandidates {
		candidateCount = maxCandidates
	}

	// 创建一个切片来存储最终的候选节点。
	// 切片的类型是 []CandidateNode，容量预设为 candidateCount 以提高效率。
	candidates := make([]CandidateNode, 0, candidateCount)
	// 遍历排序后列表的前 candidateCount 个元素。
	for i := 0; i < candidateCount; i++ {
		// 将每个节点的 Name 和 Score 封装成 CandidateNode 结构体，
		// 并追加到 candidates 切片中。
		candidates = append(candidates, CandidateNode{
			Name:  sortedList[i].Name,  // 节点名称
			Score: sortedList[i].Score, // 节点分数
		})
	}

	// 返回包含最高分节点的候选列表和 nil 错误。
	return candidates, nil
}

// assume signals to the cache that a pod is already in the cache, so that binding can be asynchronous.
// assume modifies `assumed`.
func (sched *Scheduler) assume(assumed *v1.Pod, host string) error {
	// Optimistically assume that the binding will succeed and send it to apiserver
	// in the background.
	// If the binding fails, scheduler will release resources allocated to assumed pod
	// immediately.
	assumed.Spec.NodeName = host

	if err := sched.Cache.AssumePod(assumed); err != nil {
		klog.ErrorS(err, "Scheduler cache AssumePod failed")
		return err
	}
	// if "assumed" is a nominated pod, we should remove it from internal cache
	if sched.SchedulingQueue != nil {
		sched.SchedulingQueue.DeleteNominatedPodIfExists(assumed)
	}

	return nil
}

// bind binds a pod to a given node defined in a binding object.
// The precedence for binding is: (1) extenders and (2) framework plugins.
// We expect this to run asynchronously, so we handle binding metrics internally.
func (sched *Scheduler) bind(ctx context.Context, fwk framework.Framework, assumed *v1.Pod, targetNode string, state *framework.CycleState) (err error) {
	defer func() {
		sched.finishBinding(fwk, assumed, targetNode, err)
	}()

	bound, err := sched.extendersBinding(assumed, targetNode)
	if bound {
		return err
	}
	bindStatus := fwk.RunBindPlugins(ctx, state, assumed, targetNode)
	if bindStatus.IsSuccess() {
		return nil
	}
	if bindStatus.Code() == framework.Error {
		return bindStatus.AsError()
	}
	return fmt.Errorf("bind status: %s, %v", bindStatus.Code().String(), bindStatus.Message())
}

// TODO(#87159): Move this to a Plugin.
func (sched *Scheduler) extendersBinding(pod *v1.Pod, node string) (bool, error) {
	for _, extender := range sched.Extenders {
		if !extender.IsBinder() || !extender.IsInterested(pod) {
			continue
		}
		return true, extender.Bind(&v1.Binding{
			ObjectMeta: metav1.ObjectMeta{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
			Target:     v1.ObjectReference{Kind: "Node", Name: node},
		})
	}
	return false, nil
}

func (sched *Scheduler) finishBinding(fwk framework.Framework, assumed *v1.Pod, targetNode string, err error) {
	if finErr := sched.Cache.FinishBinding(assumed); finErr != nil {
		klog.ErrorS(finErr, "Scheduler cache FinishBinding failed")
	}
	if err != nil {
		klog.V(1).InfoS("Failed to bind pod", "pod", klog.KObj(assumed))
		return
	}

	fwk.EventRecorder().Eventf(assumed, nil, v1.EventTypeNormal, "Scheduled", "Binding", "Successfully assigned %v/%v to %v", assumed.Namespace, assumed.Name, targetNode)
}

func getAttemptsLabel(p *framework.QueuedPodInfo) string {
	// We breakdown the pod scheduling duration by attempts capped to a limit
	// to avoid ending up with a high cardinality metric.
	if p.Attempts >= 15 {
		return "15+"
	}
	return strconv.Itoa(p.Attempts)
}

// handleSchedulingFailure records an event for the pod that indicates the
// pod has failed to schedule. Also, update the pod condition and nominated node name if set.
func (sched *Scheduler) handleSchedulingFailure(fwk framework.Framework, podInfo *framework.QueuedPodInfo, err error, reason string, nominatingInfo *framework.NominatingInfo) {
	sched.Error(podInfo, err)

	// Update the scheduling queue with the nominated pod information. Without
	// this, there would be a race condition between the next scheduling cycle
	// and the time the scheduler receives a Pod Update for the nominated pod.
	// Here we check for nil only for tests.
	if sched.SchedulingQueue != nil {
		sched.SchedulingQueue.AddNominatedPod(podInfo.PodInfo, nominatingInfo)
	}

	pod := podInfo.Pod
	msg := truncateMessage(err.Error())
	fwk.EventRecorder().Eventf(pod, nil, v1.EventTypeWarning, "FailedScheduling", "Scheduling", msg)
	if err := updatePod(sched.client, pod, &v1.PodCondition{
		Type:    v1.PodScheduled,
		Status:  v1.ConditionFalse,
		Reason:  reason,
		Message: err.Error(),
	}, nominatingInfo); err != nil {
		klog.ErrorS(err, "Error updating pod", "pod", klog.KObj(pod))
	}
}

// truncateMessage truncates a message if it hits the NoteLengthLimit.
func truncateMessage(message string) string {
	max := validation.NoteLengthLimit
	if len(message) <= max {
		return message
	}
	suffix := " ..."
	return message[:max-len(suffix)] + suffix
}

func updatePod(client clientset.Interface, pod *v1.Pod, condition *v1.PodCondition, nominatingInfo *framework.NominatingInfo) error {
	klog.V(3).InfoS("Updating pod condition", "pod", klog.KObj(pod), "conditionType", condition.Type, "conditionStatus", condition.Status, "conditionReason", condition.Reason)
	podStatusCopy := pod.Status.DeepCopy()
	// NominatedNodeName is updated only if we are trying to set it, and the value is
	// different from the existing one.
	nnnNeedsUpdate := nominatingInfo.Mode() == framework.ModeOverride && pod.Status.NominatedNodeName != nominatingInfo.NominatedNodeName
	if !podutil.UpdatePodCondition(podStatusCopy, condition) && !nnnNeedsUpdate {
		return nil
	}
	if nnnNeedsUpdate {
		podStatusCopy.NominatedNodeName = nominatingInfo.NominatedNodeName
	}
	return util.PatchPodStatus(client, pod, podStatusCopy)
}

// tryCandidateNodesForReserve 尝试在候选节点上执行 Reserve 和 Permit 操作
// 返回是否成功、成功时的节点名称、候选节点索引、Permit状态（如果返回Wait）
func (sched *Scheduler) tryCandidateNodesForReserve(
	ctx context.Context,
	fwk framework.Framework,
	state *framework.CycleState,
	assumedPod *v1.Pod,
	podInfo *framework.QueuedPodInfo,
	candidateNodes []CandidateNode,
	startTime time.Time,
) (bool, string, int, *framework.Status) {
	i := 1
	for _, candidate := range candidateNodes {
		i++
		candidateNode := candidate.Name
		klog.V(4).InfoS("Trying to reserve on candidate node",
			"pod", klog.KObj(assumedPod),
			"candidateNode", candidateNode)

		// 更新 Pod 的 NodeName
		assumedPod.Spec.NodeName = candidateNode

		// 尝试 Assume Pod 到候选节点
		if assumeErr := sched.Cache.AssumePod(assumedPod); assumeErr != nil {
			klog.V(4).InfoS("AssumePod failed for candidate node",
				"pod", klog.KObj(assumedPod),
				"node", candidateNode,
				"error", assumeErr)
			sched.nodeHistoryManager.RecordConflict(candidateNode)
			continue
		}

		// 尝试 Reserve
		if sts := fwk.RunReservePluginsReserve(ctx, state, assumedPod, candidateNode); !sts.IsSuccess() {
			// Reserve 失败，清理状态
			fwk.RunReservePluginsUnreserve(ctx, state, assumedPod, candidateNode)
			if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
				klog.ErrorS(forgetErr, "Scheduler cache ForgetPod failed")
			}
			sched.nodeHistoryManager.RecordConflict(candidateNode)
			klog.V(4).InfoS("Reserve failed for candidate node",
				"pod", klog.KObj(assumedPod),
				"node", candidateNode)
			continue
		}

		// Reserve 成功，重新执行 Permit 插件
		permitStatus, needWait := sched.reexecutePermitForCandidate(ctx, fwk, state, assumedPod, candidateNode)
		if permitStatus != nil && !permitStatus.IsSuccess() && permitStatus.Code() != framework.Wait {
			// Permit 失败或拒绝，清理状态
			fwk.RunReservePluginsUnreserve(ctx, state, assumedPod, candidateNode)
			if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
				klog.ErrorS(forgetErr, "Scheduler cache ForgetPod failed")
			}
			sched.nodeHistoryManager.RecordConflict(candidateNode)
			klog.V(4).InfoS("Permit failed for candidate node",
				"pod", klog.KObj(assumedPod),
				"node", candidateNode,
				"status", permitStatus)
			continue
		}

		// Reserve 和 Permit 都成功（Permit 可能返回 Wait）
		klog.V(4).InfoS("Successfully reserved on candidate node",
			"pod", klog.KObj(assumedPod),
			"node", candidateNode,
			"permitWait", needWait)
		return true, candidateNode, i, permitStatus
	}

	// 所有候选节点都失败
	return false, "", i, nil
}

// tryCandidateNodesForPreBind 尝试在候选节点上执行 Reserve、Permit、PreBind 和 Bind 操作
// 返回是否成功以及成功时的节点名称
func (sched *Scheduler) tryCandidateNodesForPreBind(
	ctx context.Context,
	fwk framework.Framework,
	state *framework.CycleState,
	assumedPod *v1.Pod,
	podInfo *framework.QueuedPodInfo,
	candidateNodes []CandidateNode,
	startTime time.Time,
) (bool, string, int) {
	i := 1
	for _, candidate := range candidateNodes {
		i++
		candidateNode := candidate.Name
		klog.V(4).InfoS("Trying to prebind on candidate node",
			"pod", klog.KObj(assumedPod),
			"candidateNode", candidateNode)

		// 更新 Pod 的 NodeName
		assumedPod.Spec.NodeName = candidateNode

		// 尝试 Assume Pod 到候选节点
		if assumeErr := sched.Cache.AssumePod(assumedPod); assumeErr != nil {
			klog.V(4).InfoS("AssumePod failed for candidate node",
				"pod", klog.KObj(assumedPod),
				"node", candidateNode,
				"error", assumeErr)
			sched.nodeHistoryManager.RecordConflict(candidateNode)
			continue
		}

		// 尝试 Reserve
		if sts := fwk.RunReservePluginsReserve(ctx, state, assumedPod, candidateNode); !sts.IsSuccess() {
			fwk.RunReservePluginsUnreserve(ctx, state, assumedPod, candidateNode)
			if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
				klog.ErrorS(forgetErr, "Scheduler cache ForgetPod failed")
			}
			sched.nodeHistoryManager.RecordConflict(candidateNode)
			continue
		}

		// 重新执行 Permit 插件（因为节点变了）
		permitStatus, needWait := sched.reexecutePermitForCandidate(ctx, fwk, state, assumedPod, candidateNode)
		if permitStatus != nil && !permitStatus.IsSuccess() && permitStatus.Code() != framework.Wait {
			// Permit 失败或拒绝，清理状态
			fwk.RunReservePluginsUnreserve(ctx, state, assumedPod, candidateNode)
			if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
				klog.ErrorS(forgetErr, "Scheduler cache ForgetPod failed")
			}
			sched.nodeHistoryManager.RecordConflict(candidateNode)
			continue
		}

		// 如果 Permit 返回 Wait，需要等待
		if needWait {
			waitOnPermitStatus := fwk.WaitOnPermit(ctx, assumedPod)
			if !waitOnPermitStatus.IsSuccess() {
				// WaitOnPermit 失败，清理状态
				fwk.RunReservePluginsUnreserve(ctx, state, assumedPod, candidateNode)
				if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
					klog.ErrorS(forgetErr, "Scheduler cache ForgetPod failed")
				}
				sched.nodeHistoryManager.RecordConflict(candidateNode)
				continue
			}
		}

		// 尝试 PreBind
		if preBindStatus := fwk.RunPreBindPlugins(ctx, state, assumedPod, candidateNode); !preBindStatus.IsSuccess() {
			fwk.RunReservePluginsUnreserve(ctx, state, assumedPod, candidateNode)
			if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
				klog.ErrorS(forgetErr, "Scheduler cache ForgetPod failed")
			}
			sched.nodeHistoryManager.RecordConflict(candidateNode)
			continue
		}

		// 尝试 Bind
		if bindErr := sched.bind(ctx, fwk, assumedPod, candidateNode, state); bindErr != nil {
			fwk.RunReservePluginsUnreserve(ctx, state, assumedPod, candidateNode)
			if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
				klog.ErrorS(forgetErr, "Scheduler cache ForgetPod failed")
			}
			sched.nodeHistoryManager.RecordConflict(candidateNode)
			continue
		}

		// 绑定成功
		klog.V(2).InfoS("Successfully bound to candidate node",
			"pod", klog.KObj(assumedPod),
			"node", candidateNode)
		sched.nodeHistoryManager.RecordSuccess(candidateNode)
		metrics.PodScheduled(fwk.ProfileName(), metrics.SinceInSeconds(startTime))
		metrics.PodSchedulingAttempts.Observe(float64(podInfo.Attempts))
		metrics.PodSchedulingDuration.WithLabelValues(getAttemptsLabel(podInfo)).Observe(metrics.SinceInSeconds(podInfo.InitialAttemptTimestamp))
		fwk.RunPostBindPlugins(ctx, state, assumedPod, candidateNode)
		return true, candidateNode, i
	}

	// 所有候选节点都失败
	return false, "", i
}

// selectNodesByCumulativeProbability 根据累积概率选择节点
// 逻辑：
// - 第一个节点的采纳率为 p1，有 p1 的概率直接 reserve 第一个节点
// - 如果第一个节点没有被选中（概率 1-p1），第二个节点的采纳率为 p2，有 (1-p1)*p2 的概率 reserve 第二个节点
// - 如果前两个都没被选中，第三个节点的采纳率为 p3，有 (1-p1)*(1-p2)*p3 的概率 reserve 第三个节点
// - 最多返回 maxCandidates 个候选节点（包括主节点）
// 如果所有节点都没有被选中，则fallback到第一个节点
func (sched *Scheduler) selectNodesByCumulativeProbability(
	candidateNodes []CandidateNode,
	maxCandidates int,
	pod *v1.Pod,
) []CandidateNode {
	if len(candidateNodes) == 0 {
		return candidateNodes
	}

	// 限制候选节点数量
	if len(candidateNodes) > maxCandidates {
		candidateNodes = candidateNodes[:maxCandidates]
	}

	// 生成一个随机数用于选择
	randomValue := rand.Float64()
	klog.V(5).InfoS("Selecting nodes by cumulative probability",
		"pod", klog.KObj(pod),
		"randomValue", randomValue,
		"candidateCount", len(candidateNodes))

	// 计算累积概率并选择节点
	var selectedNodes []CandidateNode
	cumulativeProb := 0.0
	selectedIndex := -1

	for i := 0; i < len(candidateNodes) && i < maxCandidates; i++ {
		p := candidateNodes[i].AdoptionProbability

		// 计算当前节点的累积概率
		// 节点1: p1
		// 节点2: p1 + (1-p1)*p2
		// 节点3: p1 + (1-p1)*p2 + (1-p1)*(1-p2)*p3
		if i == 0 {
			cumulativeProb = p
		} else {
			// 计算前面所有节点都不被选中的概率
			prevNotSelectedProb := 1.0
			for j := 0; j < i; j++ {
				prevNotSelectedProb *= (1.0 - candidateNodes[j].AdoptionProbability)
			}
			cumulativeProb += prevNotSelectedProb * p
		}

		// 检查随机数是否落在当前节点的概率区间内
		if randomValue < cumulativeProb {
			selectedIndex = i
			klog.V(4).InfoS("Node selected by cumulative probability",
				"pod", klog.KObj(pod),
				"node", candidateNodes[i].Name,
				"adoptionProbability", p,
				"cumulativeProbability", cumulativeProb,
				"randomValue", randomValue,
				"index", i)
			break
		}

		klog.V(5).InfoS("Node not selected in cumulative probability check",
			"pod", klog.KObj(pod),
			"node", candidateNodes[i].Name,
			"adoptionProbability", p,
			"cumulativeProbability", cumulativeProb,
			"randomValue", randomValue,
			"index", i)
	}

	// 如果没有节点被选中，fallback到第一个节点
	if selectedIndex == -1 {
		selectedIndex = 0
		klog.V(4).InfoS("No node selected by cumulative probability, falling back to first node",
			"pod", klog.KObj(pod),
			"fallbackNode", candidateNodes[0].Name,
			"randomValue", randomValue,
			"cumulativeProbability", cumulativeProb)
	}

	// 构建返回的候选节点列表
	// 主节点是选中的节点，然后按顺序添加其他节点（最多 maxCandidates 个）
	selectedNodes = make([]CandidateNode, 0, maxCandidates)

	// 首先添加选中的节点作为主节点
	selectedNodes = append(selectedNodes, candidateNodes[selectedIndex])

	// 然后按顺序添加其他节点作为备选（不包括已选中的节点）
	for i := 0; i < len(candidateNodes) && len(selectedNodes) < maxCandidates; i++ {
		if i != selectedIndex {
			selectedNodes = append(selectedNodes, candidateNodes[i])
		}
	}

	return selectedNodes
}

// shouldReserveSecondaryNode 判断是否应该同时为次优节点预留资源
// 返回：是否应该为次优节点预留
// 逻辑：
// - 首选节点的采纳概率为 p1，次优节点的采纳概率为 p2
// - 以 (1-p1)*p2 的概率为次优节点预留资源
// - 这样，次优节点被预留的概率是 (1-p1)*p2
func (sched *Scheduler) shouldReserveSecondaryNode(
	candidateNodes []CandidateNode,
	pod *v1.Pod,
) bool {
	if len(candidateNodes) < 2 {
		return false
	}

	p1 := candidateNodes[0].AdoptionProbability
	p2 := candidateNodes[1].AdoptionProbability

	// 确保概率值在有效范围内
	if p1 < 0 {
		p1 = 0
	}
	if p1 > 1 {
		p1 = 1
	}
	if p2 < 0 {
		p2 = 0
	}
	if p2 > 1 {
		p2 = 1
	}

	// 计算次优节点预留概率：(1-p1)*p2
	secondaryReserveProb := (1.0 - p1) * p2

	// 生成随机数
	randomValue := rand.Float64()

	shouldReserve := randomValue < secondaryReserveProb

	klog.V(4).InfoS("Determining secondary node reserve by probability",
		"pod", klog.KObj(pod),
		"primaryNode", candidateNodes[0].Name,
		"primaryProb", p1,
		"secondaryNode", candidateNodes[1].Name,
		"secondaryProb", p2,
		"secondaryReserveProb", secondaryReserveProb,
		"randomValue", randomValue,
		"shouldReserve", shouldReserve)

	return shouldReserve
}

// determineReserveStrategy 根据采纳概率决定预留策略
// 返回：预留的节点名称，是否应该为次优节点预留
// 逻辑：
// - 首选节点的采纳概率为 p1，次优节点的采纳概率为 p2
// - 以 p1 的概率为首选节点预留
// - 如果不为首选节点预留（概率 1-p1），则以 p2 的概率为次优节点预留
// - 这样，次优节点被预留的概率是 (1-p1)*p2
func (sched *Scheduler) determineReserveStrategy(
	candidateNodes []CandidateNode,
	pod *v1.Pod,
) (string, bool) {
	if len(candidateNodes) < 2 {
		// 只有一个候选节点，直接为首选节点预留
		return candidateNodes[0].Name, false
	}

	p1 := candidateNodes[0].AdoptionProbability
	p2 := candidateNodes[1].AdoptionProbability

	// 确保概率值在有效范围内
	if p1 < 0 {
		p1 = 0
	}
	if p1 > 1 {
		p1 = 1
	}
	if p2 < 0 {
		p2 = 0
	}
	if p2 > 1 {
		p2 = 1
	}

	// 计算预留概率
	// 首选节点预留概率：p1
	// 次优节点预留概率：(1-p1)*p2
	primaryReserveProb := p1
	secondaryReserveProb := (1.0 - p1) * p2

	// 生成随机数
	randomValue := rand.Float64()

	klog.V(4).InfoS("Determining reserve strategy by probability",
		"pod", klog.KObj(pod),
		"primaryNode", candidateNodes[0].Name,
		"primaryProb", p1,
		"secondaryNode", candidateNodes[1].Name,
		"secondaryProb", p2,
		"primaryReserveProb", primaryReserveProb,
		"secondaryReserveProb", secondaryReserveProb,
		"randomValue", randomValue)

	if randomValue < primaryReserveProb {
		// 为首选节点预留
		klog.V(4).InfoS("Reserving primary node by probability",
			"pod", klog.KObj(pod),
			"node", candidateNodes[0].Name,
			"probability", primaryReserveProb,
			"randomValue", randomValue)
		return candidateNodes[0].Name, false
	} else if randomValue < primaryReserveProb+secondaryReserveProb {
		// 为次优节点预留
		klog.V(4).InfoS("Reserving secondary node by probability",
			"pod", klog.KObj(pod),
			"node", candidateNodes[1].Name,
			"probability", secondaryReserveProb,
			"randomValue", randomValue)
		return candidateNodes[1].Name, true
	} else {
		// Fallback：为首选节点预留
		klog.V(4).InfoS("Fallback to primary node reservation",
			"pod", klog.KObj(pod),
			"node", candidateNodes[0].Name,
			"randomValue", randomValue,
			"totalProb", primaryReserveProb+secondaryReserveProb)
		return candidateNodes[0].Name, false
	}
}

// adjustCandidateNodesOrder 调整候选节点列表的顺序
// 如果为次优节点预留，将预留的节点移到第一位
func (sched *Scheduler) adjustCandidateNodesOrder(
	candidateNodes []CandidateNode,
	reservedNode string,
) []CandidateNode {
	if len(candidateNodes) < 2 {
		return candidateNodes
	}

	// 找到预留节点在列表中的位置
	reservedIndex := -1
	for i, node := range candidateNodes {
		if node.Name == reservedNode {
			reservedIndex = i
			break
		}
	}

	if reservedIndex <= 0 {
		// 预留节点已经是第一个，或者没找到，不需要调整
		return candidateNodes
	}

	// 创建新的候选节点列表
	newCandidates := make([]CandidateNode, 0, len(candidateNodes))

	// 首先添加预留节点
	newCandidates = append(newCandidates, candidateNodes[reservedIndex])

	// 然后按顺序添加其他节点（不包括预留节点）
	for i, node := range candidateNodes {
		if i != reservedIndex {
			newCandidates = append(newCandidates, node)
		}
	}

	klog.V(4).InfoS("Adjusted candidate nodes order",
		"reservedNode", reservedNode,
		"originalCount", len(candidateNodes),
		"newCount", len(newCandidates))

	return newCandidates
}

// cancelPermitWait 取消 Pod 的 Permit Wait 状态
// 如果 Pod 正在等待 Permit，拒绝它以便切换到候选节点
func (sched *Scheduler) cancelPermitWait(fwk framework.Framework, pod *v1.Pod) {
	if fwk.GetWaitingPod(pod.UID) != nil {
		fwk.RejectWaitingPod(pod.UID)
		klog.V(4).InfoS("Cancelled permit wait for pod to switch to candidate node", "pod", klog.KObj(pod))
	}
}

// reexecutePermitForCandidate 为候选节点重新执行 Permit 插件
// 返回 Permit 状态和是否需要等待
// 如果返回 needWait=true，调用者需要在绑定阶段调用 WaitOnPermit
func (sched *Scheduler) reexecutePermitForCandidate(
	ctx context.Context,
	fwk framework.Framework,
	state *framework.CycleState,
	pod *v1.Pod,
	nodeName string,
) (*framework.Status, bool) {
	permitStatus := fwk.RunPermitPlugins(ctx, state, pod, nodeName)

	if permitStatus.Code() == framework.Wait {
		// Permit 返回 Wait，需要等待
		klog.V(4).InfoS("Permit returned Wait for candidate node",
			"pod", klog.KObj(pod),
			"node", nodeName)
		return permitStatus, true
	}

	if !permitStatus.IsSuccess() {
		// Permit 失败或拒绝
		klog.V(4).InfoS("Permit failed for candidate node",
			"pod", klog.KObj(pod),
			"node", nodeName,
			"status", permitStatus)
		return permitStatus, false
	}

	// Permit 成功
	klog.V(4).InfoS("Permit succeeded for candidate node",
		"pod", klog.KObj(pod),
		"node", nodeName)
	return nil, false
}
