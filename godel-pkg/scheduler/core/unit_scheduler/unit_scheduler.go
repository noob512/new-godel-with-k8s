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

package unitscheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/clock"
	clientset "k8s.io/client-go/kubernetes"
	corelister "k8s.io/client-go/listers/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"

	schedulingv1a1 "github.com/kubewharf/godel-scheduler-api/pkg/apis/scheduling/v1alpha1"
	godelclient "github.com/kubewharf/godel-scheduler-api/pkg/client/clientset/versioned"
	"github.com/kubewharf/godel-scheduler-api/pkg/client/listers/scheduling/v1alpha1"
	commonstore "github.com/kubewharf/godel-scheduler/pkg/common/store"
	framework "github.com/kubewharf/godel-scheduler/pkg/framework/api"
	"github.com/kubewharf/godel-scheduler/pkg/framework/utils"
	"github.com/kubewharf/godel-scheduler/pkg/scheduler/cache"
	"github.com/kubewharf/godel-scheduler/pkg/scheduler/core"
	schedulerframework "github.com/kubewharf/godel-scheduler/pkg/scheduler/framework"
	"github.com/kubewharf/godel-scheduler/pkg/scheduler/framework/handle"
	"github.com/kubewharf/godel-scheduler/pkg/scheduler/framework/runtime"
	unitruntime "github.com/kubewharf/godel-scheduler/pkg/scheduler/framework/unit_runtime"
	"github.com/kubewharf/godel-scheduler/pkg/scheduler/metrics"
	schedulingqueue "github.com/kubewharf/godel-scheduler/pkg/scheduler/queue"
	"github.com/kubewharf/godel-scheduler/pkg/scheduler/reconciler"
	"github.com/kubewharf/godel-scheduler/pkg/util"
	"github.com/kubewharf/godel-scheduler/pkg/util/helper"
	"github.com/kubewharf/godel-scheduler/pkg/util/interpretabity"
	"github.com/kubewharf/godel-scheduler/pkg/util/parallelize"
	podutil "github.com/kubewharf/godel-scheduler/pkg/util/pod"
	"github.com/kubewharf/godel-scheduler/pkg/util/tracing"
	unitstatus "github.com/kubewharf/godel-scheduler/pkg/util/unitstatus"
)

const FailToScheduleUnit = "FailToScheduleUnit"

// ------------------------------------------------------------------------------------------

// unitScheduler is the component managing cache such as node and pod info, and other configs sharing the same life cycle with scheduler
type unitScheduler struct {
	schedulerName     string
	switchType        framework.SwitchType
	subCluster        string
	disablePreemption bool

	client    clientset.Interface
	crdClient godelclient.Interface

	podLister corelisters.PodLister
	pvcLister corelisters.PersistentVolumeClaimLister
	pgLister  v1alpha1.PodGroupLister

	Cache      cache.SchedulerCache
	Snapshot   *cache.Snapshot
	Queue      schedulingqueue.SchedulingQueue
	Scheduler  core.PodScheduler
	Reconciler *reconciler.FailedTaskReconciler

	nextUnit func() *framework.QueuedUnitInfo

	PluginRegistry framework.PluginMap
	PluginOrder    framework.PluginOrder

	Recorder events.EventRecorder
	// TODO: following fields useless for now
	MetricsRecorder         *runtime.MetricsRecorder
	Clock                   clock.Clock
	LatestScheduleTimestamp time.Time

	// Misc...
	MaxWaitingDeletionDuration time.Duration
}

var (
	_ core.UnitScheduler         = &unitScheduler{}
	_ core.SchedulerHooks        = &unitScheduler{}
	_ handle.UnitFrameworkHandle = &unitScheduler{}
)

func NewUnitScheduler(
	// basic infos...
	schedulerName string,
	switchType framework.SwitchType,
	subCluster string,
	disablePreemption bool,
	// clients...
	client clientset.Interface,
	crdClient godelclient.Interface,
	// listers...
	podLister corelisters.PodLister,
	pvcLister corelisters.PersistentVolumeClaimLister,
	pgLister v1alpha1.PodGroupLister,
	// components...
	cache cache.SchedulerCache,
	snapshot *cache.Snapshot,
	queue schedulingqueue.SchedulingQueue,
	reconciler *reconciler.FailedTaskReconciler,
	podScheduler core.PodScheduler,
	clock clock.Clock,
	recorder events.EventRecorder,
	// misc...
	maxWaitingDeletionDuration time.Duration,
) core.UnitScheduler {
	gs := &unitScheduler{
		schedulerName:     schedulerName,
		switchType:        switchType,
		subCluster:        subCluster,
		disablePreemption: disablePreemption,

		client:    client,
		crdClient: crdClient,

		podLister: podLister,
		pvcLister: pvcLister,
		pgLister:  pgLister,

		Cache:      cache,
		Snapshot:   snapshot,
		Queue:      queue,
		Scheduler:  podScheduler,
		Reconciler: reconciler,

		nextUnit: schedulingqueue.MakeNextUnitFunc(queue),

		Recorder:                recorder,
		MetricsRecorder:         runtime.NewMetricsRecorder(1000, time.Second, switchType, subCluster, schedulerName),
		Clock:                   clock,
		LatestScheduleTimestamp: clock.Now(),

		MaxWaitingDeletionDuration: maxWaitingDeletionDuration,
	}

	gs.PluginRegistry = schedulerframework.NewUnitPluginsRegistry(schedulerframework.NewUnitInTreeRegistry(), nil, gs)
	gs.PluginOrder = schedulerframework.NewOrderedUnitPluginRegistry()

	return gs
}

// --------------------------------------------------- SchedulerHooks ---------------------------------------------------

func (gs *unitScheduler) PodScheduler() core.PodScheduler {
	return gs.Scheduler
}

func (gs *unitScheduler) EventRecorder() events.EventRecorder {
	return gs.Recorder
}

func (gs *unitScheduler) BootstrapSchedulePod(ctx context.Context, pod *v1.Pod, podTrace tracing.SchedulingTrace, nodeGroup string) (string, framework.SchedulerFramework, framework.SchedulerPreemptionFramework, *framework.CycleState, error) {
	godelScheduler, switchType, subCluster := gs.Scheduler, gs.switchType, gs.subCluster

	if err := podPassesBasicChecks(pod, gs.pvcLister); err != nil {
		return "", nil, nil, nil, err
	}

	fwk, err := godelScheduler.GetFrameworkForPod(pod)
	if err != nil {
		// This shouldn't happen, because we only schedule pods having annotations set correctly.
		// API server is supposed to do the validation for annotation.
		klog.ErrorS(err, "Failed to get framework for pod", "switchType", switchType, "subCluster", subCluster, "pod", klog.KObj(pod))
		return "", nil, nil, nil, err
	}
	godelScheduler.SetFrameworkForPod(fwk)
	klog.V(4).InfoS("Generate ScheduleFramework for pod", "pod", podutil.GetPodKey(pod), "framework", fwk.ListPlugins())

	// init cycle state
	state, err := fwk.InitCycleState(pod)
	if err != nil {
		// This shouldn't happen, because we only schedule pods having annotations set
		klog.ErrorS(err, "Failed to initialize cycle state", "switchType", switchType, "subCluster", subCluster, "pod", klog.KObj(pod))
		return "", nil, nil, nil, err
	}
	state.SetRecordPluginMetrics(true)

	if err = framework.SetPodTrace(podTrace, state); err != nil {
		klog.ErrorS(err, "Fail to set pod tracing context map", "switchType", switchType, "subCluster", subCluster, "pod", podutil.GetPodKey(pod))
		return "", nil, nil, nil, err
	}

	if err = framework.SetNodeGroupKeyState(nodeGroup, state); err != nil {
		klog.ErrorS(err, "Failed to set node group", "switchType", switchType, "subCluster", subCluster, "pod", klog.KObj(pod), "nodeGroup", nodeGroup)
		return "", nil, nil, nil, err
	}

	// set unitProperty to CycleState
	framework.SetPodProperty(framework.ExtractPodProperty(pod), state)

	pfwk := godelScheduler.GetPreemptionFrameworkForPod(pod)
	godelScheduler.SetPreemptionFrameworkForPod(pfwk)

	return podutil.GetPodKey(pod), fwk, pfwk, state, nil
}

func (gs *unitScheduler) ReservePod(ctx context.Context, clonedPod *v1.Pod, scheduleResult core.PodScheduleResult) (string, error) {
	// update pod state
	clonedPod.Annotations[podutil.PodStateAnnotationKey] = string(podutil.PodAssumed)
	// update pod assumed node or nominated node
	targetNode := ""
	if scheduleResult.SuggestedHost != "" {
		// update assumed pod info in cache
		clonedPod.Annotations[podutil.AssumedNodeAnnotationKey] = scheduleResult.SuggestedHost
		targetNode = scheduleResult.SuggestedHost
	} else if scheduleResult.NominatedNode != nil {
		if len(scheduleResult.NominatedNode.VictimPods) == 0 {
			clonedPod.Annotations[podutil.AssumedNodeAnnotationKey] = scheduleResult.NominatedNode.NodeName
		} else {
			if err := utils.SetPodNominatedNode(clonedPod, scheduleResult.NominatedNode); err != nil {
				parsingErr := fmt.Errorf("error updating pod %s/%s with nominated node: %v", clonedPod.Namespace, clonedPod.Name, err)
				return targetNode, parsingErr
			}
		}
		targetNode = scheduleResult.NominatedNode.NodeName
	}

	cachePodInfo := framework.MakeCachePodInfoWrapper().Pod(clonedPod.DeepCopy()).Victims(scheduleResult.Victims).Obj()
	if err := gs.Snapshot.AssumePod(cachePodInfo); err != nil {
		klog.ErrorS(err, "Failed to assume pod in scheduler snapshot", "pod", klog.KObj(clonedPod), "nodeName", utils.GetNodeNameFromPod(clonedPod))
		return targetNode, err
	}

	return targetNode, nil
}

// --------------------------------------------------- UnitFrameworkHandle ---------------------------------------------------

func (gs *unitScheduler) SchedulerName() string {
	return gs.schedulerName
}

func (gs *unitScheduler) SwitchType() framework.SwitchType {
	return gs.switchType
}

func (gs *unitScheduler) SubCluster() string {
	return gs.subCluster
}

func (gs *unitScheduler) GetUnitStatus(unitKey string) unitstatus.UnitStatus {
	return gs.Cache.GetUnitStatus(unitKey)
}

func (gs *unitScheduler) IsCachedPod(pod *v1.Pod) (bool, error) {
	return gs.Cache.IsCachedPod(pod)
}

func (gs *unitScheduler) GetNodeInfo(nodeName string) framework.NodeInfo {
	return gs.Snapshot.GetNodeInfo(nodeName)
}

func (gs *unitScheduler) FindStore(storeName commonstore.StoreName) commonstore.Store {
	return gs.Snapshot.FindStore(storeName)
}

func (gs *unitScheduler) IsAssumedPod(pod *v1.Pod) (bool, error) {
	return gs.Cache.IsAssumedPod(pod)
}

func (gs *unitScheduler) PvcLister() corelister.PersistentVolumeClaimLister {
	return gs.pvcLister
}

// --------------------------------------------------- UnitScheduler ---------------------------------------------------

func (gs *unitScheduler) CanBeRecycle() bool {
	return gs.LatestScheduleTimestamp.Add(framework.RecycleExpiration).Before(gs.Clock.Now())
}

func (gs *unitScheduler) Close() {
	gs.MetricsRecorder.Close()
	gs.PodScheduler().Close()
}

// Schedule 执行一个调度单元（Unit）的完整调度流程。
// 它从队列中取出下一个待调度单元，构建调度上下文，运行定位（Locating）和分组（Grouping）插件，
// 然后在每个候选节点组中尝试调度，若成功则应用到缓存并持久化结果。
// 该方法由调度器工作流驱动，是 Godel 调度器的核心调度入口。
func (gs *unitScheduler) Schedule(ctx context.Context) {
	// 记录本次调度开始时间，用于性能分析或指标采集。
	gs.LatestScheduleTimestamp = gs.Clock.Now()

	// 快照当前调度上下文关键信息，避免在长流程中因状态变更导致不一致。
	snapshot, switchType, subCluster := gs.Snapshot, gs.switchType, gs.subCluster

	// 从调度队列中获取下一个待处理的调度单元（QueuedUnitInfo）。
	queuedUnitInfo := gs.nextUnit()
	if inValidUnit(queuedUnitInfo) {
		// 若单元无效（如 nil 或包含无效 Pod），记录日志并上报失败结果，且不重新入队。
		klog.InfoS("无效pod因为Empty unit or invalid queued pod info, ignore this unit and don't re-enqueue", "unit", queuedUnitInfo)
		gs.recordUnitSchedulingResults(queuedUnitInfo, false, "InvalidUnit", core.ReturnAction, "Empty unit or invalid queued pod info, ignore this unit and don't re-enqueue")
		return
	}

	// 记录调试日志：开始尝试调度该单元。
	klog.InfoS("尝试调度单元",
		"switchType", switchType,
		"subCluster", subCluster,
		"unitKey", queuedUnitInfo.UnitKey)

	// Step 1: 构建完整的调度单元信息（SchedulingUnitInfo），包含 Pod 列表、依赖、QoS 等。
	unitInfo, err := gs.constructSchedulingUnitInfo(ctx, queuedUnitInfo)
	if err != nil {
		klog.InfoS("Failed to construct scheduling unit info", "switchType", switchType, "subCluster", subCluster, "unitKey", queuedUnitInfo.UnitKey, "err", err)
		gs.recordUnitSchedulingResults(queuedUnitInfo, false, "FailToConstructUnitInfo", core.ReturnAction, helper.TruncateMessage(err.Error()))
		gs.handleSchedulingUnitFailure(ctx, core.NewUnitResult(false, 0), unitInfo, err, "FailToConstructUnitInfo")
		return
	}

	// Step 2: 更新调度器缓存的集群快照（Snapshot），确保调度基于最新集群状态。
	if err = gs.Cache.UpdateSnapshot(snapshot); err != nil {
		klog.InfoS("Failed to update snapshot", "switchType", switchType, "subCluster", subCluster, "unitKey", unitInfo.UnitKey, "err", err)
		gs.recordUnitSchedulingResults(queuedUnitInfo, false, "FailToUpdateSnapshot", core.ReturnAction, helper.TruncateMessage(err.Error()))
		gs.handleSchedulingUnitFailure(ctx, core.NewUnitResult(false, 0), unitInfo, err, "FailToUpdateSnapshot")
		return
	}

	// Step 3: 创建调度框架实例，用于插件化调度流程。
	unitFramework := unitruntime.NewUnitFramework(gs, gs, gs.PluginRegistry, gs.PluginOrder, unitInfo.QueuedUnitInfo)

	// Step 4: 运行「定位插件」（Locating Plugins），筛选出初步可行的节点集合（nodeGroup）。
	nodeGroup, status := unitFramework.RunLocatingPlugins(ctx, unitInfo.QueuedUnitInfo, unitInfo.UnitCycleState, snapshot.MakeBasicNodeGroup())
	if !status.IsSuccess() {
		klog.InfoS("Failed to run locating plugins", "switchType", switchType, "subCluster", subCluster, "unitKey", unitInfo.UnitKey, "status", status)
		gs.recordUnitSchedulingResults(queuedUnitInfo, false, "FailToLocating", core.ReturnAction, helper.TruncateMessage(status.AsError().Error()))
		gs.handleSchedulingUnitFailure(ctx, core.NewUnitResult(false, 0), unitInfo, err, "FailToLocating")
		return
	}

	// Step 5: 运行「分组插件」（Grouping Plugin），将初步节点组进一步划分为多个候选节点组（如按故障域、资源池等）。
	nodeGroups, status := unitFramework.RunGroupingPlugin(ctx, unitInfo.QueuedUnitInfo, unitInfo.UnitCycleState, nodeGroup)
	if !status.IsSuccess() {
		klog.InfoS("Failed to run grouping plugin", "switchType", switchType, "subCluster", subCluster, "unitKey", unitInfo.UnitKey, "status", status)
		gs.recordUnitSchedulingResults(queuedUnitInfo, false, "FailToGrouping", core.ReturnAction, helper.TruncateMessage(status.AsError().Error()))
		gs.handleSchedulingUnitFailure(ctx, core.NewUnitResult(false, 0), unitInfo, err, "FailToGrouping")
		return
	}

	// 准备通用日志与结果对象
	unitMessage := fmt.Sprintf("unit key=%v, ever scheduled=%v, allMember=%d, minMember=%d",
		unitInfo.UnitKey, unitInfo.EverScheduled, unitInfo.AllMember, unitInfo.MinMember)
	finalUnitResult := core.NewUnitResult(false, unitInfo.AllMember)

	// Step 6: 遍历每个候选节点组，尝试调度该单元
	for _, nodeGroup := range nodeGroups {
		nodeGroupName := nodeGroup.GetKey()
		klog.V(4).InfoS("Attempting to schedule unit in this node group",
			"switchType", switchType,
			"subCluster", subCluster,
			"unitKey", unitInfo.UnitKey,
			"nodeGroup", nodeGroupName)

		// 启动分布式追踪 Span
		unitInfo.StartUnitTraceContext(tracing.RootSpan, tracing.SchedulerScheduleSpan, tracing.WithEverScheduledTag(unitInfo.EverScheduled))
		unitInfo.SetUnitTraceContextFields(tracing.SchedulerScheduleSpan, tracing.WithNodeGroupField(nodeGroupName))

		// 在当前节点组中执行实际调度（包括预选、优选、打分、绑定模拟等）
		unitResult := gs.scheduleUnitInNodeGroup(ctx, unitInfo, unitFramework, nodeGroup)

		// 判断调度是否成功：
		// - 若是重调度（EverScheduled），只要有部分 Pod 成功即可；
		// - 否则需满足最小成员数（MinMember）要求。
		scheduleSucceed := (unitInfo.EverScheduled && len(unitResult.SuccessfulPods) > 0) ||
			len(unitResult.SuccessfulPods) >= unitInfo.MinMember

		if scheduleSucceed && gs.applyToCache(ctx, unitInfo, unitResult) {
			// 调度成功且成功应用到缓存
			msg := "Schedule unit succeeded both for snapshot and cache"
			klog.V(4).InfoS(msg, "switchType", switchType, "subCluster", subCluster, "unitKey", unitInfo.UnitKey, "nodeGroup", nodeGroupName)

			// 更新追踪信息
			unitInfo.SetUnitTraceContextTags(tracing.SchedulerScheduleSpan, tracing.WithResultTag(tracing.ResultSuccess))
			unitInfo.SetUnitTraceContextFields(tracing.SchedulerScheduleSpan, tracing.WithMessageField(msg))

			// 标记结果为成功，避免后续清理已调度 Pod
			unitResult.Successfully = true
			metrics.UnitScheduleResultObserve(unitInfo.QueuedUnitInfo.GetUnitProperty(), metrics.UnitScheduleSucceed, float64(unitInfo.MinMember))
		} else {
			// 调度失败：回滚状态，清理已成功调度的 Pod（un-reserve）
			gs.resetRunningUnitInfo(ctx, unitInfo, unitResult, nodeGroupName)

			msg := "Failed to schedule unit in this node group"
			klog.InfoS(msg,
				"switchType", switchType,
				"subCluster", subCluster,
				"nodeGroup", nodeGroupName,
				"scheduleSucceedInSnapshot", scheduleSucceed,
				"unitMessage", unitMessage,
				"failureMessage", unitResult.Details.FailureMessage())

			// 更新追踪信息
			unitInfo.SetUnitTraceContextTags(tracing.SchedulerScheduleSpan, tracing.WithResultTag(tracing.ResultFailure))
			unitInfo.SetUnitTraceContextFields(tracing.SchedulerScheduleSpan,
				tracing.WithMessageField(msg),
				tracing.WithMessageField(unitResult.Details.FailureMessage()))

			// 调试模式：输出详细调度尝试信息（用于问题排查）
			// if queuedUnitInfo.IsDebugModeOn() {
			// 	klog.V(4).InfoS("DEBUG: unit can not be scheduled in this node group", ...)
			// 	for _, podKey := range unitResult.SuccessfulPods {
			// 		klog.V(4).InfoS("DEBUG: this pod can be scheduled in this attempt", ...)
			// 	}
			// }

			// 记录指标：区分是“调度失败”还是“调度成功但应用缓存失败”
			failedReason := metrics.UnitScheduleFailed
			if scheduleSucceed {
				failedReason = metrics.UnitApplyToCacheFailed
			}
			metrics.UnitScheduleResultObserve(unitInfo.QueuedUnitInfo.GetUnitProperty(), failedReason, float64(unitInfo.MinMember))
		}

		// 结束当前节点组的追踪 Span
		unitInfo.FinishUnitTraceContext(tracing.SchedulerScheduleSpan)

		// 保留成功 Pod 最多的调度结果（用于最终上报）
		if len(unitResult.SuccessfulPods) >= len(finalUnitResult.SuccessfulPods) {
			finalUnitResult = unitResult
		}

		// 一旦在某节点组调度成功，立即退出循环（不再尝试其他组）
		if unitResult.Successfully {
			break
		}
	}

	// Step 7: 汇总最终结果
	errMessage := fmt.Sprintf("Failed to schedule unit. unit message:%v; failure message:%v", unitMessage, finalUnitResult.Details.FailureMessage())

	if !finalUnitResult.Successfully {
		// 完全失败：记录结果、更新调度单元状态、触发失败处理逻辑（如重入队、退避）
		gs.recordUnitSchedulingResults(queuedUnitInfo, false, FailToScheduleUnit, core.ReturnAction, helper.TruncateMessage(errMessage))
		klog.V(4).InfoS(errMessage)

		if err := gs.updateFailedScheduleUnit(unitInfo.QueuedUnitInfo.ScheduleUnit, finalUnitResult.Details); err != nil {
			klog.InfoS("Failed to update schedule unit", "switchType", switchType, "subCluster", subCluster, "unitKey", unitInfo.UnitKey, "message", finalUnitResult.Details.FailureMessage())
		}

		gs.handleSchedulingUnitFailure(ctx, finalUnitResult, unitInfo, errors.New(errMessage), "SchedulingFailed")
		return
	}

	// 调度成功（可能部分成功）：记录成功结果
	message := fmt.Sprintf("Schedule unit successfully. unit message: %v; successful pods:%d, failed pods:%d",
		unitMessage, len(finalUnitResult.SuccessfulPods), len(finalUnitResult.FailedPods))
	klog.V(4).InfoS("Scheduled unit successfully", "unitKey", unitInfo.UnitKey, "numSuccessfulPods", len(finalUnitResult.SuccessfulPods), "numFailedPods", len(finalUnitResult.FailedPods))
	gs.recordUnitSchedulingResults(queuedUnitInfo, true, "ScheduleUnitSuccessfully", core.ContinueAction, helper.TruncateMessage(message))

	// 注意：即使部分失败，仍调用 handleSchedulingUnitFailure 以处理未调度成功的 Pod
	// （该方法会根据 result 决定是否重入队）
	gs.handleSchedulingUnitFailure(ctx, finalUnitResult, unitInfo, errors.New(errMessage), "SchedulingFailed")

	// 异步持久化成功调度的 Pod（如更新 API Server 状态）
	go gs.PersistSuccessfulPods(ctx, finalUnitResult, unitInfo)
}

func (gs *unitScheduler) constructSchedulingUnitInfo(ctx context.Context, queuedUnitInfo *framework.QueuedUnitInfo) (*core.SchedulingUnitInfo, error) {
	unitInfo := &core.SchedulingUnitInfo{
		UnitKey:        queuedUnitInfo.UnitKey,
		QueuedUnitInfo: queuedUnitInfo,
		UnitCycleState: framework.NewCycleState(),
	}

	unit := queuedUnitInfo.ScheduleUnit
	minMember, err := unit.GetMinMember()
	if err != nil {
		return unitInfo, err
	}
	unitInfo.MinMember = minMember
	unitInfo.EverScheduled = gs.Cache.GetUnitSchedulingStatus(unitInfo.UnitKey) == unitstatus.ScheduledStatus

	// We write this into the cycle state to avoid modifying the UnitFramework interface.
	framework.SetEverScheduledState(unitInfo.EverScheduled, unitInfo.UnitCycleState)

	// run this before any potential error to make sure that all pods(queued pod info) are stored in RunningUnitInfo
	gs.constructRunningUnitInfo(ctx, unit, unitInfo)
	if !unitInfo.EverScheduled && unitInfo.MinMember > unitInfo.AllMember {
		return unitInfo, fmt.Errorf("min member is greater than all member which is unexpected")
	}

	// only when the node partition is Physical and the preemption feature is disabled,
	// we will reset the selected scheduler annotation and let dispatcher re-dispatch these pods when scheduling failed
	// TODO: revisit this
	if gs.disablePreemption {
		unitInfo.DispatchToAnotherScheduler = true
	}

	return unitInfo, nil
}

func (gs *unitScheduler) constructRunningUnitInfo(ctx context.Context, unit framework.ScheduleUnit, unitInfo *core.SchedulingUnitInfo) {
	runningUnitMap := unitInfo.DispatchedPods
	if runningUnitMap == nil {
		runningUnitMap = make(map[string]*core.RunningUnitInfo)
		unitInfo.DispatchedPods = runningUnitMap
	}

	allMember := 0
	for _, podInfo := range unit.GetPods() {
		podKey := podutil.GetPodKey(podInfo.Pod)
		podProperty := podInfo.GetPodProperty()
		podTrace := tracing.NewSchedulingTrace(
			podInfo.Pod,
			podProperty.ConvertToTracingTags(),
			tracing.WithSchedulerOption(),
			tracing.WithScheduler(gs.schedulerName), // TODO: hook?
		)

		runningUnitMap[podKey] = &core.RunningUnitInfo{
			QueuedPodInfo: podInfo,
			ClonedPod:     getAndInitClonedPod(podTrace.GetRootSpanContext(), podInfo),
			Trace:         podTrace,
		}
		allMember++
	}
	unitInfo.AllMember = allMember
}

func (gs *unitScheduler) scheduleUnitInNodeGroup(ctx context.Context, unitInfo *core.SchedulingUnitInfo, unitFramework unitruntime.SchedulerUnitFramework, nodeGroup framework.NodeGroup) *core.UnitResult {
	// Reset SchedulingUnitInfo
	unitInfo.Reset()

	// Scheduling & Preempting
	unitInfo.StartUnitTraceContext(tracing.SchedulerScheduleSpan, tracing.SchedulerScheduleUnitSpan)
	scheduleResult := unitFramework.Scheduling(ctx, unitInfo, nodeGroup)

	unitInfo.SetUnitTraceContextFields(tracing.SchedulerScheduleUnitSpan, tracing.WithMessageField(scheduleResult.Marshal()))
	unitInfo.SetUnitTraceContextFields(tracing.SchedulerScheduleUnitSpan, tracing.WithErrorFields(tracing.TruncateErrors(scheduleResult.Details.GetErrors()))...)
	unitInfo.FinishUnitTraceContext(tracing.SchedulerScheduleUnitSpan)

	if gs.disablePreemption {
		return core.TransferToUnitResult(unitInfo, scheduleResult.Details, scheduleResult.SuccessfulPods, scheduleResult.FailedPods)
	}

	unitInfo.StartUnitTraceContext(tracing.SchedulerScheduleSpan, tracing.SchedulerPreemptUnitSpan)
	preemptResult := unitFramework.Preempting(ctx, unitInfo, nodeGroup)

	unitInfo.SetUnitTraceContextFields(tracing.SchedulerPreemptUnitSpan, tracing.WithMessageField(preemptResult.Marshal()))
	unitInfo.SetUnitTraceContextFields(tracing.SchedulerPreemptUnitSpan, tracing.WithErrorFields(tracing.TruncateErrors(preemptResult.Details.GetErrors()))...)
	unitInfo.FinishUnitTraceContext(tracing.SchedulerPreemptUnitSpan)
	return core.TransferToUnitResult(unitInfo, preemptResult.Details, append(scheduleResult.SuccessfulPods, preemptResult.SuccessfulPods...), preemptResult.FailedPods)
}

func (gs *unitScheduler) resetRunningUnitInfo(ctx context.Context, unitInfo *core.SchedulingUnitInfo, result *core.UnitResult, nodeGroupName string) {
	// un-reserve pods
	gs.unReservePods(ctx, unitInfo, result, nodeGroupName)

	for _, runningUnitInfo := range unitInfo.DispatchedPods {
		runningUnitInfo.Victims = nil
		runningUnitInfo.NodeToPlace = ""
		runningUnitInfo.ClonedPod = getAndInitClonedPod(runningUnitInfo.Trace.GetRootSpanContext(), runningUnitInfo.QueuedPodInfo)
	}
}

func (gs *unitScheduler) unReservePods(ctx context.Context, unitInfo *core.SchedulingUnitInfo, result *core.UnitResult, nodeGroupName string) {
	snapshot, switchType, subCluster := gs.Snapshot, gs.switchType, gs.subCluster
	for _, podKey := range result.SuccessfulPods {
		runningUnitInfo := unitInfo.DispatchedPods[podKey]
		if runningUnitInfo != nil && runningUnitInfo.ClonedPod != nil {
			cachePodInfo := framework.MakeCachePodInfoWrapper().
				Pod(runningUnitInfo.ClonedPod).
				Victims(runningUnitInfo.Victims).
				Obj()
			if err := snapshot.ForgetPod(cachePodInfo); err != nil {
				// TODO: do we need to panic here ?
				klog.InfoS("Failed to un-reserve pod", "switchType", switchType, "subCluster", subCluster, "pod", klog.KObj(unitInfo.DispatchedPods[podKey].ClonedPod),
					"unitKey", unitInfo.UnitKey, "nodeGroup", nodeGroupName, "err", err)
			}
		}
	}
}

func (gs *unitScheduler) handleSchedulingUnitFailure(ctx context.Context, result *core.UnitResult, unitInfo *core.SchedulingUnitInfo,
	err error, reason string,
) {
	queue, switchType, subCluster := gs.Queue, gs.switchType, gs.subCluster
	// 1. remove successful pods before re-enqueue
	if result.Successfully {
		for _, podKey := range result.SuccessfulPods {
			podInfo := unitInfo.DispatchedPods[podKey].QueuedPodInfo
			if podInfo != nil {
				unitInfo.QueuedUnitInfo.DeletePod(podInfo)
			}
		}
	}
	// 2. refresh the pod info
	podInfos := unitInfo.QueuedUnitInfo.GetPods()
	for i := range podInfos {
		podInfo := podInfos[i]
		cachedPod, err := gs.podLister.Pods(podInfo.Pod.Namespace).Get(podInfo.Pod.Name)
		if apierrors.IsNotFound(err) {
			klog.InfoS("WARN: failed to re-enqueue the pod cause it doesn't exist in informer cache", "pod", klog.KObj(podInfo.Pod))
			_ = unitInfo.QueuedUnitInfo.DeletePod(podInfo)
		} else if gs.skipPodSchedule(cachedPod) {
			klog.InfoS("WARN: failed to re-enqueue the pod cause it already exist in scheduler cache", "pod", klog.KObj(cachedPod))
			_ = unitInfo.QueuedUnitInfo.DeletePod(podInfo)
		} else {
			// refresh queue span
			podInfo.QueueSpan = tracing.NewSpanInfo(podInfo.GetPodProperty().ConvertToTracingTags())
			if err == nil {
				podInfo.Pod = cachedPod.DeepCopy()
			}
		}
	}

	// 3. re-enqueue
	if !unitInfo.DispatchToAnotherScheduler && unitInfo.QueuedUnitInfo.NumPods() > 0 {
		unitInfo.QueuedUnitInfo.SetEnqueuedTimeStamp(time.Now())
		reEnqueueErr := queue.AddUnschedulableIfNotPresent(unitInfo.QueuedUnitInfo, queue.SchedulingCycle())
		if reEnqueueErr != nil {
			klog.InfoS("Failed to re-enqueue the unit", "switchType", switchType, "subCluster", subCluster, "unitKey", unitInfo.UnitKey, "err", reEnqueueErr)
		}
	}
	// 4. update pod condition
	for i := range podInfos {
		// use the error from scheduling result if it exists, it's more accurate
		podError := err
		if result.Details != nil {
			if got := result.Details.GetPodError(podutil.GetPodKey(podInfos[i].Pod)); got != nil {
				podError = got
			}
		}
		if updateErr := updateFailedSchedulingPod(gs.client, gs.schedulerName, podInfos[i].Pod, !unitInfo.DispatchToAnotherScheduler, podError, reason); updateErr != nil {
			klog.InfoS("Failed to update the failed scheduling pod", "switchType", switchType, "subCluster", subCluster, "pod", klog.KObj(podInfos[i].Pod), "err", updateErr)
		}
	}
}

func (gs *unitScheduler) recordUnitSchedulingResults(unitInfo *framework.QueuedUnitInfo, successful bool, reason string, action string, message string) {
	if unitInfo == nil || unitInfo.ScheduleUnit == nil {
		return
	}
	// TODO: send warning event to unit object, e.g. PodGroup
	// send events to each pod
	var eventType string
	if successful {
		eventType = v1.EventTypeNormal
	} else {
		eventType = v1.EventTypeWarning
	}
	for _, podInfo := range unitInfo.GetPods() {
		if podInfo == nil {
			klog.ErrorS(nil, "DEBUG: got a nil PodInfo, which shouldn't happen", "unitKey", unitInfo.UnitKey, "unitPodInfos", unitInfo.ScheduleUnit)
			klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		}
		if podInfo.Pod == nil {
			continue
		}
		gs.Recorder.Eventf(podInfo.Pod, nil, eventType, reason, action, message)
	}

	if unitInfo.Type() == framework.PodGroupUnitType {
		// record event for PodGroup.
		pg, err := gs.pgLister.PodGroups(unitInfo.GetNamespace()).Get(unitInfo.GetName())
		if err != nil {
			if !apierrors.IsNotFound(err) {
				klog.InfoS("Failed to get PodGroup", "unitNamespace", unitInfo.GetNamespace(), "unitName", unitInfo.GetName(), "err", err)
			}
			// do nothing, if PodGroup is not found.
			return
		}
		gs.Recorder.Eventf(pg, nil, eventType, reason, action, helper.TruncateMessage(message))
	}
}

// updateFailedScheduleUnit reports the failure details for PodGroupUnit, by updating the condition.
func (gs *unitScheduler) updateFailedScheduleUnit(scheduleUnit framework.ScheduleUnit, failureDetails *interpretabity.UnitSchedulingDetails) error {
	// do nothing for other Unit, right now.
	if scheduleUnit.Type() != framework.PodGroupUnitType {
		return nil
	}

	pg, err := gs.pgLister.PodGroups(scheduleUnit.GetNamespace()).Get(scheduleUnit.GetName())
	if err != nil {
		return fmt.Errorf("failed to update ScheduleUnit. type:%v; key:%v/%v; err:%v", scheduleUnit.Type(), scheduleUnit.GetNamespace(), scheduleUnit.GetName(), err)
	}

	cond := schedulingv1a1.PodGroupCondition{
		Phase:              schedulingv1a1.PodGroupPreScheduling,
		Status:             v1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             FailToScheduleUnit,
		Message:            failureDetails.FailureMessage(),
	}

	return interpretabity.UpdatePreSchedulingCondition(gs.crdClient, pg, cond)
}

func (gs *unitScheduler) applyToCache(ctx context.Context, unitInfo *core.SchedulingUnitInfo, result *core.UnitResult) bool {
	cache, switchType, subCluster := gs.Cache, gs.switchType, gs.subCluster
	for i, key := range result.SuccessfulPods {
		runningPodInfo := unitInfo.DispatchedPods[key]
		cachePodInfo := framework.MakeCachePodInfoWrapper().Pod(runningPodInfo.ClonedPod).Victims(runningPodInfo.Victims).Obj()

		traceContext := runningPodInfo.Trace.NewTraceContext(tracing.SchedulerScheduleSpan, tracing.SchedulerAssumePodSpan)
		err := cache.AssumePod(cachePodInfo)
		defer tracing.AsyncFinishTraceContext(traceContext, time.Now())

		if err != nil {
			klog.ErrorS(err, "Failed to assume pod in scheduler cache, will forget the pod",
				"switchType", switchType, "subCluster", subCluster,
				"pod", klog.KObj(runningPodInfo.ClonedPod),
				"node", runningPodInfo.NodeToPlace)

			if err := cache.ForgetPod(cachePodInfo); err != nil {
				msg := "Failed to forget pod in scheduler cache after the assume pod failure occured"
				traceContext.WithFields(tracing.WithMessageField(msg))
				traceContext.WithFields(tracing.WithErrorField(err))
				traceContext.WithTags(tracing.WithResultTag(tracing.ResultFailure))
				klog.ErrorS(err, msg, "pod", klog.KObj(runningPodInfo.ClonedPod), "node", runningPodInfo.NodeToPlace)
			}

			// Considering that there may be sequential dependencies during different Pods, once an AssumePod failure occured,
			// subsequent operations will no longer continue.
			//
			// At the same time, we will decide to apply or roll back the previous operations based on whether the unit
			// scheduling conditions are met.
			if !unitInfo.EverScheduled && i < unitInfo.MinMember {
				// For min-fail, we revert the operations and return false.
				msg := "Failed to assume pod in scheduler cache and result in a min-fail, ready to revert"
				klog.InfoS(msg, "numSuccessfulPods", len(result.SuccessfulPods), "podIndex", i)
				for index := 0; index < i; index++ {
					runningPodInfo := unitInfo.DispatchedPods[result.SuccessfulPods[index]]
					previousTraceContext := runningPodInfo.Trace.GetTraceContext(tracing.SchedulerAssumePodSpan)
					if err := cache.ForgetPod(cachePodInfo); err != nil {
						msg := "Failed to forget pod in scheduler cache during revert"
						previousTraceContext.WithFields(tracing.WithMessageField(msg))
						previousTraceContext.WithFields(tracing.WithErrorField(err))
						previousTraceContext.WithTags(tracing.WithResultTag(tracing.ResultFailure))
						klog.ErrorS(err, msg,
							"pod", klog.KObj(runningPodInfo.ClonedPod),
							"node", runningPodInfo.NodeToPlace)
					}
				}

				// Remove all Pods from successfulPods, which failed to assume cache, and mark them as FailedPods.
				result.Details.AddPodsError(err, result.SuccessfulPods...)
				result.FailedPods = append(result.FailedPods, result.SuccessfulPods...)
				result.SuccessfulPods = []string{}
				return false
			} else {
				msg := "Failed to assume pod in scheduler cache but the min-member can be met or unit ever scheduled"
				traceContext.WithFields(tracing.WithMessageField(msg))
				klog.InfoS(msg, "unitKey", unitInfo.UnitKey, "numSuccessfulPods", len(result.SuccessfulPods), "podIndex", i)

				// Otherwise, move the left pods from successfulPods to failedPods and return true.
				result.Details.AddPodsError(err, result.SuccessfulPods[i:]...)
				result.FailedPods = append(result.FailedPods, result.SuccessfulPods[i:]...)
				result.SuccessfulPods = result.SuccessfulPods[:i]
				return true
			}
		} else {
			traceContext.WithTags(tracing.WithResultTag(tracing.ResultSuccess))
		}
	}
	return true
}

func (gs *unitScheduler) PersistSuccessfulPods(ctx context.Context,
	result *core.UnitResult, unitInfo *core.SchedulingUnitInfo,
) {
	var failedPods []string
	var fpMutex sync.Mutex

	cache, switchType, subCluster := gs.Cache, gs.switchType, gs.subCluster
	unitProperty := unitInfo.QueuedUnitInfo.GetUnitProperty()

	updatePod := func(i int) {
		podKey := result.SuccessfulPods[i]
		runningUnitInfo := unitInfo.DispatchedPods[podKey]

		podProperty := runningUnitInfo.QueuedPodInfo.GetPodProperty()
		metrics.SchedulerGoroutinesInc(podProperty, metrics.UpdatingPod)
		defer metrics.SchedulerGoroutinesDec(podProperty, metrics.UpdatingPod)

		podTrace := runningUnitInfo.Trace
		updatingTraceContext := podTrace.NewTraceContext(tracing.RootSpan, tracing.SchedulerUpdatingPodSpan)
		defer tracing.AsyncFinishTraceContext(updatingTraceContext, time.Now())

		// exclude pods that are not scheduled successfully within one scheduling attempt
		if unitInfo.QueuedUnitInfo.Attempts > 1 ||
			!runningUnitInfo.QueuedPodInfo.InitialPreemptAttemptTimestamp.IsZero() {
			runningUnitInfo.ClonedPod.Annotations[podutil.E2EExcludedPodAnnotationKey] = "true"
		}

		err := util.PatchPod(gs.client, runningUnitInfo.QueuedPodInfo.Pod, runningUnitInfo.ClonedPod)
		if err == nil {
			updatingTraceContext.WithTags(tracing.WithResultTag(tracing.ResultSuccess))
			if err := cache.FinishReserving(runningUnitInfo.ClonedPod); err != nil {
				klog.InfoS("Failed to finish reserving", "switchType", switchType, "subCluster", subCluster, "podKey", podKey, "unitKey", unitInfo.UnitKey, "err", err)
			}

			metrics.ObservePodSchedulingLatency(podProperty, getAttemptsLabel(runningUnitInfo.QueuedPodInfo), helper.SinceInSeconds(runningUnitInfo.QueuedPodInfo.InitialAttemptTimestamp))

			klog.V(2).InfoS("Persisted this pod successfully, and the pod now is in assumed state",
				"switchType", switchType, "subCluster", subCluster,
				"pod", klog.KObj(runningUnitInfo.ClonedPod),
				"unitKey", unitInfo.UnitKey)
			gs.Recorder.Eventf(runningUnitInfo.ClonedPod, nil, v1.EventTypeNormal,
				"PersistPodSuccessfully", core.ContinueAction, "Reserve pod successfully, and pod now is in assumed state")
		} else {
			updatingTraceContext.WithTags(tracing.WithResultTag(tracing.ResultFailure))
			updatingTraceContext.WithFields(tracing.WithErrorField(err))

			if apierrors.IsNotFound(err) {
				klog.InfoS("Failed to persist this pod because the pod has been deleted",
					"switchType", switchType, "subCluster", subCluster,
					"pod", klog.KObj(runningUnitInfo.ClonedPod),
					"unitKey", unitInfo.UnitKey, "err", err)
				// this pod is deleted, forget it from cache if it is still in assumed state in cache
				if assumed, _ := gs.Cache.IsAssumedPod(runningUnitInfo.ClonedPod); assumed {
					cachePodInfo := framework.MakeCachePodInfoWrapper().
						Pod(unitInfo.DispatchedPods[podKey].ClonedPod).
						Obj()
					if err := gs.Cache.ForgetPod(cachePodInfo); err != nil {
						klog.InfoS("Failed to forget pod when we found out that the pod had been deleted", "pod", klog.KObj(runningUnitInfo.ClonedPod), "err", err)
					}
				}
				return
			}
			// TODO: do we want to forget this pod directly
			// TODO: even not: if this pod is deleted or updated to other states, we need to try to forget it too,
			// since this pod is reserved to cache in scheduling workflow
			/*if err := gs.SchedulerCache.ForgetPod(unitInfo.DispatchedPods[podKey].ClonedPod); err != nil {
				klog.ErrorS(err, "Forget pod failed", "podKey", podKey, "unit", unitInfo.UnitKey)
				panic("reserve pod successfully but forget failed, this may cause data inconsistent, so panic...")
			}*/
			fpMutex.Lock()
			failedPods = append(failedPods, podKey)
			fpMutex.Unlock()

			klog.InfoS("Failed to persist this pod",
				"switchType", switchType, "subCluster", subCluster,
				"pod", klog.KObj(runningUnitInfo.ClonedPod),
				"unitKey", unitInfo.UnitKey, "err", err)
			gs.Recorder.Eventf(runningUnitInfo.ClonedPod, nil, v1.EventTypeWarning,
				"FailToPersistPod", core.ContinueAction, helper.TruncateMessage(err.Error()))
		}
	}

	parallelize.Until(ctx, len(result.SuccessfulPods), updatePod)
	metrics.SchedulerUnitE2ELatencyObserve(unitProperty, helper.SinceInSeconds(unitInfo.QueuedUnitInfo.InitialAttemptTimestamp))

	// if we fail to patch some of them pods, this probably may due to network problem.
	// even if we try to reset these pods (API call), error will still appear.
	// so, keep reserving them in cache and add them to failed-task-reconciler to retry periodically.
	// since these failed pods are in assumed state in cache, if they are deleted or updated, cache won't react to these events,
	// so we need to let the reconciler take this into account
	// TODO: revisit this later
	if len(failedPods) > 0 {
		klog.InfoS("Failed to patch some pods, and will add them to failed task reconciler", "switchType", switchType, "subCluster", subCluster, "failedPods", failedPods)
		for _, podKey := range failedPods {
			gs.Reconciler.AddFailedTask(reconciler.NewFailedPatchTask(framework.MakeCachePodInfoWrapper().Pod(unitInfo.DispatchedPods[podKey].ClonedPod).Obj()))
		}
	}
}

func (gs *unitScheduler) skipPodSchedule(pod *v1.Pod) bool {
	isCached, err := gs.Cache.IsCachedPod(pod)
	if err != nil {
		return false
	}
	// Since we got the Pod from informer cache, it's ok to call `podutil.AssumedPod` or `podutil.BoundPod` here.
	return isCached || podutil.AssumedPod(pod) || podutil.BoundPod(pod)

}

func (gs *unitScheduler) GetMaxWaitingDeletionDuration() time.Duration {
	return gs.MaxWaitingDeletionDuration
}
