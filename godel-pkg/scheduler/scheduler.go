/*
Copyright 2017 The Kubernetes Authors.

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
	"math/rand"
	"time"

	godelclient "github.com/kubewharf/godel-scheduler-api/pkg/client/clientset/versioned"
	crdinformers "github.com/kubewharf/godel-scheduler-api/pkg/client/informers/externalversions"
	"github.com/kubewharf/godel-scheduler-api/pkg/client/listers/scheduling/v1alpha1"
	katalystinformers "github.com/kubewharf/katalyst-api/pkg/client/informers/externalversions"
	//"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	//"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	commoncache "k8s.io/kubernetes/godel-pkg/common/cache"
	"k8s.io/kubernetes/godel-pkg/features"
	framework "k8s.io/kubernetes/godel-pkg/framework/api"
	"k8s.io/kubernetes/godel-pkg/scheduler/apis/config"
	godelcache "k8s.io/kubernetes/godel-pkg/scheduler/cache"
	//preemptionstore "k8s.io/kubernetes/godel-pkg/scheduler/cache/commonstores/preemption_store"
	cachedebugger "k8s.io/kubernetes/godel-pkg/scheduler/cache/debugger"
	"k8s.io/kubernetes/godel-pkg/scheduler/controller"
	podscheduler "k8s.io/kubernetes/godel-pkg/scheduler/core/pod_scheduler"
	unitscheduler "k8s.io/kubernetes/godel-pkg/scheduler/core/unit_scheduler"
	"k8s.io/kubernetes/godel-pkg/scheduler/metrics"
	godelqueue "k8s.io/kubernetes/godel-pkg/scheduler/queue"
	"k8s.io/kubernetes/godel-pkg/scheduler/reconciler"
	schedulerutil "k8s.io/kubernetes/godel-pkg/scheduler/util"
)

// Scheduler watches for new unscheduled pods. It attempts to find
// nodes that they fit on and writes bindings back to the api server. Scheduler is the wrapper that watches for all
// pod and node event, the actual scheduling process is handled by schedule framework.
type Scheduler struct {
	// Name identifies the Godel Scheduler
	Name string
	// SchedulerName here is the higher level scheduler name, which is used to select pods
	// that godel schedulers should be responsible for and filter out irrelevant pods.
	SchedulerName *string

	// Close this to shut down the scheduler.
	StopEverything         <-chan struct{}
	scheduledPodsHasSynced func() bool
	clock                  clock.Clock

	// client syncs K8S object
	client clientset.Interface
	// crdClient syncs custom resources
	crdClient          godelclient.Interface
	informerFactory    informers.SharedInformerFactory
	crdInformerFactory crdinformers.SharedInformerFactory
	options            schedulerOptions

	podLister corelisters.PodLister
	pgLister  v1alpha1.PodGroupLister
	pvcLister corelisters.PersistentVolumeClaimLister

	commonCache    godelcache.SchedulerCache
	ScheduleSwitch ScheduleSwitch

	mayHasPreemption        bool
	defaultSubClusterConfig *subClusterConfig

	schedulerMaintainer StatusMaintainer
	recorder            events.EventRecorder
	metricsRecorder     *godelcache.ClusterCollectable

	movementController controller.CommonController
}

// New 创建并初始化一个 Godel 调度器实例（*Scheduler）。
// 该函数负责：
//   - 注册指标
//   - 构建共享缓存（commonCache）
//   - 初始化核心依赖（client、lister、informer 等）
//   - 根据 Feature Gate 动态启用高级功能（如重调度）
//   - 按配置创建子集群调度工作流（DataSet）
//   - 注册事件处理器
//
// 参数说明：
//   - godelSchedulerName: 调度器实例的唯一标识名（如 "godel-scheduler-my-custom"）
//   - schedulerName: 指向 Kubernetes Pod.spec.schedulerName 的指针，用于筛选待调度 Pod
//   - client/crdClient: Kubernetes 和 Godel CRD 的客户端
//   - informerFactory 系列: 用于监听集群资源变更的共享 Informer 工厂
//   - stopCh: 全局停止信号通道（若为 nil，则使用永不关闭的 wait.NeverStop）
//   - recorder: 事件记录器，用于向 API Server 发送调度事件
//   - reservationTTL: 调度预占（reservation）的 TTL 时长
//   - opts: 可选配置项（如子集群配置、Profile 等）
func New(
	godelSchedulerName string,
	schedulerName *string,
	client clientset.Interface,
	crdClient godelclient.Interface,
	informerFactory informers.SharedInformerFactory,
	crdInformerFactory crdinformers.SharedInformerFactory,
	katalystCrdInformerFactory katalystinformers.SharedInformerFactory,
	stopCh <-chan struct{},
	recorder events.EventRecorder,
	reservationTTL time.Duration,
	opts ...Option,
) (*Scheduler, error) {

	// =============== 第 1 步：准备依赖项 ===============

	// 优先注册指标，避免因延迟注册导致指标丢失（参见 issue #79）
	metrics.Register(godelSchedulerName)

	// 标准化停止通道：若未提供，则使用永不关闭的通道
	stopEverything := stopCh
	if stopEverything == nil {
		stopEverything = wait.NeverStop
	}

	// 解析可选配置参数
	options := renderOptions(opts...)

	// 使用真实系统时钟
	globalClock := clock.RealClock{}

	// 从 Informer Factory 获取核心资源的 Lister 和 Informer
	podLister := informerFactory.Core().V1().Pods().Lister()
	podInformer := informerFactory.Core().V1().Pods()
	pvcLister := informerFactory.Core().V1().PersistentVolumeClaims().Lister()
	pgLister := crdInformerFactory.Scheduling().V1alpha1().PodGroups().Lister()

	// 检查是否启用了抢占（preemption）功能
	// mayHasPreemption := parseProfilesBoolConfiguration(options, profileNeedPreemption)

	// 构建通用缓存（commonCache）的初始化参数包装器
	handlerWrapper := commoncache.MakeCacheHandlerWrapper().
		ComponentName(godelSchedulerName).
		SchedulerType(*schedulerName).
		SubCluster(framework.DefaultSubCluster).
		PodAssumedTTL(15 * time.Minute). // Pod 假定（assumed）状态的 TTL
		Period(10 * time.Second).        // 缓存定期同步周期
		ReservationTTL(reservationTTL).  // 预占资源的 TTL
		StopCh(stopEverything).
		PodLister(podLister).
		PodInformer(podInformer).
		PVCLister(pvcLister)

	// 若启用了抢占，则在缓存中启用抢占状态存储
	// if mayHasPreemption {
	// 	handlerWrapper.EnableStore(string(preemptionstore.Name))
	// }

	// =============== 第 2 步：创建 Scheduler 主体 ===============

	sched := &Scheduler{
		Name:                   godelSchedulerName,
		SchedulerName:          schedulerName,
		StopEverything:         stopEverything,
		scheduledPodsHasSynced: informerFactory.Core().V1().Pods().Informer().HasSynced,
		clock:                  globalClock,
		client:                 client,
		crdClient:              crdClient,
		informerFactory:        informerFactory,
		crdInformerFactory:     crdInformerFactory,
		options:                options,

		podLister: podLister,
		pgLister:  pgLister,
		pvcLister: pvcLister,

		// 初始化通用缓存（包含 Pod、Node、Reservation 等状态）
		commonCache: godelcache.New(handlerWrapper.Obj()),

		mayHasPreemption:        false,
		defaultSubClusterConfig: newDefaultSubClusterConfig(options.defaultProfile),

		// 调度器状态维护器：定期向 API Server 更新调度器健康状态
		schedulerMaintainer: NewSchedulerStatusMaintainer(globalClock, crdClient, godelSchedulerName, options.renewInterval),
		recorder:            recorder,
		// 用于收集集群级调度指标
		metricsRecorder: godelcache.NewEmptyClusterCollectable(godelSchedulerName),
	}

	// =============== 第 3 步：按需启用重调度（Rescheduling）功能 ===============

	// 如果启用了 SupportRescheduling 特性，则初始化 MovementController
	// Movement 是 Godel 中表示“Pod 迁移/重调度请求”的 CRD
	// if utilfeature.DefaultFeatureGate.Enabled(features.SupportRescheduling) {
	// 	// 配置带退避和速率限制的工作队列
	// 	rateLimiter := workqueue.NewMaxOfRateLimiter(
	// 		workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 500*time.Second),
	// 		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
	// 	)
	// 	movementQueue := workqueue.NewNamedRateLimitingQueue(rateLimiter, "Movement")
	// 	movementLister := crdInformerFactory.Scheduling().V1alpha1().Movements().Lister()
	// 	sched.movementController = controller.NewMovementController(
	// 		movementQueue, stopEverything, movementLister, godelSchedulerName, crdClient,
	// 	)
	// }

	// =============== 第 4 步：创建子集群调度工作流（核心调度循环） ===============

	// 设置全局子集群标识键（用于从 Pod 标签提取子集群信息）
	framework.SetGlobalSubClusterKey(options.subClusterKey)
	// 清理历史子集群索引（确保干净启动）
	framework.CleanClusterIndex()

	// 初始化调度开关（ScheduleSwitch），用于管理所有 DataSet 生命周期
	sched.ScheduleSwitch = NewScheduleSwitch()

	if !utilfeature.DefaultFeatureGate.Enabled(features.SchedulerConcurrentScheduling) {
		// 模式一：无并发调度 → 创建单个全局 DataSet
		globalDataSet := sched.createDataSet(framework.DefaultSubClusterIndex, framework.DefaultSubCluster, framework.DisableScheduleSwitch)
		sched.ScheduleSwitch.Register(framework.DisableScheduleSwitch, globalDataSet)
	} else {
		// 模式二/三：启用并发调度
		subClusters := []string{framework.DefaultSubCluster}
		if utilfeature.DefaultFeatureGate.Enabled(features.SchedulerSubClusterConcurrentScheduling) {
			// 若启用子集群并发，则从配置中加载所有子集群
			for i := range options.subClusterProfiles {
				subClusters = append(subClusters, options.subClusterProfiles[i].SubClusterName)
			}
		}
		// 为每个子集群分配唯一索引，并创建对应的 GT/BE 调度工作流
		for i := range subClusters {
			subCluster := subClusters[i]
			idx := framework.AllocClusterIndex(subCluster)
			sched.createSubClusterWorkflow(idx, subCluster)
		}
	}

	// =============== 第 5 步：注册事件处理器 ===============

	// 为 Pod、Node、PodGroup、Movement 等资源注册 Add/Update/Delete 事件回调
	// 这些处理器负责将集群变更同步到调度缓存和队列中
	addAllEventHandlers(sched, informerFactory, crdInformerFactory, katalystCrdInformerFactory)

	return sched, nil
}

// Run 启动调度器的监听和调度循环。
// 它会等待本地缓存同步完成，然后开始执行调度逻辑，并一直阻塞运行直到传入的上下文（ctx）被取消。
func (sched *Scheduler) Run(ctx context.Context) {
	// 启动一个 goroutine 来维护调度器的状态信息（通常存储在 CRD 中）。
	// 这个维护器可能负责更新调度器的健康状态、活跃节点列表等信息。
	go sched.schedulerMaintainer.Run(sched.StopEverything)

	// 等待调度器内部的缓存（特别是已调度的 Pod 缓存）与 API Server 同步。
	// 如果在上下文取消之前未能同步成功，则退出 Run 函数。
	// sched.scheduledPodsHasSynced 是一个函数，用于检查缓存是否已同步。
	if !cache.WaitForCacheSync(ctx.Done(), sched.scheduledPodsHasSynced) {
		return
	}

	// 检查 "SchedulerCacheScrape" 特性门控是否启用。
	// if utilfeature.DefaultFeatureGate.Enabled(features.SchedulerCacheScrape) {
	// 	// 如果启用，则启动一个 goroutine 定期（每 5 秒）收集调度器缓存的指标数据，
	// 	// 并更新到指标记录器中。这有助于监控调度器的内部状态和性能。
	// 	go wait.Until(func() {
	// 		// 从缓存中抓取可收集的指标数据
	// 		sched.commonCache.ScrapeCollectable(sched.metricsRecorder)
	// 		// 更新指标记录器中的指标值
	// 		sched.metricsRecorder.UpdateMetrics()
	// 	}, 5*time.Second, sched.StopEverything) // sched.StopEverything 通道用于控制这个 goroutine 的停止。
	// }

	// 初始化随机数生成器种子，用于调度器中可能需要随机选择的逻辑（例如，从多个可选节点中随机选择一个）。
	rand.Seed(time.Now().Unix())

	// 检查 "SupportRescheduling" 特性门控是否启用。
	// if utilfeature.DefaultFeatureGate.Enabled(features.SupportRescheduling) {
	// 	// 如果启用，则运行调度器的移动控制器（movementController）。
	// 	// 该控制器可能负责处理需要重新调度的 Pod（例如，由于节点故障或资源不足而需要迁移的 Pod）。
	// 	sched.movementController.Run()
	// 	// 在 Run 函数退出前，确保关闭移动控制器，释放相关资源。
	// 	defer sched.movementController.Close()
	// }

	// 启动调度开关（ScheduleSwitch），这通常会启动主调度循环。
	// 这个方法会一直运行，处理待调度的 Pod，直到传入的上下文 ctx.Done() 通道被关闭。
	sched.ScheduleSwitch.Run(ctx)
}

func (sched *Scheduler) createDataSet(idx int, subCluster string, switchType framework.SwitchType) ScheduleDataSet {
	var subClusterConfig *subClusterConfig
	if profile, ok := sched.options.subClusterProfiles[subCluster]; ok {
		subClusterConfig = newSubClusterConfigFromDefaultConfig(&profile, sched.defaultSubClusterConfig)
	} else {
		subClusterConfig = sched.defaultSubClusterConfig
	}
	klog.InfoS("CreateSubClusterWorkflow DataSet", "subCluster", subCluster, "clusterIndex", idx, "subClusterConfig", subClusterConfig)

	pluginArgs := make(map[string]*config.PluginConfig)
	for index := range subClusterConfig.PluginConfigs {
		pluginArg := subClusterConfig.PluginConfigs[index]
		pluginArgs[pluginArg.Name] = &pluginArg
	}
	unitQueueSortPlugin, err := godelqueue.InitUnitQueueSortPlugin(subClusterConfig.UnitQueueSortPlugin, pluginArgs)
	if err != nil {
		panic(err)
	}
	preemptionPluginArgs := make(map[string]*config.PluginConfig)
	for index := range subClusterConfig.PreemptionPluginConfigs {
		pluginArgs := subClusterConfig.PreemptionPluginConfigs[index]
		preemptionPluginArgs[pluginArgs.Name] = &pluginArgs
	}

	handler := commoncache.MakeCacheHandlerWrapper().
		SubCluster(subCluster).SwitchType(switchType).
		EnableStore(schedulerutil.FilterTrueKeys(subClusterConfig.EnableStore)...).
		PodLister(sched.podLister).
		PVCLister(sched.pvcLister).
		Obj()
	snapshot := godelcache.NewEmptySnapshot(handler)
	podScheduler := podscheduler.NewPodScheduler(
		sched.Name,
		switchType,
		subCluster,
		sched.client,
		sched.crdClient,
		sched.informerFactory,
		sched.crdInformerFactory,
		snapshot,
		sched.clock,
		subClusterConfig.DisablePreemption,
		subClusterConfig.CandidatesSelectPolicy,
		subClusterConfig.BetterSelectPolicies,
		subClusterConfig.PercentageOfNodesToScore,
		subClusterConfig.IncreasedPercentageOfNodesToScore,
		subClusterConfig.BasePlugins,
		pluginArgs,
		preemptionPluginArgs,
	)
	schedulingQueue := godelqueue.NewSchedulingQueue(
		sched.commonCache,
		sched.informerFactory.Scheduling().V1().PriorityClasses().Lister(),
		sched.crdInformerFactory.Scheduling().V1alpha1().PodGroups().Lister(),
		unitQueueSortPlugin.Less,
		subClusterConfig.UseBlockQueue,
		godelqueue.WithUnitInitialBackoffDuration(time.Duration(subClusterConfig.UnitInitialBackoffSeconds)*time.Second),
		godelqueue.WithPodMaxBackoffDuration(time.Duration(subClusterConfig.UnitMaxBackoffSeconds)*time.Second),
		godelqueue.WithOwner(sched.Name),
		godelqueue.WithSwitchType(switchType),
		godelqueue.WithSubCluster(subCluster),
		godelqueue.WithClock(sched.clock),
	)
	reconciler := reconciler.NewFailedTaskReconciler(sched.client, sched.informerFactory.Core().V1().Pods().Lister(), sched.commonCache, *sched.SchedulerName)
	unitScheduler := unitscheduler.NewUnitScheduler(
		sched.Name,
		switchType,
		subCluster,
		subClusterConfig.DisablePreemption,
		sched.client,
		sched.crdClient,
		sched.podLister,
		sched.pvcLister,
		sched.pgLister,
		sched.commonCache,
		snapshot,
		schedulingQueue,
		reconciler,
		podScheduler,
		sched.clock,
		sched.recorder,
		time.Duration(subClusterConfig.MaxWaitingDeletionDuration)*time.Second,
	)
	debugger := cachedebugger.New(
		sched.informerFactory.Core().V1().Nodes().Lister(),
		sched.informerFactory.Core().V1().Pods().Lister(),
		sched.commonCache,
		schedulingQueue,
	)
	return NewScheduleDataSet(
		idx,
		subCluster,
		switchType,
		snapshot,
		schedulingQueue,
		unitScheduler,
		reconciler,
		debugger,
	)
}

func (sched *Scheduler) createSubClusterWorkflow(idx int, subCluster string) (ScheduleDataSet, ScheduleDataSet) {
	klog.V(4).InfoS("Entered createSubClusterWorkflow", "subCluster", subCluster, "clusterIndex", idx)
	gt, be := framework.ClusterIndexToSwitchType(idx)
	for _, st := range []framework.SwitchType{gt, be} {
		dataSet := sched.createDataSet(idx, subCluster, st)
		sched.ScheduleSwitch.Register(st, dataSet)
	}
	return sched.ScheduleSwitch.Get(gt), sched.ScheduleSwitch.Get(be)
}
