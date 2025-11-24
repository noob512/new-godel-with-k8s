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
	"errors"
	"fmt"
	"time"

	//--------------------------------------------------------------------------------
	godelclient "github.com/kubewharf/godel-scheduler-api/pkg/client/clientset/versioned"
	crdinformers "github.com/kubewharf/godel-scheduler-api/pkg/client/informers/externalversions"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/kubernetes/godel-pkg/scheduler/apis/config"
	commoncache "k8s.io/kubernetes/godel-pkg/common/cache"
	godelcache "k8s.io/kubernetes/godel-pkg/scheduler/cache"
	"k8s.io/client-go/tools/events"
	godelutil "k8s.io/kubernetes/godel-pkg/util"
	godelframework "k8s.io/kubernetes/godel-pkg/framework/api"
	"github.com/kubewharf/godel-scheduler-api/pkg/apis/scheduling/v1alpha1"
	//-------------------------------------------------------------------------------------

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	//"k8s.io/kube-scheduler/config/v1beta3"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	//"k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"
	frameworkplugins "k8s.io/kubernetes/pkg/scheduler/framework/plugins"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	internalcache "k8s.io/kubernetes/pkg/scheduler/internal/cache"
	cachedebugger "k8s.io/kubernetes/pkg/scheduler/internal/cache/debugger"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/internal/queue"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/profile"
	 
)
type subClusterConfig struct {
	PercentageOfNodesToScore          int32
	IncreasedPercentageOfNodesToScore int32

	MaxWaitingDeletionDuration int64

	UseBlockQueue                 bool
	UnitInitialBackoffSeconds     int64
	UnitMaxBackoffSeconds         int64
	AttemptImpactFactorOnPriority float64

	BasePlugins             godelframework.PluginCollectionSet
	PluginConfigs           []config.PluginConfig
	PreemptionPluginConfigs []config.PluginConfig
	UnitQueueSortPlugin     *godelframework.PluginSpec

	DisablePreemption      bool
	CandidatesSelectPolicy string
	BetterSelectPolicies   []string

	EnableStore map[string]bool
}

const (
	// maxUpdateRetries is the number of immediate, successive retries the Scheduler will attempt
	// when renewing the Scheduler status before it waits for the renewal interval before trying again,
	// similar to what we do for node status retries
	maxUpdateRetries = 5
	// sleep is the default interval for retry
	sleep = 100 * time.Millisecond
)

// StatusMaintainer manages creating and renewing the status for this Scheduler
type StatusMaintainer interface {
	Run(stopCh <-chan struct{})
}
type maintainer struct {
	crdClient     godelclient.Interface
	schedulerName string
	renewInterval time.Duration
	clock         clock.Clock
}

// Run 启动调度器状态维护器的主循环。
// 它会定期同步调度器的状态信息到自定义资源定义（CRD）中，直到收到停止信号。
func (c *maintainer) Run(stopCh <-chan struct{}) {
	// 检查 CRD 客户端是否已初始化。
	// CRD 客户端用于与存储调度器状态的 CRD 进行交互。
	if c.crdClient == nil {
		// 如果客户端为 nil，记录错误日志并退出程序。
		// 这通常意味着配置错误或依赖注入失败。
		klog.Info("c.crdClient为nil")
		klog.ErrorS(nil, "Exited the scheduler status maintainer because the CRD client was nil", "schedulerName", c.schedulerName)
		// 确保日志被刷新到输出，然后以退出码 1 结束程序。
	}
	// 启动一个无限循环，定期执行同步操作。
	// wait.Until 会按照 c.renewInterval 指定的时间间隔，调用 c.sync 方法。
	// 当 stopCh 通道被关闭时，循环会停止，Run 方法返回。
	klog.Info("c.crdClient不为nil")
	wait.Until(c.sync, c.renewInterval, stopCh)
}

// sync attempts to update the status for Scheduler
// update Status.LastUpdateTime at the moment
// sync 同步调度器的当前状态到其对应的 CRD（自定义资源定义）中。
// 它尝试更新 CRD 对象以反映调度器的最新状态（如健康状况、活跃节点列表等）。
// 如果更新失败，它会记录一条信息日志，然后在下次定时周期重试。
func (c *maintainer) sync() {
	// 调用 ensureSchedulerUpToDate 函数来执行实际的状态更新逻辑。
	// 该函数会使用 c.crdClient 与 API Server 通信，更新与 c.schedulerName 相关的 CRD 状态。
	if err := ensureSchedulerUpToDate(c.crdClient, c.clock, c.schedulerName); err != nil {
		// 如果更新失败（例如网络问题、权限不足、资源冲突等），
		// 记录一条 Info 级别的日志，告知用户更新失败，并将在下一次 sync 周期（由 c.renewInterval 控制）重试。
		klog.InfoS("Failed to update scheduler status, will retry later", "schedulerName", c.schedulerName, "renewInterval", c.renewInterval)
	}
	// 如果更新成功，函数静默返回，等待下一次定时调用。
}

// ensureSchedulerUpToDate try to update scheduler status, if failed, retry after sleep duration, at most maxUpdateRetries
// ensureSchedulerUpToDate 确保调度器的状态在 CRD 中保持最新。
// 它会尝试最多 maxUpdateRetries 次调用 updateSchedulerStatus 来更新状态。
// 如果所有重试都失败，则返回一个错误。
func ensureSchedulerUpToDate(client godelclient.Interface, clock clock.Clock, schedulerName string) error {
	// 循环最多 maxUpdateRetries 次，尝试更新调度器状态。
	for i := 0; i < maxUpdateRetries; i++ {
		// 调用 updateSchedulerStatus 尝试更新调度器在 CRD 中的状态。
		err := updateSchedulerStatus(client, schedulerName)
		klog.Info("更新一次调度器资源")
		if err != nil {
			// 如果更新失败，记录一条 Info 日志，包含错误信息。
			klog.InfoS("Failed to update scheduler, will retry later", "schedulerName", schedulerName, "err", err)
			// 使用 clock.Sleep 暂停 sleep 时间，然后再进行下一次重试。
			// 这可以避免在失败时进行过于频繁的 API 调用。
			clock.Sleep(sleep)
			// 继续下一次循环尝试更新。
			continue
		}
		// 如果更新成功（err 为 nil），则退出函数，返回 nil。
		return nil
	}
	// 如果循环结束仍未成功（即 maxUpdateRetries 次都失败了），
	// 返回一个格式化的错误，表明所有重试都已用尽。
	return fmt.Errorf("failed %d attempts to update scheduler status", maxUpdateRetries)
}

// updateSchedulerStatus tries to update Scheduler status to apiserver, if Scheduler not exists, add new Scheduler to apiserver
// updateSchedulerStatus 更新或创建指定名称的调度器 CRD 对象及其状态。
// 它会尝试获取现有的调度器 CRD，如果存在则更新其状态中的最后更新时间；
// 如果不存在，则创建一个新的调度器 CRD 对象，并随后更新其状态。
func updateSchedulerStatus(client godelclient.Interface, schedulerName string) error {
	// 获取当前时间，用于记录最后更新时间。
	now := metav1.Now()

	//schedulerName+="-my-custom"
	// 尝试从 API Server 获取指定名称的调度器 CRD 对象。
	existed, err := godelutil.GetScheduler(client, schedulerName)
	if err == nil && existed != nil {
		// 如果获取成功且对象存在。
		// 创建一个现有对象的深拷贝，以避免修改原始对象。
		updated := existed.DeepCopy()
		// 更新深拷贝对象的状态，设置最后更新时间为当前时间。
		updated.Status.LastUpdateTime = &now

		// 调用工具函数更新调度器 CRD 的状态子资源。
		if _, err := godelutil.UpdateSchedulerStatus(client, updated); err != nil {
			// 如果更新状态失败，包装错误信息并返回，以便调用方进行重试。
			err = fmt.Errorf("failed to update scheduler %v, will retry later, error is %v", schedulerName, err)
			return err
		}
		// 更新成功，返回 nil。
		return nil
	}

	// 检查获取失败的原因是否是因为对象不存在 (IsNotFound)。
	// 如果不是 NotFound 错误，说明是其他问题（如网络、权限等），记录错误并返回。
	if !apierrors.IsNotFound(err) {
		err = fmt.Errorf("failed to get scheduler %v, will retry later, error is %v", schedulerName, err)
		return err
	}

	// 如果错误是 NotFound (apierrors.IsNotFound(err) 为 true)，
	// 表示调度器 CRD 对象不存在。记录警告日志。
	klog.InfoS("WARN: scheduler was gone, should check this", "schedulerName", schedulerName, "err", err)

	// 准备创建一个新的调度器 CRD 对象。
	schedulerCRD := &v1alpha1.Scheduler{
		ObjectMeta: metav1.ObjectMeta{
			Name: schedulerName, // 设置对象名称。
		},
		// Status 字段可能在创建时是空的，稍后会单独更新。
	}

	// 调用工具函数创建调度器 CRD 对象。
	created, err := godelutil.PostScheduler(client, schedulerCRD)
	if err != nil {
		// 检查创建失败的原因是否是因为对象已存在 (IsAlreadyExists)。
		// 这可能发生在并发场景下。
		if apierrors.IsAlreadyExists(err) {
			// 如果是因为已存在，记录警告日志，但不视为错误，返回 nil。
			// 这可能意味着在检查和创建之间，另一个实例已经创建了该对象。
			klog.InfoS("WARN: skipped register because scheduler already existed", "schedulerName", schedulerName, "err", err)
			return nil
		}
		// 如果是其他创建错误，包装错误信息并返回。
		err = fmt.Errorf("failed to update scheduler %v, will retry later, error is %v", schedulerName, err)
		return err
	}

	// 创建成功。通常，创建对象时无法直接设置 status 子资源。
	// 因此，需要单独更新 status。
	// 更新刚创建对象的状态，设置最后更新时间为当前时间。
	created.Status.LastUpdateTime = &now
	// 调用工具函数更新调度器 CRD 的状态子资源。
	if _, err := godelutil.UpdateSchedulerStatus(client, created); err != nil {
		// 如果更新状态失败，包装错误信息并返回。
		err = fmt.Errorf("failed to update scheduler %v, will retry later, error is %v", schedulerName, err)
		return err
	}
	// 成功创建并更新状态，返回 nil。
	return nil
}

// NewSchedulerStatusMaintainer constructs and returns a maintainer
func NewSchedulerStatusMaintainer(clock clock.Clock, client godelclient.Interface, schedulerName string, renewIntervalSeconds int64) StatusMaintainer {
	renewInterval := time.Duration(renewIntervalSeconds) * time.Second
	return &maintainer{
		crdClient:     client,
		schedulerName: schedulerName,
		renewInterval: renewInterval,
		clock:         clock,
	}
}

// onSchedulerUpdate 是一个事件处理器函数，当监听的 Scheduler 自定义资源 (CRD) 发生更新时被调用。
// 此函数目前的实现是无条件地触发调度器的调度开关 (ScheduleSwitch)，
// 但具体的处理逻辑（在 Process 函数的回调中）是空的。
// 参数 `oldObj` 和 `newObj` 分别代表更新前和更新后的 Scheduler 对象，
// 但在此函数中并未使用它们的具体内容。
func (sched *Scheduler) onSchedulerUpdate(oldObj, newObj interface{}) {
	klog.Info("onSchedulerUpdate函数被调用")
	//// 调用调度开关的 Process 方法。
	//// 这通常用于通知调度器其内部状态或配置可能已更改，需要进行相应处理。
	//sched.ScheduleSwitch.Process(
	//	//
	//	// 目前硬编码为 framework.SwitchTypeAll，表示可能需要处理所有类型的变更。
	//	godelframework.SwitchTypeAll,
	//	// 传入一个空的处理函数。
	//	// 这个函数接收一个 ScheduleDataSet 参数，该参数理论上包含更新所需的数据。
	//	// 但当前实现中，此函数体为空，意味着没有实际的处理逻辑。
	//	func(dataSet ScheduleDataSet) {},
	//)
}
//-----------------------------------------------------------





const (
	// Duration the scheduler will wait before expiring an assumed pod.
	// See issue #106361 for more details about this parameter and its value.
	durationToExpireAssumedPod = 15 * time.Minute
)

// ErrNoNodesAvailable is used to describe the error that no nodes available to schedule pods.
var ErrNoNodesAvailable = fmt.Errorf("no nodes available to schedule pods")

// Scheduler watches for new unscheduled pods. It attempts to find
// nodes that they fit on and writes bindings back to the api server.
type Scheduler struct {
		//------------------------------------------------------------
	// Name 用于标识这个 Godel 调度器实例的名称。
	Name string
	// SchedulerName 是更高层级的调度器名称，用于选择哪些 Pod 应由 Godel 调度器负责，
	// 并过滤掉不相关的 Pod。
	// Pod 的 Spec.SchedulerName 必须与此字段匹配，才会被此调度器处理。
	SchedulerName *string

	// informerFactory 是标准 Kubernetes 核心资源的 SharedInformer 工厂。
	informerFactory informers.SharedInformerFactory

	// crdInformerFactory 是 Godel 自定义资源的 SharedInformer 工厂。
	crdInformerFactory crdinformers.SharedInformerFactory

	// crdClient 是 Godel 自定义资源（如 Scheduler, PodGroup 等）的客户端接口。
	crdClient godelclient.Interface

	// options 存储调度器的配置选项。
	options schedulerOptions

	// podLister 是 Pod 资源的 Lister，提供对 Pod 信息的只读缓存访问。
	podLister corelisters.PodLister

	// commonCache 是调度器使用的缓存接口，用于存储和管理节点、Pod 等资源的状态信息。
	commonCache godelcache.SchedulerCache

	// mayHasPreemption 标记此调度器实例是否可能执行抢占（Preemption）操作。
	mayHasPreemption bool
	// defaultSubClusterConfig 是默认子集群的配置。
	defaultSubClusterConfig *subClusterConfig


	// schedulerMaintainer 是一个状态维护器，负责维护和更新调度器自身的状态。
	schedulerMaintainer StatusMaintainer


	// recorder 是事件记录器，用于向 Kubernetes API Server 发送调度器相关的事件。
	// 根据 KEP 383，这应该是新的 events.k8s.io/v1 API 的适配器。
	recorder events.EventRecorder

	//------------------------------------------------------------
	// It is expected that changes made via Cache will be observed
	// by NodeLister and Algorithm.
	Cache internalcache.Cache

	Extenders []framework.Extender

	// NextPod should be a function that blocks until the next pod
	// is available. We don't use a channel for this, because scheduling
	// a pod may take some amount of time and we don't want pods to get
	// stale while they sit in a channel.
	NextPod func() *framework.QueuedPodInfo

	// Error is called if there is an error. It is passed the pod in
	// question, and the error
	Error func(*framework.QueuedPodInfo, error)

	// SchedulePod tries to schedule the given pod to one of the nodes in the node list.
	// Return a struct of ScheduleResult with the name of suggested host on success,
	// otherwise will return a FitError with reasons.
	SchedulePod func(ctx context.Context, fwk framework.Framework, state *framework.CycleState, pod *v1.Pod) (ScheduleResult, error)

	// Close this to shut down the scheduler.
	StopEverything <-chan struct{}

	// SchedulingQueue holds pods to be scheduled
	SchedulingQueue internalqueue.SchedulingQueue

	// Profiles are the scheduling profiles.
	Profiles profile.Map

	client clientset.Interface

	nodeInfoSnapshot *internalcache.Snapshot

	percentageOfNodesToScore int32

	nextStartNodeIndex int
}

type schedulerOptions struct {
	componentConfigVersion            string
	kubeConfig                        *restclient.Config
	percentageOfNodesToScore          int32
	podInitialBackoffSeconds          int64
	podMaxBackoffSeconds              int64
	podMaxInUnschedulablePodsDuration time.Duration
	// Contains out-of-tree plugins to be merged with the in-tree registry.
	frameworkOutOfTreeRegistry frameworkruntime.Registry
	profiles                   []schedulerapi.KubeSchedulerProfile
	extenders                  []schedulerapi.Extender
	frameworkCapturer          FrameworkCapturer
	parallelism                int32
	applyDefaultProfile        bool
}

// Option configures a Scheduler
type Option func(*schedulerOptions)

// ScheduleResult represents the result of scheduling a pod.
type ScheduleResult struct {
	// Name of the selected node.
	SuggestedHost string
	// The number of nodes the scheduler evaluated the pod against in the filtering
	// phase and beyond.
	EvaluatedNodes int
	// The number of nodes out of the evaluated ones that fit the pod.
	FeasibleNodes int
}
//-=-------------------------------------------
type GodelschedulerOptions struct {
	defaultProfile     *config.GodelSchedulerProfile
	subClusterProfiles map[string]config.GodelSchedulerProfile

	renewInterval int64
	subClusterKey string
}

var defaultGodelSchedulerOptions = GodelschedulerOptions{
	renewInterval: config.DefaultRenewIntervalInSeconds,
	subClusterKey: config.DefaultSubClusterKey,
}
//-------------------------------------------

// WithComponentConfigVersion sets the component config version to the
// KubeSchedulerConfiguration version used. The string should be the full
// scheme group/version of the external type we converted from (for example
// "kubescheduler.config.k8s.io/v1beta2")
func WithComponentConfigVersion(apiVersion string) Option {
	return func(o *schedulerOptions) {
		o.componentConfigVersion = apiVersion
	}
}

// WithKubeConfig sets the kube config for Scheduler.
func WithKubeConfig(cfg *restclient.Config) Option {
	return func(o *schedulerOptions) {
		o.kubeConfig = cfg
	}
}

// WithProfiles sets profiles for Scheduler. By default, there is one profile
// with the name "default-scheduler".
func WithProfiles(p ...schedulerapi.KubeSchedulerProfile) Option {
	return func(o *schedulerOptions) {
		o.profiles = p
		o.applyDefaultProfile = false
	}
}

// WithParallelism sets the parallelism for all scheduler algorithms. Default is 16.
func WithParallelism(threads int32) Option {
	return func(o *schedulerOptions) {
		o.parallelism = threads
	}
}

// WithPercentageOfNodesToScore sets percentageOfNodesToScore for Scheduler, the default value is 50
func WithPercentageOfNodesToScore(percentageOfNodesToScore int32) Option {
	return func(o *schedulerOptions) {
		o.percentageOfNodesToScore = percentageOfNodesToScore
	}
}

// WithFrameworkOutOfTreeRegistry sets the registry for out-of-tree plugins. Those plugins
// will be appended to the default registry.
func WithFrameworkOutOfTreeRegistry(registry frameworkruntime.Registry) Option {
	return func(o *schedulerOptions) {
		o.frameworkOutOfTreeRegistry = registry
	}
}

// WithPodInitialBackoffSeconds sets podInitialBackoffSeconds for Scheduler, the default value is 1
func WithPodInitialBackoffSeconds(podInitialBackoffSeconds int64) Option {
	return func(o *schedulerOptions) {
		o.podInitialBackoffSeconds = podInitialBackoffSeconds
	}
}

// WithPodMaxBackoffSeconds sets podMaxBackoffSeconds for Scheduler, the default value is 10
func WithPodMaxBackoffSeconds(podMaxBackoffSeconds int64) Option {
	return func(o *schedulerOptions) {
		o.podMaxBackoffSeconds = podMaxBackoffSeconds
	}
}

// WithPodMaxInUnschedulablePodsDuration sets podMaxInUnschedulablePodsDuration for PriorityQueue.
func WithPodMaxInUnschedulablePodsDuration(duration time.Duration) Option {
	return func(o *schedulerOptions) {
		o.podMaxInUnschedulablePodsDuration = duration
	}
}

// WithExtenders sets extenders for the Scheduler
func WithExtenders(e ...schedulerapi.Extender) Option {
	return func(o *schedulerOptions) {
		o.extenders = e
	}
}

// FrameworkCapturer is used for registering a notify function in building framework.
type FrameworkCapturer func(schedulerapi.KubeSchedulerProfile)

// WithBuildFrameworkCapturer sets a notify function for getting buildFramework details.
func WithBuildFrameworkCapturer(fc FrameworkCapturer) Option {
	return func(o *schedulerOptions) {
		o.frameworkCapturer = fc
	}
}

var defaultSchedulerOptions = schedulerOptions{
	percentageOfNodesToScore:          schedulerapi.DefaultPercentageOfNodesToScore,
	podInitialBackoffSeconds:          int64(internalqueue.DefaultPodInitialBackoffDuration.Seconds()),
	podMaxBackoffSeconds:              int64(internalqueue.DefaultPodMaxBackoffDuration.Seconds()),
	podMaxInUnschedulablePodsDuration: internalqueue.DefaultPodMaxInUnschedulablePodsDuration,
	parallelism:                       int32(parallelize.DefaultParallelism),
	// Ideally we would statically set the default profile here, but we can't because
	// creating the default profile may require testing feature gates, which may get
	// set dynamically in tests. Therefore, we delay creating it until New is actually
	// invoked.
	applyDefaultProfile: true,
}

// New returns a Scheduler
// New 函数创建并初始化一个新的调度器实例。
// 它负责设置调度器的各种组件，包括配置选项、插件注册表、扩展器、队列、缓存和事件处理器。
func New(
	godelSchedulerName string, // Godel 调度器的名称（可能用于区分不同的调度器实例）
	schedulerName *string,     // 调度器的名称指针
	crdClient godelclient.Interface, // Godel 自定义资源定义的客户端
	crdInformerFactory crdinformers.SharedInformerFactory, // Godel CRD 的 Informer 工厂
	client clientset.Interface,        // Kubernetes 核心 API 的客户端
	informerFactory informers.SharedInformerFactory, // 标准 Kubernetes 资源的 Informer 工厂
	dynInformerFactory dynamicinformer.DynamicSharedInformerFactory, // 动态资源的 Informer 工厂
	recorderFactory profile.RecorderFactory, // 事件记录器工厂
	stopCh <-chan struct{}, // 用于停止调度器的通道
	opts ...Option) (*Scheduler, error) { // 可变数量的选项函数，用于配置调度器

	// 如果没有提供停止通道，则使用一个永远不会关闭的通道。
	stopEverything := stopCh
	if stopEverything == nil {
		stopEverything = wait.NeverStop
	}

	// 获取默认的调度器选项。
	options := defaultSchedulerOptions
	// 遍历并应用所有传入的选项函数，以修改默认选项。
	//--------------------------------------------------
	Godeloptions:=defaultGodelSchedulerOptions
	globalClock := clock.RealClock{}
	podLister := informerFactory.Core().V1().Pods().Lister()
	podInformer := informerFactory.Core().V1().Pods()
	//-------------------------------------------------
	for _, opt := range opts {
		opt(&options)
	}

	podLister = informerFactory.Core().V1().Pods().Lister()
	nodeLister := informerFactory.Core().V1().Nodes().Lister()

	handlerWrapper := commoncache.MakeCacheHandlerWrapper().
	ComponentName(godelSchedulerName).
	SchedulerType(*schedulerName).
	PodAssumedTTL(15 * time.Minute). // Pod 假定（assumed）状态的 TTL
	Period(10 * time.Second).        // 缓存定期同步周期
	StopCh(stopEverything).
	PodLister(podLister).
	PodInformer(podInformer)

	

	// 创建内置（in-tree）插件的注册表。
	registry := frameworkplugins.NewInTreeRegistry()
	// 将外部（out-of-tree）插件注册表合并到内置注册表中。
	if err := registry.Merge(options.frameworkOutOfTreeRegistry); err != nil {
		return nil, err
	}

	// 注册调度器相关的指标。
	metrics.Register()

	// 根据选项中的扩展器配置和配置文件构建扩展器列表。
	extenders, err := buildExtenders(options.extenders, options.profiles)
	if err != nil {
		return nil, fmt.Errorf("couldn't build extenders: %w", err)
	}



	// 创建 Pod 提名器（Nominator），用于在调度决策过程中暂时提名一个节点给 Pod。
	// 这个提名器会在调度框架和队列之间传递。
	nominator := internalqueue.NewPodNominator(podLister)
	// 创建一个空的调度缓存快照，用于插件在调度过程中进行只读访问。
	snapshot := internalcache.NewEmptySnapshot()
	// 创建一个映射，用于跟踪调度框架需要监听的集群事件类型。
	clusterEventMap := make(map[framework.ClusterEvent]sets.String)

	// 为每个配置文件创建调度框架实例。
	// 框架是调度逻辑的核心，它集成了插件、扩展器等。
	profiles, err := profile.NewMap(options.profiles, registry, recorderFactory,
		frameworkruntime.WithComponentConfigVersion(options.componentConfigVersion), // 设置组件配置版本
		frameworkruntime.WithClientSet(client),              // 设置 Kubernetes 客户端
		frameworkruntime.WithKubeConfig(options.kubeConfig), // 设置 kubeconfig
		frameworkruntime.WithInformerFactory(informerFactory), // 设置 Informer 工厂
		frameworkruntime.WithSnapshotSharedLister(snapshot),   // 设置快照共享 Lister
		frameworkruntime.WithPodNominator(nominator),          // 设置 Pod 提名器
		frameworkruntime.WithCaptureProfile(frameworkruntime.CaptureProfile(options.frameworkCapturer)), // 设置配置文件捕获器
		frameworkruntime.WithClusterEventMap(clusterEventMap), // 设置集群事件映射
		frameworkruntime.WithParallelism(int(options.parallelism)), // 设置并行度
		frameworkruntime.WithExtenders(extenders),           // 设置扩展器
	)
	if err != nil {
		return nil, fmt.Errorf("initializing profiles: %v", err)
	}

	// 确保至少有一个配置文件被成功创建。
	if len(profiles) == 0 {
		return nil, errors.New("at least one profile is required")
	}

	// 创建调度队列，用于存放等待调度的 Pod。
	// 队列会根据配置文件中的 QueueSort 插件来排序 Pod。
	podQueue := internalqueue.NewSchedulingQueue(
		profiles[options.profiles[0].SchedulerName].QueueSortFunc(), // 使用第一个配置文件的队列排序函数
		informerFactory,                                           // Informer 工厂
		internalqueue.WithPodInitialBackoffDuration(time.Duration(options.podInitialBackoffSeconds)*time.Second), // 设置 Pod 初始退避时间
		internalqueue.WithPodMaxBackoffDuration(time.Duration(options.podMaxBackoffSeconds)*time.Second),       // 设置 Pod 最大退避时间
		internalqueue.WithPodNominator(nominator),                 // 设置 Pod 提名器
		internalqueue.WithClusterEventMap(clusterEventMap),        // 设置集群事件映射
		internalqueue.WithPodMaxInUnschedulablePodsDuration(options.podMaxInUnschedulablePodsDuration), // 设置 Pod 在未调度Pod列表中的最大持续时间
	)

	// 创建调度缓存，用于存储集群的节点和 Pod 信息。
	// durationToExpireAssumedPod 是假设 Pod 过期的持续时间。
	schedulerCache := internalcache.New(durationToExpireAssumedPod, stopEverything)

	// 设置缓存调试器，用于诊断缓存和队列的状态。
	debugger := cachedebugger.New(nodeLister, podLister, schedulerCache, podQueue)
	// 让调试器监听信号以启动调试功能。
	debugger.ListenForSignal(stopEverything)

	// 创建调度器结构体实例。
	sched := newScheduler(
		schedulerCache,                                   // 调度缓存
		extenders,                                        // 扩展器
		internalqueue.MakeNextPodFunc(podQueue),         // 获取下一个待调度 Pod 的函数
		MakeDefaultErrorFunc(client, podLister, podQueue, schedulerCache), // 默认错误处理函数
		stopEverything,                                   // 停止通道
		podQueue,                                         // 调度队列
		profiles,                                         // 调度配置文件映射
		client,                                           // Kubernetes 客户端
		snapshot,                                         // 调度缓存快照
		options.percentageOfNodesToScore,                 // 评分节点的百分比
	)
	//---------------------------------------
	sched.Name=godelSchedulerName
	sched.SchedulerName=schedulerName
	sched.commonCache=godelcache.New(handlerWrapper.Obj())
	sched.mayHasPreemption=false
	sched.schedulerMaintainer=NewSchedulerStatusMaintainer(globalClock, crdClient, godelSchedulerName, Godeloptions.renewInterval)
	sched.podLister=podLister
	sched.informerFactory=informerFactory
	sched.crdInformerFactory=crdInformerFactory
	// 对配置器创建的调度器实例进行额外的调整。
	sched.StopEverything = stopEverything // 设置停止信号通道。
	sched.client = client                 // 设置 API 客户端。
	//---------------------------------------

	// 添加所有事件处理器，监听 Pod、Node 等资源的变化，并相应地更新缓存和队列。
	addAllEventHandlers(sched, informerFactory, dynInformerFactory, unionedGVKs(clusterEventMap),crdInformerFactory)

	// 返回创建好的调度器实例。
	return sched, nil
}

// Run begins watching and scheduling. It starts scheduling and blocked until the context is done.
func (sched *Scheduler) Run(ctx context.Context) {
	go sched.schedulerMaintainer.Run(sched.StopEverything)
	sched.SchedulingQueue.Run()
	wait.UntilWithContext(ctx, sched.scheduleOne, 0)
	sched.SchedulingQueue.Close()
}

// MakeDefaultErrorFunc construct a function to handle pod scheduler error
func MakeDefaultErrorFunc(client clientset.Interface, podLister corelisters.PodLister, podQueue internalqueue.SchedulingQueue, schedulerCache internalcache.Cache) func(*framework.QueuedPodInfo, error) {
	return func(podInfo *framework.QueuedPodInfo, err error) {
		pod := podInfo.Pod
		if err == ErrNoNodesAvailable {
			klog.V(2).InfoS("Unable to schedule pod; no nodes are registered to the cluster; waiting", "pod", klog.KObj(pod))
		} else if fitError, ok := err.(*framework.FitError); ok {
			// Inject UnschedulablePlugins to PodInfo, which will be used later for moving Pods between queues efficiently.
			podInfo.UnschedulablePlugins = fitError.Diagnosis.UnschedulablePlugins
			klog.V(2).InfoS("Unable to schedule pod; no fit; waiting", "pod", klog.KObj(pod), "err", err)
		} else if apierrors.IsNotFound(err) {
			klog.V(2).InfoS("Unable to schedule pod, possibly due to node not found; waiting", "pod", klog.KObj(pod), "err", err)
			if errStatus, ok := err.(apierrors.APIStatus); ok && errStatus.Status().Details.Kind == "node" {
				nodeName := errStatus.Status().Details.Name
				// when node is not found, We do not remove the node right away. Trying again to get
				// the node and if the node is still not found, then remove it from the scheduler cache.
				_, err := client.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
				if err != nil && apierrors.IsNotFound(err) {
					node := v1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}
					if err := schedulerCache.RemoveNode(&node); err != nil {
						klog.V(4).InfoS("Node is not found; failed to remove it from the cache", "node", node.Name)
					}
				}
			}
		} else {
			klog.ErrorS(err, "Error scheduling pod; retrying", "pod", klog.KObj(pod))
		}

		// Check if the Pod exists in informer cache.
		cachedPod, err := podLister.Pods(pod.Namespace).Get(pod.Name)
		if err != nil {
			klog.InfoS("Pod doesn't exist in informer cache", "pod", klog.KObj(pod), "err", err)
			return
		}

		// In the case of extender, the pod may have been bound successfully, but timed out returning its response to the scheduler.
		// It could result in the live version to carry .spec.nodeName, and that's inconsistent with the internal-queued version.
		if len(cachedPod.Spec.NodeName) != 0 {
			klog.InfoS("Pod has been assigned to node. Abort adding it back to queue.", "pod", klog.KObj(pod), "node", cachedPod.Spec.NodeName)
			return
		}

		// As <cachedPod> is from SharedInformer, we need to do a DeepCopy() here.
		podInfo.PodInfo = framework.NewPodInfo(cachedPod.DeepCopy())
		if err := podQueue.AddUnschedulableIfNotPresent(podInfo, podQueue.SchedulingCycle()); err != nil {
			klog.ErrorS(err, "Error occurred")
		}
	}
}

// NewInformerFactory creates a SharedInformerFactory and initializes a scheduler specific
// in-place podInformer.
func NewInformerFactory(cs clientset.Interface, resyncPeriod time.Duration) informers.SharedInformerFactory {
	informerFactory := informers.NewSharedInformerFactory(cs, resyncPeriod)
	informerFactory.InformerFor(&v1.Pod{}, newPodInformer)
	return informerFactory
}

func buildExtenders(extenders []schedulerapi.Extender, profiles []schedulerapi.KubeSchedulerProfile) ([]framework.Extender, error) {
	var fExtenders []framework.Extender
	if len(extenders) == 0 {
		return nil, nil
	}

	var ignoredExtendedResources []string
	var ignorableExtenders []framework.Extender
	for i := range extenders {
		klog.V(2).InfoS("Creating extender", "extender", extenders[i])
		extender, err := NewHTTPExtender(&extenders[i])
		if err != nil {
			return nil, err
		}
		if !extender.IsIgnorable() {
			fExtenders = append(fExtenders, extender)
		} else {
			ignorableExtenders = append(ignorableExtenders, extender)
		}
		for _, r := range extenders[i].ManagedResources {
			if r.IgnoredByScheduler {
				ignoredExtendedResources = append(ignoredExtendedResources, r.Name)
			}
		}
	}
	// place ignorable extenders to the tail of extenders
	fExtenders = append(fExtenders, ignorableExtenders...)

	// If there are any extended resources found from the Extenders, append them to the pluginConfig for each profile.
	// This should only have an effect on ComponentConfig, where it is possible to configure Extenders and
	// plugin args (and in which case the extender ignored resources take precedence).
	if len(ignoredExtendedResources) == 0 {
		return fExtenders, nil
	}

	for i := range profiles {
		prof := &profiles[i]
		var found = false
		for k := range prof.PluginConfig {
			if prof.PluginConfig[k].Name == noderesources.Name {
				// Update the existing args
				pc := &prof.PluginConfig[k]
				args, ok := pc.Args.(*schedulerapi.NodeResourcesFitArgs)
				if !ok {
					return nil, fmt.Errorf("want args to be of type NodeResourcesFitArgs, got %T", pc.Args)
				}
				args.IgnoredResources = ignoredExtendedResources
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("can't find NodeResourcesFitArgs in plugin config")
		}
	}
	return fExtenders, nil
}

// newScheduler creates a Scheduler object.
func newScheduler(
	cache internalcache.Cache,
	extenders []framework.Extender,
	nextPod func() *framework.QueuedPodInfo,
	Error func(*framework.QueuedPodInfo, error),
	stopEverything <-chan struct{},
	schedulingQueue internalqueue.SchedulingQueue,
	profiles profile.Map,
	client clientset.Interface,
	nodeInfoSnapshot *internalcache.Snapshot,
	percentageOfNodesToScore int32) *Scheduler {
	sched := Scheduler{
		Cache:                    cache,
		Extenders:                extenders,
		NextPod:                  nextPod,
		Error:                    Error,
		StopEverything:           stopEverything,
		SchedulingQueue:          schedulingQueue,
		Profiles:                 profiles,
		client:                   client,
		nodeInfoSnapshot:         nodeInfoSnapshot,
		percentageOfNodesToScore: percentageOfNodesToScore,
	}
	sched.SchedulePod = sched.schedulePod
	return &sched
}

func unionedGVKs(m map[framework.ClusterEvent]sets.String) map[framework.GVK]framework.ActionType {
	gvkMap := make(map[framework.GVK]framework.ActionType)
	for evt := range m {
		if _, ok := gvkMap[evt.Resource]; ok {
			gvkMap[evt.Resource] |= evt.ActionType
		} else {
			gvkMap[evt.Resource] = evt.ActionType
		}
	}
	return gvkMap
}

// newPodInformer creates a shared index informer that returns only non-terminal pods.
func newPodInformer(cs clientset.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	selector := fmt.Sprintf("status.phase!=%v,status.phase!=%v", v1.PodSucceeded, v1.PodFailed)
	tweakListOptions := func(options *metav1.ListOptions) {
		options.FieldSelector = selector
	}
	return coreinformers.NewFilteredPodInformer(cs, metav1.NamespaceAll, resyncPeriod, nil, tweakListOptions)
}
