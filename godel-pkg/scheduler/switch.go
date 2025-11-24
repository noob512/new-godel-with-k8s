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
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	nodev1alpha1 "github.com/kubewharf/godel-scheduler-api/pkg/apis/node/v1alpha1"
	katalystv1alpha1 "github.com/kubewharf/katalyst-api/pkg/apis/node/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/godel-pkg/features"
	framework "k8s.io/kubernetes/godel-pkg/framework/api"
	"k8s.io/kubernetes/godel-pkg/scheduler/cache"
	cachedebugger "k8s.io/kubernetes/godel-pkg/scheduler/cache/debugger"
	"k8s.io/kubernetes/godel-pkg/scheduler/core"
	"k8s.io/kubernetes/godel-pkg/scheduler/queue"
	"k8s.io/kubernetes/godel-pkg/scheduler/reconciler"
	podutil "k8s.io/kubernetes/godel-pkg/util/pod"
)

type CtxKeyScheduleDataSetType string

const (
	CtxKeyScheduleDataSet = CtxKeyScheduleDataSetType("ScheduleDataSet")
)

type ScheduleDataSet interface {
	ClusterIndex() int
	SubCluster() string
	Type() framework.SwitchType
	Ctx() context.Context

	Run(context.Context) bool
	Close() bool
	CanBeRecycle() bool

	Snapshot() *cache.Snapshot
	SchedulingQueue() queue.SchedulingQueue
	ScheduleFunc() func(context.Context)
}

type ScheduleDataSetImpl struct {
	ctx    context.Context
	cancel context.CancelFunc
	state  int32 // Default 0, be set to 1 after `Run`.

	clusterIndex int
	subCluster   string
	switchType   framework.SwitchType

	snapshot        *cache.Snapshot
	schedulingQueue queue.SchedulingQueue
	unitScheduler   core.UnitScheduler
	reconciler      *reconciler.FailedTaskReconciler
	debugger        *cachedebugger.CacheDebugger
}

func NewScheduleDataSet(
	idx int,
	subCluster string,
	switchType framework.SwitchType,
	snapshot *cache.Snapshot,
	schedulingQueue queue.SchedulingQueue,
	unitScheduler core.UnitScheduler,
	reconciler *reconciler.FailedTaskReconciler,
	debugger *cachedebugger.CacheDebugger,
) ScheduleDataSet {
	return &ScheduleDataSetImpl{
		clusterIndex:    idx,
		subCluster:      subCluster,
		switchType:      switchType,
		snapshot:        snapshot,
		schedulingQueue: schedulingQueue,
		unitScheduler:   unitScheduler,
		reconciler:      reconciler,
		debugger:        debugger,
	}
}

var _ ScheduleDataSet = &ScheduleDataSetImpl{}

func (s *ScheduleDataSetImpl) ClusterIndex() int {
	return s.clusterIndex
}

func (s *ScheduleDataSetImpl) SubCluster() string {
	return s.subCluster
}

func (s *ScheduleDataSetImpl) Type() framework.SwitchType {
	return s.switchType
}

func (s *ScheduleDataSetImpl) Ctx() context.Context {
	return s.ctx
}

// Run 启动当前 ScheduleDataSetImpl 实例所管理的调度工作流。
// 该方法确保每个实例只会被启动一次（通过原子状态标志），并在启动后并发运行多个核心组件。
// 参数 parentCtx 用于继承父上下文的生命周期（如来自调度器主循环的 context）。
// 返回值表示是否成功启动：true 表示首次成功启动，false 表示已处于运行或已启动过。
func (s *ScheduleDataSetImpl) Run(parentCtx context.Context) bool {
	// 使用原子操作进行状态检查和切换，防止重复启动。
	// 初始状态为 0（未启动），期望将其 CAS（Compare-And-Swap）为 1（已启动）。
	// 若当前状态不为 0（例如已是 1 或其他值），说明已启动过，直接返回 false。
	if !atomic.CompareAndSwapInt32(&s.state, 0, 1) {
		return false
	}

	// 记录启动日志（详细级别 4），包含子集群和调度开关类型，便于调试和追踪。
	klog.InfoS("Started the Run workflow", "subCluster", s.subCluster, "switchType", s.switchType)
	// 使用 defer 确保在函数退出时记录完成日志，无论成功或 panic。
	defer klog.V(4).InfoS("Completed the Run workflow", "subCluster", s.subCluster, "switchType", s.switchType)

	// 基于 parentCtx 创建一个可取消的子上下文，用于管理本 DataSet 内部所有 goroutine 的生命周期。
	s.ctx, s.cancel = context.WithCancel(parentCtx)

	// 并发启动三个核心组件（每个组件内部应已自行启动 goroutine 或监听循环）：
	
	// 1. schedulingQueue: 负责管理待调度的 Pod 队列，接收新 Pod 并触发调度流程。
	s.schedulingQueue.Run()
	
	// 2. reconciler: 负责状态协调（如处理 Pod 更新、删除、节点变化等事件），
	//    确保调度视图与集群实际状态最终一致。
	s.reconciler.Run()
	
	// 3. debugger: 可能用于运行时调试、指标采集、状态快照或诊断信息暴露，
	//    具体功能取决于实现（例如提供 HTTP 调试端点或日志钩子）。

	// s.debugger.Run()

	// 返回 true，表示本实例已成功启动。
	return true
}

func (s *ScheduleDataSetImpl) Close() bool {
	// Only workflows that have been started can be recycled.
	if atomic.LoadInt32(&s.state) != 1 {
		return false
	}

	klog.V(4).InfoS("Started the Close workflow", "subCluster", s.subCluster, "switchType", s.switchType)
	defer klog.V(4).InfoS("Completed the Close workflow", "subCluster", s.subCluster, "switchType", s.switchType)

	s.schedulingQueue.Close()
	s.reconciler.Close()
	s.debugger.Close()
	s.cancel()
	return true
}

func (s *ScheduleDataSetImpl) Snapshot() *cache.Snapshot {
	return s.snapshot
}

func (s *ScheduleDataSetImpl) SchedulingQueue() queue.SchedulingQueue {
	return s.schedulingQueue
}

func (s *ScheduleDataSetImpl) ScheduleFunc() func(context.Context) {
	return s.unitScheduler.Schedule
}

// TODO: revisit this rule.
func (s *ScheduleDataSetImpl) CanBeRecycle() bool {
	return s.schedulingQueue.CanBeRecycle() && s.unitScheduler.CanBeRecycle()
}

func (s *ScheduleDataSetImpl) String() string {
	return s.switchType.String() + "_" + s.subCluster
}

type ProcessFunc func(ScheduleDataSet)

type ScheduleSwitch interface {
	Run(context.Context)
	Get(framework.SwitchType) ScheduleDataSet
	Register(switchType framework.SwitchType, dataSet ScheduleDataSet)
	Process(framework.SwitchType, ProcessFunc)
}

type ScheduleSwitchImpl struct {
	registry map[framework.SwitchType]ScheduleDataSet
	mutex    sync.RWMutex
}

var _ ScheduleSwitch = &ScheduleSwitchImpl{}

func NewScheduleSwitch() ScheduleSwitch {
	return &ScheduleSwitchImpl{map[framework.SwitchType]ScheduleDataSet{}, sync.RWMutex{}}
}

// Run 启动调度开关（ScheduleSwitch）的核心控制逻辑，
// 根据不同 FeatureGate 的启用状态，决定如何运行调度数据集（DataSet）和工作流（Workflow）。
func (s *ScheduleSwitchImpl) Run(ctx context.Context) {
	// 情况一：如果「并发调度」特性（SchedulerConcurrentScheduling）未启用
	if !utilfeature.DefaultFeatureGate.Enabled(features.SchedulerConcurrentScheduling) {
		klog.Info("「并发调度」特性（SchedulerConcurrentScheduling）未启用")
		// 尝试从 registry 中获取全局 DataSet（标识为 DisableScheduleSwitch）
		if globalDataSet, ok := s.registry[framework.DisableScheduleSwitch]; ok && globalDataSet != nil {
			// 启动该 DataSet 的初始化逻辑
			globalDataSet.Run(ctx)
			// 使用 wait.UntilWithContext 循环执行该 DataSet 的调度函数（ScheduleFunc），
			// 间隔为 0，即尽可能快地连续执行（类似 for 循环），但受 context 控制生命周期。
			// 同时将 globalDataSet 注入到 context 中，便于调度函数内部访问。
			wait.UntilWithContext(
				context.WithValue(globalDataSet.Ctx(), CtxKeyScheduleDataSet, globalDataSet),
				globalDataSet.ScheduleFunc(),
				0,
			)
		} else {
			// 如果未找到所需的 DataSet，则记录严重错误并退出进程
			klog.ErrorS(nil, "SchedulerConcurrentScheduling was disabled while the DataSet couldn't be found")
			klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		}
		return
	}

	// 情况二：「并发调度」已启用，但「子集群并发调度」（SchedulerSubClusterConcurrentScheduling）未启用
	if !utilfeature.DefaultFeatureGate.Enabled(features.SchedulerSubClusterConcurrentScheduling) {
		// 此时只需启动一次工作流（workflowStartup），无需周期性执行
		klog.Info("「并发调度」已启用，但「子集群并发调度」未启用")
		s.workflowStartup(ctx)
		// 阻塞等待 context 被取消（如进程退出信号）
		<-ctx.Done()
		return
	}

	// 情况三：两个特性均启用（SchedulerConcurrentScheduling 和 SchedulerSubClusterConcurrentScheduling）
	// 此时系统支持动态创建/销毁调度工作流（例如按队列、子集群等维度）
	klog.Info("两种特性都被启动")
	// 1. 启动一个后台 Goroutine，周期性地检查并启动新的调度工作流
	//    （例如当新调度队列或子集群出现时，自动创建对应的调度循环）
	{
		go wait.UntilWithContext(ctx, s.workflowStartup, 1*time.Second)
	}

	// 2. 启动工作流回收机制，清理长期未使用的工作流以节省资源
	{
		// TODO: 在 beta 阶段是否应禁用回收？（避免误删）
		// TODO: 魔数（trick number）需重新评估

		// 当前回收间隔设为 24 小时（可能用于定期释放空闲资源）
		interval := 1 * 24 * time.Hour

		// 使用 wait.Until 在主 Goroutine 中定期执行回收逻辑
		wait.Until(func() {
			// 简单防护：当 Goroutine 数量超过 2000 时跳过回收（避免雪崩？）
			// TODO: 此阈值和逻辑需重新审视
			if runtime.NumGoroutine() > 2000 {
				return
			}
			// 执行回收：参数 0 表示“按空闲时间回收”
			s.workflowRecycle(0)
		}, interval, ctx.Done())

		// 在 context 取消后，执行一次强制回收（参数 1 可能表示“彻底清理”）
		// 确保进程退出前释放所有资源
		s.workflowRecycle(1)
	}
}

// workflowStartup 负责检查并启动所有子集群（subcluster）对应的调度工作流。
// 在 Godel 调度器中，每个子集群通常维护两个独立的调度数据集（DataSet）：
//   - GT（Guaranteed Tier）：用于高优先级/保障型工作负载
//   - BE（Best Effort）：用于低优先级/弹性工作负载
// 该方法确保这两个 DataSet 要么同时成功启动，要么都不启动，以维持调度语义的一致性。
func (s *ScheduleSwitchImpl) workflowStartup(ctx context.Context) {
	// 加锁保护 registry 的读取和 DataSet 状态变更，避免并发冲突。
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// 定义局部函数 startup：尝试启动一个 DataSet。
	// 若 DataSet 非空且首次成功启动（Run 返回 true），则：
	//   1. 记录启动日志；
	//   2. 启动一个无限循环的调度 goroutine（通过 wait.UntilWithContext）；
	//   3. 将 DataSet 注入 context，供调度函数内部使用。
	// 返回是否成功启动。
	startup := func(dataSet ScheduleDataSet) bool {
		if dataSet != nil && dataSet.Run(ctx) {
			klog.InfoS("Detected WorkflowStartup",
				"subCluster", dataSet.SubCluster(),
				"switchType", dataSet.Type())
			go wait.UntilWithContext(
				context.WithValue(dataSet.Ctx(), CtxKeyScheduleDataSet, dataSet),
				dataSet.ScheduleFunc(),
				0, // 间隔为 0，表示尽可能快地连续执行（由内部调度节流控制）
			)
			return true
		}
		return false
	}

	klog.InfoS("查看framework.MaxSwitchNum","framework.MaxSwitchNum",framework.MaxSwitchNum)
	// 遍历所有预定义的子集群索引（由 framework.MaxSwitchNum 限定最大数量）
	for i := 0; i < framework.MaxSwitchNum; i++ {
		// 从 registry 中获取当前子集群 i 对应的 GT 和 BE DataSet
		gtDataSet := s.registry[framework.ClusterIndexToGTSwitchType(i)]
		beDataSet := s.registry[framework.ClusterIndexToBESwitchType(i)]

		// 关键一致性校验：GT 和 BE DataSet 的启动结果必须一致。
		// 即：要么都成功启动，要么都未启动（例如尚未创建或已销毁）。
		// 若出现一个启动成功而另一个失败，说明系统处于不一致状态，属于严重错误。
		if startup(gtDataSet) != startup(beDataSet) {
			// TODO: 需要重新评估此错误日志的表述和处理策略。

			// 记录严重错误：同一子集群的两个工作流未能同步启动，违反设计约束。
			klog.ErrorS(nil,
				"WorkflowStartup was invalid, the workflows of the same subcluster could not start running at the same time, which should not happen",
				"subCluster", gtDataSet.SubCluster(),
				"index", i)

			// 立即刷新日志并退出进程，防止调度器在不一致状态下运行。
			klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		}
	}
}

func (s *ScheduleSwitchImpl) workflowRecycle(force int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	canBeRecycle := func(gtDataSet, beDataSet ScheduleDataSet) bool {
		if gtDataSet == nil || beDataSet == nil {
			return false
		}
		return force == 1 || (gtDataSet.CanBeRecycle() && beDataSet.CanBeRecycle())
	}

	recycle := func(dataSet ScheduleDataSet) bool {
		if dataSet != nil && dataSet.Close() {
			klog.V(4).InfoS("Detected WorkflowRecycle", "subCluster", dataSet.SubCluster(), "switchType", dataSet.Type(), "force", force)
			delete(s.registry, dataSet.Type())
			return true
		}
		return false
	}

	// ATTENTION: we won't recycle the index 0 unless force is 1.
	for i := 1 - force; i < framework.MaxSwitchNum; i++ {
		gtDataSet, beDataSet := s.registry[framework.ClusterIndexToGTSwitchType(i)], s.registry[framework.ClusterIndexToBESwitchType(i)]
		if canBeRecycle(gtDataSet, beDataSet) {
			if recycle(gtDataSet) != recycle(beDataSet) {
				// TODO: revisit this message.
				klog.ErrorS(nil, "WorkflowRecycle was invalid, the workflows of same subcluster could not stop running at the same time, which should not happen", "subCluster", gtDataSet.SubCluster(), "index", i)
				klog.FlushAndExit(klog.ExitFlushTimeout, 1)
			}
			freedClusterIndex := framework.FreeClusterIndex(gtDataSet.SubCluster())
			if freedClusterIndex != i {
				// TODO: revisit this message.
				klog.ErrorS(nil, "WorkflowRecycle was invalid, freed cluster index did not match the subcluster, which should not happen", "freedClusterIndex", freedClusterIndex, "index", i, "subCluster", gtDataSet.SubCluster())
				klog.FlushAndExit(klog.ExitFlushTimeout, 1)
			}
		}
	}
}

func (s *ScheduleSwitchImpl) Register(switchType framework.SwitchType, dataSet ScheduleDataSet) {
	if dataSet == nil {
		return
	}
	state := dataSet.Type()
	if switchType != state {
		panic("SwitchType doesn't match")
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	klog.V(4).InfoS("Registered workflow", "subCluster", dataSet.SubCluster(), "switchType", dataSet.Type())
	if _, ok := s.registry[state]; ok {
		panic("Duplicate ScheduleDataSet")
	}
	s.registry[state] = dataSet
}

func (s *ScheduleSwitchImpl) Get(state framework.SwitchType) ScheduleDataSet {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	var dataSet ScheduleDataSet
	for i := 0; 1<<i <= state; i++ {
		if (state >> i & 1) != 0 {
			if dataSet != nil {
				// This should not be happen.
				klog.ErrorS(nil, "Invalid SwitchType State", "state", state)
				klog.FlushAndExit(klog.ExitFlushTimeout, 1)
			}
			dataSet = s.registry[1<<i]
		}
	}
	return dataSet
}

func (s *ScheduleSwitchImpl) Process(state framework.SwitchType, f ProcessFunc) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if !utilfeature.DefaultFeatureGate.Enabled(features.SchedulerConcurrentScheduling) {
		if dataSet, ok := s.registry[framework.DisableScheduleSwitch]; ok && dataSet != nil {
			f(dataSet)
		} else {
			klog.ErrorS(nil, "SchedulerConcurrentScheduling was disabled while the DataSet couldn't be found")
			klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		}
		return
	}

	var wg sync.WaitGroup
	for i := 0; 1<<i < framework.SwitchTypeAll; i++ {
		if (state >> i & 1) != 0 {
			if dataSet, ok := s.registry[1<<i]; ok {
				wg.Add(1)
				go func() {
					defer func() {
						if err := recover(); err != nil {
							panic(err)
						}
						wg.Done()
					}()
					f(dataSet)
				}()
			}
		}
	}
	wg.Wait()
}

func ParseSwitchTypeForNode(node *v1.Node) framework.SwitchType {
	st := framework.DefaultSubClusterSwitchType
	if utilfeature.DefaultFeatureGate.Enabled(features.SchedulerSubClusterConcurrentScheduling) {
		return st |
			framework.ParseSwitchTypeFromSubCluster(node.Labels[framework.GetGlobalSubClusterKey()])
	}
	return st
}

func ParseSwitchTypeForNMNode(nmNode *nodev1alpha1.NMNode) framework.SwitchType {
	st := framework.DefaultSubClusterSwitchType
	if utilfeature.DefaultFeatureGate.Enabled(features.SchedulerSubClusterConcurrentScheduling) {
		return st |
			framework.ParseSwitchTypeFromSubCluster(nmNode.Labels[framework.GetGlobalSubClusterKey()])
	}
	return st
}

func ParseSwitchTypeForCNR(cnr *katalystv1alpha1.CustomNodeResource) framework.SwitchType {
	st := framework.DefaultSubClusterSwitchType
	if utilfeature.DefaultFeatureGate.Enabled(features.SchedulerSubClusterConcurrentScheduling) {
		return st |
			framework.ParseSwitchTypeFromSubCluster(cnr.Labels[framework.GetGlobalSubClusterKey()])
	}
	return st
}

func ParseSwitchTypeForPod(pod *v1.Pod) framework.SwitchType {
	var st framework.SwitchType
	if utilfeature.DefaultFeatureGate.Enabled(features.SchedulerSubClusterConcurrentScheduling) {
		st = framework.ParseSwitchTypeFromSubCluster(pod.Spec.NodeSelector[framework.GetGlobalSubClusterKey()])
	} else {
		st = framework.DefaultSubClusterSwitchType
	}
	resourceType, _ := podutil.GetPodResourceType(pod)
	if resourceType == podutil.BestEffortPod {
		return st & framework.BEBitMask
	}
	return st & framework.GTBitMask
}
