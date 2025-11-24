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
	"context"
	"fmt"

	scheduling "github.com/kubewharf/godel-scheduler-api/pkg/apis/scheduling/v1alpha1"
	crdclient "github.com/kubewharf/godel-scheduler-api/pkg/client/clientset/versioned"
	schedulinginformer "github.com/kubewharf/godel-scheduler-api/pkg/client/informers/externalversions/scheduling/v1alpha1"
	nodelister "github.com/kubewharf/godel-scheduler-api/pkg/client/listers/node/v1alpha1"
	schedulinglister "github.com/kubewharf/godel-scheduler-api/pkg/client/listers/scheduling/v1alpha1"
	v1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	listerv1 "k8s.io/client-go/listers/core/v1"
	schedulingv1 "k8s.io/client-go/listers/scheduling/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/godel-pkg/dispatcher/internal/queue"
	"k8s.io/kubernetes/godel-pkg/dispatcher/internal/store"
	"k8s.io/kubernetes/godel-pkg/dispatcher/metrics"
	nodeshuffler "k8s.io/kubernetes/godel-pkg/dispatcher/node-shuffler"
	"k8s.io/kubernetes/godel-pkg/dispatcher/reconciler"
	schemaintainer "k8s.io/kubernetes/godel-pkg/dispatcher/scheduler-maintainer"
	"k8s.io/kubernetes/godel-pkg/features"
	"k8s.io/kubernetes/godel-pkg/util"
	podutil "k8s.io/kubernetes/godel-pkg/util/pod"
)

const (
	DispatcherTag = "dispatcher"
)

// Dispatcher 是 Godel 调度器架构中的核心组件，负责接收待调度的 Pod，
// 并根据节点分区和负载均衡策略将它们分发给合适的 Scheduler 实例进行处理。
type Dispatcher struct {
	// StopEverything 是一个只读的结构体通道 (struct{} channel)，用于接收停止信号。
	// 当外部上下文取消或需要优雅关闭 Dispatcher 时，会关闭此通道。
	// Dispatcher 内部的 goroutines 会监听此通道，一旦收到信号就会停止运行。
	StopEverything <-chan struct{}

	// client 是 Kubernetes 核心 API 的客户端，用于与 API Server 进行交互，
	// 例如更新 Pod 状态、获取 Node 信息等。
	client kubernetes.Interface

	// podLister 是一个 Pod 列表器，提供对本地缓存中 Pod 对象的只读访问。
	// 它通常由一个 Informer 维护，避免了频繁的 API Server 查询。
	podLister listerv1.PodLister

	// TODO: move to policy manager
	// UnitInfos (单元信息管理器) 存储属于调度单元 (Scheduling Unit) 的待处理 Pod 信息。
	// Godel 调度器以单元为粒度进行调度，UnitInfos 负责管理这些单元的状态和 Pod 列表。
	// 注释 "move to policy manager" 暗示这部分逻辑未来可能被移到策略管理模块。
	UnitInfos queue.UnitInfos

	// FIFOPendingPodsQueue 是一个先进先出 (FIFO) 队列，用于存储待分发的 Pod。
	// 这里的注释表明其必要性待定，可能在未来被移除或整合。
	// TODO: figure out if we really need this queue
	// TODO: move to policy manager if necessary
	FIFOPendingPodsQueue queue.PendingQueue

	// SortedPodsQueue 存储已经根据配置的排序策略排好序的待处理 Pod。
	// 当 Dispatcher 需要向调度器分发 Pod 时，会从此队列中取出 Pod。
	// 排序可能基于优先级、创建时间、资源需求等因素。
	SortedPodsQueue queue.SortedQueue

	// DispatchInfo 存储与分发过程相关的状态和信息，例如分发成功率、延迟统计等。
	DispatchInfo store.DispatchInfo

	// OwnerInfos 存储 Pod 的所有者信息（如 ReplicaSet, Deployment 等），
	// 可能用于调度决策或状态管理。
	OwnerInfos store.OwnerInfo

	// SchedulerLister 是 Godel Scheduler CRD 的列表器，用于获取自定义调度器实例的信息。
	SchedulerLister     schedulinglister.SchedulerLister
	// NodeLister 是 Node 资源的列表器，用于获取集群节点信息。
	NodeLister          listerv1.NodeLister
	// NMNodeLister 是 Godel NMNode CRD 的列表器，用于获取节点的扩展信息。
	NMNodeLister        nodelister.NMNodeLister
	// PodGroupLister 是 Godel PodGroup CRD 的列表器，用于获取 Pod 组信息。
	PodGroupLister      schedulinglister.PodGroupLister
	// PriorityClassLister 是 PriorityClass 资源的列表器，用于获取 Pod 优先级信息。
	PriorityClassLister schedulingv1.PriorityClassLister

	// maintainer (维护器) 负责执行一些周期性的维护任务，如清理过期数据、更新统计信息等。
	maintainer *schemaintainer.SchedulerMaintainer
	// shuffler (节点打乱器) 负责定期打乱节点列表，以增加调度的随机性或公平性（如果启用了相关特性）。
	shuffler   *nodeshuffler.NodeShuffler

	// reconciler (对账器) 负责确保集群的实际状态与期望状态一致，例如，重新调度失败的 Pod 或处理节点状态变化。
	reconciler *reconciler.PodStateReconciler

	// SchedulerName 是 Godel 调度器的名称。Dispatcher 使用此名称来选择需要由 Godel 调度器处理的 Pod
	// （通过 Pod 的 schedulerName 字段）并过滤掉不由其处理的 Pod。
	SchedulerName string

	// recorder 用于向 Kubernetes API Server 发送事件，记录调度过程中的重要信息或错误。
	recorder events.EventRecorder
}
// logPodInfo 使用 klog 记录 Pod 的关键信息
func logPodInfo(pod *v1.Pod, message string) {
	// 使用 klog.InfoS 记录结构化日志
	// message 是日志的主要描述信息
	// 后续的参数以 "key", value 的形式传入，会被格式化为 key="value"
	klog.InfoS(message,
		"podName", pod.Name,                    // Pod 名称
		"podNamespace", pod.Namespace,          // Pod 命名空间
		"podUID", string(pod.UID),              // Pod 唯一标识符 (UID)
		"phase", string(pod.Status.Phase),      // Pod 当前状态阶段 (Pending, Running, Succeeded, Failed, Unknown)
		"nodeName", pod.Spec.NodeName,          // Pod 被调度到的节点名称 (如果已调度)
		"schedulerName", pod.Spec.SchedulerName, // Pod 指定的调度器名称
		"qosClass", string(pod.Status.QOSClass), // Pod 的服务质量等级 (Guaranteed, Burstable, BestEffort)
		"restartPolicy", string(pod.Spec.RestartPolicy), // Pod 的重启策略
		"hostNetwork", pod.Spec.HostNetwork,    // 是否使用主机网络
		"hostPID", pod.Spec.HostPID,            // 是否使用主机 PID 命名空间
		"hostIPC", pod.Spec.HostIPC,            // 是否使用主机 IPC 命名空间
		"creationTimestamp", pod.CreationTimestamp.Time, // Pod 创建时间
		// 可以根据需要添加更多字段，例如：
		// "labels", pod.Labels,                 // Pod 标签 (注意：如果标签很多，可能需要谨慎记录)
		// "annotations", pod.Annotations,       // Pod 注解 (注意：如果注解很多，可能需要谨慎记录)
		// "containerNames", getContainerNames(pod.Spec.Containers), // 容器名称列表 (需要辅助函数)
		// "initContainerNames", getContainerNames(pod.Spec.InitContainers), // Init 容器名称列表
		// "resourceRequests", getResourceRequests(pod.Spec.Containers), // 资源请求 (需要辅助函数)
		// "resourceLimits", getResourceLimits(pod.Spec.Containers),     // 资源限制 (需要辅助函数)
	)
}

// New 创建并初始化一个 Dispatcher 实例。
// Dispatcher 是一个核心组件，负责协调 Pod 调度相关的各种操作，如排队、状态同步、节点管理等。
//
// 参数:
//   - stopCh: 一个只读的结构体通道 (<-chan struct{})，用于接收停止信号。
//             当通道关闭时，Dispatcher 应该开始执行清理和关闭操作。
//   - client: Kubernetes 标准客户端接口 (kubernetes.Interface)，
//             用于与 Kubernetes API 服务器进行交互（如创建、更新、删除资源）。
//   - crdClient: 自定义资源定义 (CRD) 客户端接口 (crdclient.Interface)，
//                用于与自定义资源（如 Godel Scheduler、NMNode 等）进行交互。
//   - podInformer: Pod Informer (coreinformers.PodInformer)，
//                  提供对 Pod 资源的缓存列表和事件监听功能。
//   - nodeInformer: Node Informer (coreinformers.NodeInformer)，
//                   提供对 Node 资源的缓存列表和事件监听功能。
//   - schedulerInformer: Scheduler Informer (schedulinginformer.SchedulerInformer)，
//                        提供对 Godel Scheduler 自定义资源的缓存列表和事件监听功能。
//   - nmNodeInformer: NMNode Informer (nodeinformer.NMNodeInformer)，
//                     提供对 NMNode 自定义资源的缓存列表和事件监听功能。
//   - podGroupInformer: PodGroup Informer (schedulinginformer.PodGroupInformer)，
//                       提供对 PodGroup 自定义资源的缓存列表和事件监听功能。
//   - priorityClassInformer: PriorityClass Informer (schedinformers.PriorityClassInformer)，
//                            提供对 PriorityClass 资源的缓存列表和事件监听功能。
//   - schedulerName: 该 Dispatcher 实例所关联的调度器的名称 (string)。
//   - recorder: 事件记录器 (events.EventRecorder)，
//               用于向 Kubernetes API 服务器记录事件。
//
// 返回值:
//   - *Dispatcher: 新创建并初始化的 Dispatcher 实例指针。
// New 创建并初始化一个新的 Dispatcher 实例
// Dispatcher 是调度系统的核心组件，负责管理 Pod 的调度队列、状态同步和调度决策
func New(
	stopCh <-chan struct{}, // 用于接收停止信号的只读通道
	client kubernetes.Interface, // Kubernetes 标准客户端，用于与 API 服务器交互
	crdClient crdclient.Interface, // Godel CRD 客户端，用于操作自定义资源
	podInformer coreinformers.PodInformer, // Pod Informer，监听 Pod 变化
	nodeInformer coreinformers.NodeInformer, // Node Informer，监听 Node 变化
	schedulerInformer schedulinginformer.SchedulerInformer, // Scheduler Informer，监听 Scheduler CRD 变化
	schedulerName string, // 调度器名称
	recorder events.EventRecorder, // 事件记录器，用于记录调度相关事件
) *Dispatcher {
	// 注册 Dispatcher 相关的指标 (metrics)，用于监控。
	metrics.Register()

	// 创建调度器维护器 (Scheduler Maintainer)，用于管理 Scheduler CRD 的状态。
	maintainer := schemaintainer.NewSchedulerMaintainer(crdClient, schedulerInformer.Lister())

	// // 创建节点洗牌器 (Node Shuffler)，可能用于节点打散或特定的节点选择策略。
	// shuffler := nodeshuffler.NewNodeShuffler(client, crdClient, nodeInformer.Lister(), nmNodeInformer.Lister(), schedulerInformer.Lister(), maintainer)

	// 初始化 Dispatcher 结构体实例。
	dispatcher := &Dispatcher{
		// 停止信号通道
		StopEverything: stopCh,
		// Kubernetes 标准客户端
		client: client,
		// Pod 列表器 (Lister)，提供缓存的 Pod 数据访问
		podLister: podInformer.Lister(),
		// 存储 Pod 单元信息的结构
		UnitInfos: queue.NewUnitInfos(recorder),
		// 存储所有者信息的结构 (例如，Pod 属于哪个 ReplicaSet/Deployment)
		OwnerInfos: store.NewOwnerInfo(),
		// FIFO 队列，用于存储等待调度的 Pod (pending 状态)
		FIFOPendingPodsQueue: queue.NewPendingFIFO(metrics.NewPendingPodsRecorder("pending")),
		// FIFO 队列，用于存储已准备好调度的 Pod (ready 状态)
		SortedPodsQueue: queue.NewSortedFIFO(metrics.NewPendingPodsRecorder("ready")),
		// 存储调度信息的结构
		DispatchInfo: store.NewDispatchInfo(),
		// 调度器列表器 (Lister)，提供缓存的 Scheduler CRD 数据访问
		SchedulerLister: schedulerInformer.Lister(),

		// 调度器维护器
		maintainer: maintainer,
		// 节点洗牌器
		shuffler: nil,
		// 调度器名称
		SchedulerName: schedulerName,

		// Node 列表器 (Lister)
		NodeLister: nodeInformer.Lister(),
		// NMNode 列表器 (Lister)
		NMNodeLister: nil,
		// PodGroup 列表器 (Lister)
		PodGroupLister: nil,
		// PriorityClass 列表器 (Lister)
		PriorityClassLister: nil,

		// 事件记录器
		recorder: recorder,
	}

	// 创建 Pod 状态协调器 (Pod State Reconciler)，用于同步 Pod 在 API 服务器和 Dispatcher 内部状态之间的差异。
	reconciler := reconciler.NewPodStateReconciler(client, podInformer.Lister(), nodeInformer.Lister(),
		schedulerInformer.Lister(), schedulerName, dispatcher.DispatchInfo, maintainer)

	// 将协调器实例赋值给 Dispatcher
	dispatcher.reconciler = reconciler

	// 为各种 Informer 添加事件处理器 (Event Handlers)。
	// 这些处理器会监听 Pod、Scheduler、Node、NMNode、PodGroup 等资源的变化，
	// 并触发 Dispatcher 内部相应的逻辑（如更新队列、更新状态等）。
	AddAllEventHandlers(dispatcher, podInformer, schedulerInformer, nodeInformer)

	// 启动一个 Goroutine，监听 StopEverything 通道。
	// 当接收到停止信号时，关闭两个 Pod 队列 (FIFOPendingPodsQueue 和 SortedPodsQueue)，
	// 以防止新的 Pod 被添加到队列中，并允许正在处理的逻辑优雅退出。
	go func() {
		<-dispatcher.StopEverything
		dispatcher.FIFOPendingPodsQueue.Close()
		dispatcher.SortedPodsQueue.Close()
	}()

	// 返回初始化完成的 Dispatcher 实例
	return dispatcher
}

// Run 启动 Dispatcher 组件的主运行循环。
// 它会启动多个后台 goroutine 来处理不同的任务，如单元信息管理、Pod 分发、绑定、维护、节点打乱、待处理队列和对账等。
// 该函数会阻塞直到传入的上下文 (ctx) 被取消。
func (d *Dispatcher) Run(ctx context.Context) {
	// TODO: move to policy manager
	// 启动 UnitInfos (单元信息管理器) 的运行循环。
	// UnitInfos 可能负责管理调度单元 (Unit) 的状态和信息。
	// d.StopEverything 是一个信号通道，用于通知 UnitInfos 停止运行。
	//这个是最先执行的，从内部一个jianzhi
	// go d.UnitInfos.Run(d.StopEverything)

	// TODO: sending sorted pods to scheduler in parallel if necessary
	// TODO: adaptive worker threads count
	// 启动 sortedLoop (排序循环)。
	// 该循环可能负责从队列中获取待调度的 Pod，对其进行排序（例如，基于优先级、资源需求等），
	// 然后将排序后的 Pod 分发给调度器。
	// 使用 wait.UntilWithContext 确保循环在 ctx 取消时停止。
	go wait.UntilWithContext(ctx, d.sortedLoop, 0)

	/*
		// 这些循环是旧的或替代的实现，目前被注释掉了。
		// dispatchLoop (分发循环): 可能负责将 Pod 分发给具体的调度器实例。
		go wait.UntilWithContext(ctx, d.dispatchLoop, 0)
		// bindLoop (绑定循环): 可能负责处理 Pod 与节点的绑定操作。
		go wait.UntilWithContext(ctx, d.bindLoop, 0)
	*/

	// 启动 maintainer (维护器) 的运行循环。
	// Maintainer 可能负责执行一些周期性的维护任务，如清理过期数据、更新统计信息等。
	// d.StopEverything 用于通知 maintainer 停止。
	go d.maintainer.Run(d.StopEverything)

	// 启动 pendingLoop (待处理循环)。
	// 该循环可能负责处理处于待处理 (pending) 状态的调度单元 (Units)。
	// 使用 wait.UntilWithContext 确保循环在 ctx 取消时停止。
	//第三步：从pending队列调度到sorted队列
	go wait.UntilWithContext(ctx, d.pendingLoop, 0)

	// 启动 pendingUnitPodsLoop (待处理单元Pod循环)。
	// 该循环可能负责处理属于待处理单元的 Pod。
	// 使用 wait.UntilWithContext 确保循环在 ctx 取消时停止。
	//第二步：从unitInfo调度到pending队列
	go wait.UntilWithContext(ctx, d.pendingUnitPodsLoop, 0)

	// 启动 reconciler (对账器) 的运行循环。
	// Reconciler 可能负责确保集群的实际状态与期望状态一致，例如，重新调度失败的 Pod 或处理节点状态变化。
	// d.StopEverything 用于通知 reconciler 停止。
	go d.reconciler.Run(d.StopEverything)
}

// pendingUnitPodsLoop 是一个持续运行的循环，负责从 UnitInfos 中取出已就绪的 Pod，
// 并将它们添加到 FIFOPendingPodsQueue 中，以便进行后续的调度处理
func (d *Dispatcher) pendingUnitPodsLoop(ctx context.Context) {
	// 定义工作函数，执行单次循环任务
	workFunc := func() bool {
		// 从 UnitInfos 中弹出一个 Pod 信息
		// UnitInfos 会跟踪哪些 Pod 单元已经准备好被调度
		podInfo, err := d.UnitInfos.Pop()
		if err != nil {
			// 如果弹出操作失败，记录错误信息并返回 true 表示需要退出循环
			klog.InfoS("待处理单元 Pod 循环失败", "err", err)
			return true
		}
		// 记录调试信息，显示从 UnitInfos 就绪队列中弹出的 Pod
		klog.InfoS("DEBUG: 从单元信息就绪队列中弹出 Pod,并添加到PendingPodsQueue中", "pod", podInfo.PodKey)
		// 将弹出的 Pod 信息添加到 FIFO 待处理 Pod 队列中
		// 这个队列是调度器的主要输入队列，调度器会从这里获取需要调度的 Pod
		d.FIFOPendingPodsQueue.AddPodInfo(podInfo)
		// 返回 false 表示循环应该继续执行
		return false
	}

	// 持续运行循环，直到 workFunc 返回 true（表示需要退出）
	for {
		if quit := workFunc(); quit {
			// 当需要退出时，记录关闭信息并返回
			klog.InfoS("关闭待处理单元 Pod 循环工作器")
			return
		}
	}
}

// pendingLoop 是 Dispatcher 的一个核心处理循环，专门负责处理待处理 (Pending) 的 Pod。
// 它从 FIFO 待处理队列 (FIFOPendingPodsQueue) 中取出 Pod，
// 执行一些预处理（如记录指标、传递追踪上下文），然后将它们添加到已排序队列 (SortedPodsQueue) 中，
// 等待后续的 sortedLoop 或其他分发逻辑进行处理。
// 这个函数通常由 wait.UntilWithContext 在一个 goroutine 中周期性调用。
// 从 FIFO 待处理队列中取出一批 Pod 信息 (PodInfo)。
func (d *Dispatcher) pendingLoop(ctx context.Context) {
	// 这个操作通常是阻塞的，直到队列中有 Pod 或者上下文被取消。
	podInfos, err := d.FIFOPendingPodsQueue.Pop()
	klog.Info("现在从FIFOPendingPodsQueue取出一个pod")
	if err != nil {
		// 如果从队列弹出失败（例如队列已关闭或上下文取消），记录一个信息日志并返回。
		// "BestEffort" 表明这是一个尽力而为的操作，失败时不会重试。
		klog.InfoS("BestEffort pending queue pop failed", "err", err)
		return
	}

	// 遍历从队列中取出的所有 Pod 信息。
	for _, podInfo := range podInfos {
		klog.Info("podUnit可能代表的是一个pod单元（即组团调度）")
		// 将处理过的 PodInfo 添加到已排序队列 (SortedPodsQueue)。
		// 这个队列中的 Pod 通常已经准备好被分发给具体的调度器实例。
		d.SortedPodsQueue.AddPodInfo(podInfo)
		klog.Info("现在将pendingpod添加到sortedPodQueue中")
	}
}

// dispatchingPod 是 Dispatcher 用于异步处理单个 Pod 分发逻辑的函数。
// 它负责选择一个合适的调度器实例，并将 Pod 信息发送给该调度器进行调度。
// 这个函数通常在 sortedLoop 中通过 go routine 调用，以避免阻塞主循环。
func (d *Dispatcher) dispatchingPod(ctx context.Context, podInfo *queue.QueuedPodInfo, originalQueue queue.SortedQueue) {
	// 从 PodInfo 中提取 Pod 的 namespace 和 name。
	// PodKey 通常是 "namespace/name" 的格式。
	namespace, name, err := cache.SplitMetaNamespaceKey(podInfo.PodKey)
	if err != nil {
		// 如果无法解析 PodKey，记录错误日志。
		klog.InfoS("Failed to split the Meta Namespace Key", "pod", podInfo.PodKey, "err", err)
		// 将 PodInfo 重新加入到原始队列 (originalQueue) 中，等待后续处理。
		originalQueue.AddPodInfo(podInfo)
		return
	}

	// 从本地缓存 (podLister) 中获取最新的 Pod 对象。
	pod, err := d.podLister.Pods(namespace).Get(name)
	if apierrs.IsNotFound(err) || pod.DeletionTimestamp != nil ||
		!podutil.PendingPodOfGodel(pod, d.SchedulerName) {
		// 检查几种情况：
		// 1. Pod 已被删除 (apierrs.IsNotFound(err))
		// 2. Pod 正在被删除 (pod.DeletionTimestamp != nil)
		// 3. Pod 不再处于待调度状态或不再属于当前 Godel 调度器负责 (podutil.PendingPodOfGodel)
		// 在这些情况下，认为 Pod 已无效，直接返回，不再重新排队。
		return
	}
	klog.Info("获取一个pod对象从本地中")
	logPodInfo(pod, "获取pod对象的信息包括")

	// 调用 d.selectScheduler 函数，根据 Pod 的信息和当前可用的调度器状态，选择一个合适的调度器名称。
	klog.Info("准备为该pod寻找一个调度器")
	schedulerName, err := d.selectScheduler(pod)
	if err != nil {
		// 如果选择调度器失败，记录错误日志。
		klog.InfoS("Failed to select the scheduler", "err", err)
		// 将 PodInfo 重新加入到原始队列 (originalQueue) 中，等待后续处理。
		originalQueue.AddPodInfo(podInfo)
		return
	}

	// 调用 d.sendPodToScheduler 函数，将 Pod 信息发送给选定的调度器实例。
	klog.InfoS("选择的schedulerName为","schedulerName",schedulerName)
	if err := d.sendPodToScheduler(pod, podInfo, schedulerName); err != nil {
		// 如果发送 Pod 失败，记录错误日志。
		// TODO 注释提到需要解析错误类型，以避免因 "resource too old" 等错误导致 Pod 在队列中来回重入 (ping-pong)。
		klog.InfoS("Failed to send pod to the scheduler", "err", err)
		// 避免将已经被删除的 Pod 重新加入队列。
		// 如果错误不是 "not found" (意味着 Pod 可能确实存在但更新失败)，则将其重新加入队列。
		if !apierrs.IsNotFound(err) {
			// 将 PodInfo 重新加入到原始队列 (originalQueue) 中，等待后续处理。
			originalQueue.AddPodInfo(podInfo)
		}
		return
	}
}

// sortedLoop 是 Dispatcher 的一个核心处理循环，专门负责处理已排序的 Pod。
// 它从已排序队列 (SortedPodsQueue) 中取出 Pod，
// 执行一些预处理（如记录指标、传递追踪上下文），然后启动一个 goroutine 
// (通过调用 d.dispatchingPod) 来异步地将 Pod 分发给合适的调度器实例。
// 这个函数通常由 wait.UntilWithContext 在一个 goroutine 中周期性调用。
// 注意：此循环本身不阻塞等待队列中的 Pod，如果队列为空，它会立即返回并再次循环（取决于 SortedPodsQueue.PopPodInfo 的具体实现是否阻塞）。
func (d *Dispatcher) sortedLoop(ctx context.Context) {
	// 进入无限循环，持续处理队列中的 Pod。
	for {
		// 从已排序队列 (SortedPodsQueue) 中取出一个 Pod 信息 (PodInfo)。
		// 这个操作可能根据队列实现是阻塞的（直到有 Pod 可取）或非阻塞的（立即返回，可能为 nil）。
		if podInfo, _ := d.SortedPodsQueue.PopPodInfo(); podInfo != nil {
			klog.Info("现在从SortedPodsQueue取出一个pod")
			// **关键步骤**: 启动一个 goroutine 来异步执行 Pod 的分发逻辑。
			// 这样可以避免在此循环中阻塞，允许循环继续处理队列中的下一个 Pod。
			// TODO 注释提到移除 goroutine 可能对性能有影响，需要评估。
			klog.Info("用协程来进行一次分发")
			go d.dispatchingPod(ctx, podInfo, d.SortedPodsQueue)

		}
		// 如果 d.SortedPodsQueue.PopPodInfo() 返回了 nil (表示队列为空，或者上下文已取消等)，
		// 循环会立即回到 for 的开头，再次尝试 PopPodInfo。
		// 如果 PopPodInfo 是阻塞的，则循环会在 PopPodInfo 调用处等待，直到队列中有新 Pod。
	}
}

func (d *Dispatcher) dispatchLoop(ctx context.Context) {
	// TODO: figure out what we can do if schedulers go down
}

func (d *Dispatcher) bindLoop(ctx context.Context) {
	// TODO: figure out what we can do if binders go down
}

func (d *Dispatcher) getAssignedSchedulerFromPods(pg *scheduling.PodGroup) (string, error) {
	// construct selector

	selector := labels.Set(map[string]string{
		podutil.PodGroupNameAnnotationKey: pg.Name,
	}).AsSelector()
	pods, err := d.podLister.Pods(pg.Namespace).List(selector)
	if err != nil {
		return "", err
	}

	for _, p := range pods {
		if p.Annotations != nil && p.Annotations[podutil.SchedulerAnnotationKey] != "" {
			return p.Annotations[podutil.SchedulerAnnotationKey], nil
		}
	}
	return "", nil
}

func (d *Dispatcher) getAssignedScheduler(pg *scheduling.PodGroup) (string, error) {
	cachedSched := d.UnitInfos.GetAssignedSchedulerForPodGroupUnit(pg)
	if cachedSched != "" {
		return cachedSched, nil
	}

	// in case of master/backup switch for HA, we will try to get the assigned
	// scheduler name from dispatched/assumed pods.
	schedName, err := d.getAssignedSchedulerFromPods(pg)
	if err != nil {
		return "", err
	}
	if schedName != "" {
		// update the dispatch info cache
		if err := d.UnitInfos.AssignSchedulerToPodGroupUnit(pg, schedName, false); err != nil {
			return schedName, err
		}
	}

	return schedName, nil
}

// selectSchedulerForUnit selects a secheduler for the podgroup, if the
// dispatcher has already assigned a scheduler to the podgroup, then returns
// the existing one.
func (d *Dispatcher) selectSchedulerForUnit(pg *scheduling.PodGroup, pod *v1.Pod, podOwner string) (string, error) {
	schedName, err := d.getAssignedScheduler(pg)
	if err != nil {
		return "", err
	}
	if schedName != "" && d.maintainer.SchedulerExist(schedName) && d.maintainer.IsSchedulerInActiveQueue(schedName) {
		d.DispatchInfo.AddPodInAdvance(pod, schedName)
		if utilfeature.DefaultFeatureGate.Enabled(features.SupportRescheduling) {
			d.OwnerInfos.AddDispatchedUnboundPod(pod, schedName)
		}
		return schedName, nil
	}

	forceUpdate := false
	if len(schedName) != 0 {
		// previous scheduler name for this unit is not empty, but that scheduler is inactive or deleted
		// we need to reset the scheduler name for this unit forcefully
		forceUpdate = true
	}
	klog.V(4).InfoS("Selected a new scheduler for the podGroup", "podGroup", klog.KObj(pg))
	// select a scheduler for the first dispatchable pod of the PodGroup.
	schedName, err = d.pickScheduler(pod)
	if err != nil {
		return "", err
	}
	// store the assigned scheduler in the unit info cache
	err = d.UnitInfos.AssignSchedulerToPodGroupUnit(pg, schedName, forceUpdate)
	return schedName, err
}

// selectScheduler 根据 Pod 的信息选择一个合适的调度器名称。
// 它首先检查 Pod 是否属于一个 PodGroup (调度单元)。
// 如果属于 PodGroup，则调用 d.selectSchedulerForUnit 获取为该单元分配的调度器。
// 如果不属于任何 PodGroup，则调用 d.pickScheduler 根据 Pod 本身的属性选择一个调度器。
// 返回选定的调度器名称和可能发生的错误。
func (d *Dispatcher) selectScheduler(pod *v1.Pod) (string, error) {
	
	klog.Info("正常使用的pod应该不需要podGroup，现在开始pickScheduler")
	// 如果 Pod 不属于任何 PodGroup，则调用通用的调度器选择函数。
	// 这个函数可能会根据 Pod 的标签、优先级、资源请求等信息来选择一个合适的调度器。
	return d.pickScheduler(pod)
}

// pickScheduler 为不属于任何 PodGroup (调度单元) 的 Pod 选择一个合适的调度器名称。
// 它根据是否启用了 "SupportRescheduling" 特性门控来决定使用哪种选择策略。
func (d *Dispatcher) pickScheduler(pod *v1.Pod) (string, error) {
	klog.Info("不打算启动基于owner的机制，直接使用loadBalancing")
	// 如果未启用 "SupportRescheduling" 特性，则调用 d.loadBalancing 函数。
	// 此函数会根据负载均衡策略（例如，选择负载最低的调度器实例）来选择一个调度器。
	return d.loadBalancing(pod)
}

func (d *Dispatcher) selectSchedulerBasedOnOwner(pod *v1.Pod) (string, error) {
	schedulerName := d.OwnerInfos.SelectSchedulerAndSetDispatchedUnboundPod(pod)
	if schedulerName != "" {
		d.DispatchInfo.AddPodInAdvance(pod, schedulerName)
		return schedulerName, nil
	}

	schedulerName, err := d.loadBalancing(pod)
	if err == nil && schedulerName != "" {
		gotSchedulerName := d.OwnerInfos.SetDispatchedUnboundPod(pod, schedulerName)
		if gotSchedulerName != schedulerName {
			d.DispatchInfo.UpdatePodInAdvance(pod, gotSchedulerName)
		}
		return gotSchedulerName, nil
	}

	return schedulerName, err
}

// loadBalancing 根据负载均衡策略为 Pod 选择一个合适的调度器名称。
// 它尝试找到当前最空闲（待处理 Pod 数量最少）的调度器，并将当前 Pod 预先分配给它。
// 这是一种 "抢占式" 的负载均衡，旨在避免多个 Pod 同时被分配给同一个调度器导致其负载过高。
func (d *Dispatcher) loadBalancing(pod *v1.Pod) (string, error) {
	// 调用 d.DispatchInfo.GetMostIdleSchedulerAndAddPodInAdvance 方法。
	// 此方法会:
	// 1. 扫描所有已注册的调度器实例。
	// 2. 根据其当前待处理的 Pod 数量（或其他负载指标）找到最空闲的一个。
	// 3. 将当前的 Pod 预先计入该调度器的待处理队列（在 Dispatcher 级别进行记录，以实现"预占"效果）。
	// 4. 返回该调度器的名称。
	klog.Info("即将启动寻找负载均衡的调度器的函数")
	schedulerName := d.DispatchInfo.GetMostIdleSchedulerAndAddPodInAdvance(pod)

	// 检查返回的调度器名称是否为空。
	if len(schedulerName) == 0 {
		// 如果没有找到任何注册的调度器，或者所有调度器都已满载且无法预占，
		// 则返回一个错误，表示无法为 Pod 选择调度器。
		return "", fmt.Errorf("no scheduler registered")
	} else {
		// 如果成功获取到调度器名称，则返回该名称。
		return schedulerName, nil
	}
}

// sendPodToScheduler 通过 Patch 操作将 Pod 发送到指定的调度器
// 该函数会修改 Pod 的注解，标记其目标调度器、状态，并更新调度追踪上下文，然后将这些修改持久化到 API Server
func (d *Dispatcher) sendPodToScheduler(pod *v1.Pod, podInfo *queue.QueuedPodInfo, schedulerName string) (err error) {
	// 创建 Pod 的深拷贝，避免修改原始对象
	podCopy := pod.DeepCopy()
	
	// 确保 Pod 的注解映射存在
	if podCopy.Annotations == nil {
		podCopy.Annotations = make(map[string]string)
	}
	
	// 设置目标调度器名称注解，告知 kubelet 或其他组件该 Pod 应由哪个调度器处理
	podCopy.Annotations[podutil.SchedulerAnnotationKey] = schedulerName
	
	// 设置 Pod 状态为 "Dispatched"，表示该 Pod 已被 Dispatcher 分配给调度器，正在等待调度
	podCopy.Annotations[podutil.PodStateAnnotationKey] = string(podutil.PodDispatched)
	


	// 记录调试信息，显示开始发送 Pod 到调度器
	klog.InfoS("Started to send the pod to scheduler", "pod", klog.KObj(pod), "schedulerName", schedulerName)
	
	// 通过 Patch 操作将修改后的注解应用到 API Server 上的 Pod 对象
	err = util.PatchPod(d.client, pod, podCopy)
	if err != nil {
		// 如果 Patch 操作失败，记录错误日志
		klog.ErrorS(err, "Fail to patch pod", "pod", klog.KObj(pod))
		// 从 Dispatcher 的已调度 Pod 存储中移除该 Pod 信息，因为它实际上并未成功发送
		d.DispatchInfo.RemovePod(podCopy)
		// 如果启用了重新调度特性，则同时从所有者信息存储中移除该 Pod
		if utilfeature.DefaultFeatureGate.Enabled(features.SupportRescheduling) {
			d.deletePodFromOwnerInfo(podCopy)
		}
	}
	// 返回 Patch 操作的错误结果（如果有的话）
	return err
}
