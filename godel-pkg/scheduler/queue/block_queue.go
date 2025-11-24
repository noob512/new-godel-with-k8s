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

package queue

import (
	"fmt"
	"sync"
	"time"

	"github.com/kubewharf/godel-scheduler-api/pkg/client/listers/scheduling/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	schedulingv1 "k8s.io/client-go/listers/scheduling/v1"
	"k8s.io/klog/v2"

	framework "k8s.io/kubernetes/godel-pkg/framework/api"
	"k8s.io/kubernetes/godel-pkg/framework/utils"
	"k8s.io/kubernetes/godel-pkg/scheduler/cache"
	"k8s.io/kubernetes/godel-pkg/scheduler/metrics"
	godelutil "k8s.io/kubernetes/godel-pkg/util"
	"k8s.io/kubernetes/godel-pkg/util/heap"
	metricsutil "k8s.io/kubernetes/godel-pkg/util/metrics"
	unitstatus "k8s.io/kubernetes/godel-pkg/util/unitstatus"
)

// BlockQueue implements a scheduling queue.
// TODO(lintong.jiang): LOG - revisit the logging messages in this file
type BlockQueue struct {
	lock  sync.RWMutex
	cond  sync.Cond
	stop  chan struct{}
	clock godelutil.Clock

	qos, subCluster string

	latestOperationTimestamp time.Time
	// closed indicates that the queue is closed.
	// It is mainly used to let Pop() exit its control loop while waiting for an item.
	closed bool
	// boHandler handle anything about backoff calculation
	boHandler       *backoffHandler
	metricsRecorder *metricsRecorder

	cache    cache.SchedulerCache
	pcLister schedulingv1.PriorityClassLister
	pgLister v1alpha1.PodGroupLister

	// waitingPodsList stores all the pods that can't create unit for now.
	waitingPodsList SubQueue

	// 1. ReadyQueue
	// readyQ is a heap structure that scheduler actively looks at to find units to
	// schedule. Head of heap is the highest priority unit.
	readyQ SubQueue

	// 2. WaitingQueue
	// waitingQ holds the units that doesn't have enough pods (such as podgroup MimMember).
	waitingQ SubQueue

	// schedulingCycle represents sequence number of scheduling cycle and is incremented
	// when a pod is popped.
	schedulingCycle int64
	// moveRequestCycle caches the sequence number of scheduling cycle when we
	// received a move request. Unscheduable pods in and before this scheduling
	// cycle will be put back to activeQueue if we were trying to schedule them
	// when we received move request.
	moveRequestCycle int64
}

// Making sure that BlockQueue implements SchedulingQueue.
var _ SchedulingQueue = &BlockQueue{}

// NewBlockQueue creates a BlockQueue object.
func NewBlockQueue(
	cache cache.SchedulerCache,
	pcLister schedulingv1.PriorityClassLister,
	pgLister v1alpha1.PodGroupLister,
	lessFn framework.UnitLessFunc,
	opts ...Option,
) *BlockQueue {
	options := defaultPriorityQueueOptions // TODO: fixme
	for _, opt := range opts {
		opt(&options)
	}

	comp := func(unitInfo1, unitInfo2 interface{}) bool {
		u1 := unitInfo1.(*framework.QueuedUnitInfo)
		u2 := unitInfo2.(*framework.QueuedUnitInfo)
		return lessFn(u1, u2)
	}
	boHandler := newBackoffHandler(options.unitInitialBackoffDuration, options.unitMaxBackoffDuration, options.clock)
	qos := metricsutil.SwitchTypeToQos(options.switchType)

	pq := &BlockQueue{
		stop:  make(chan struct{}),
		clock: options.clock,

		qos:        qos,
		subCluster: options.subCluster,

		cache:           cache,
		pcLister:        pcLister,
		pgLister:        pgLister,
		boHandler:       boHandler,
		metricsRecorder: newMetricsRecorder(),

		waitingPodsList: newWaitingPodsList(qos, options.subCluster, options.owner, options.clock),
		readyQ:          heap.NewWithRecorder("ready", unitInfoKeyFunc, comp, metrics.NewPendingUnitsRecorder("ready")),
		waitingQ:        heap.NewWithRecorder("waiting", unitInfoKeyFunc, alwaysFalse, metrics.NewPendingUnitsRecorder("waiting")),

		moveRequestCycle: -1,
	}
	pq.cond.L = &pq.lock
	pq.latestOperationTimestamp = pq.clock.Now()

	return pq
}

// Run starts the goroutine to pump from podBackoffQ to activeQ
// Run 启动 BlockQueue 的后台维护任务，通过多个定时协程（goroutine）周期性执行关键清理与触发逻辑。
// 该方法是非阻塞的，所有任务均在独立的 goroutine 中运行，由 p.stop 通道控制生命周期。
func (p *BlockQueue) Run() {
	klog.Info("启动BlockQueue")
	// 1. 启动一个每秒执行一次的协程，检查是否需要触发广播（例如通知调度器有新 Pod 可调度）。
	//    这通常用于避免频繁唤醒调度器，仅在满足条件时才“触发”调度流程。
	go wait.Until(p.triggerBroadcastIfNeeded, 1.0*time.Second, p.stop)

	// 2. 启动一个每 2 秒执行一次的协程，清理 waitingQ（等待队列）中已完成调度的 Unit。
	//    这些 Unit 可能已成功绑定或被删除，需及时移除以释放内存并保持队列整洁。
	// TODO: 团队需讨论是否移除此逻辑——可能因为调度模型演进，该清理逻辑已冗余或由其他组件处理。
	go wait.Until(p.flushScheduledUnitsInWaitingQ, 2.0*time.Second, p.stop)

	// 3. 启动一个每 20 秒执行一次的协程，清理 waitingPodsList 中无效或过期的 Pod 记录。
	//    这有助于防止因 Pod 删除、超时或状态变更导致的残留数据堆积。
	// TODO: 同样需要评估该逻辑是否仍必要，尤其在引入更健壮的事件驱动清理机制后。
	go wait.Until(p.flushWaitingPodsList, 20.0*time.Second, p.stop)
}

// triggerBroadcastIfNeeded
// triggerBroadcastIfNeeded 检查 BlockQueue 的就绪队列（readyQ）中是否有可调度的单元（Unit），
// 若存在且未处于退避（backoff）状态，则触发条件变量广播（Broadcast），
// 通知等待中的调度器协程有新的可调度任务，同时记录相关指标。
// 该方法由后台定时任务（如每秒）调用，用于高效驱动调度循环。
func (p *BlockQueue) triggerBroadcastIfNeeded() {
	// 获取锁，确保对 readyQ 和相关状态的访问是线程安全的。
	p.lock.Lock()
	defer p.lock.Unlock()

	// 查看 readyQ 队列头部（不弹出），获取下一个待处理的调度单元。
	rawUnit := p.readyQ.Peek()
	if rawUnit == nil {
		// 若队列为空，无需广播，直接返回。
		return
	}

	// 类型断言：将 interface{} 转换为具体的 *framework.QueuedUnitInfo。
	// QueuedUnitInfo 通常封装了 Pod/Unit 的元数据、QoS、调度状态等信息。
	unitInfo := rawUnit.(*framework.QueuedUnitInfo)

	// 检查该单元是否正处于退避状态（例如因调度失败而暂时禁止重试）。
	// 如果在退避中，则不应触发调度，跳过本次广播。
	if p.boHandler.isUnitBackoff(unitInfo) {
		return
	}

	// 记录调试日志（详细级别 4），包含子集群、QoS 类别和 Unit 唯一标识，
	// 便于追踪调度触发时机和上下文。
	klog.InfoS("SchedulingQueue triggerBroadcastIfNeeded",
		"subCluster", p.subCluster,
		"qos", p.qos,
		"unitKey", unitInfo.UnitKey)

	// TODO: 后续需增强可观测性，例如添加更丰富的指标（metrics）和分布式追踪（tracing）。

	// 向指标记录器上报事件：该 Unit 已完成退避并进入可调度状态。
	// 参数说明：
	// - unitInfo: 调度单元信息
	// - p.readyQ.String(): 队列当前状态（如长度、类型等，用于分类指标）
	// - godelutil.BackoffComplete: 事件类型，表示“退避结束，可调度”
	p.metricsRecorder.recordIncoming(unitInfo, p.readyQ.String(), godelutil.BackoffComplete)

	// 关键操作：唤醒所有因等待条件变量（p.cond）而阻塞的调度器协程。
	// 调度器通常在 readyQ 为空时调用 p.cond.Wait() 挂起；
	// 此处 Broadcast 通知它们“现在有任务可处理”，从而恢复调度循环。
	p.cond.Broadcast()
}

// flushScheduledUnitsInWaitingQ Moves all units from waitingQ which have been scheduled to readyQ.
// This case occurs when the scheduler is processing the scheduling unit but does not end.
// At this time, the all-min pods come into queue. And they will be stored in waitingQ.
// All-min pods (as a new unit) must be popped when scheduler schedule the previous unit successfully.
// TODO: discuss whether it would be better to move the unit directly while UpdateSchedulingUnitStatus
// instead of using the following loop.
// flushScheduledUnitsInWaitingQ 刷新等待队列（Waiting Queue）中已准备好被调度的调度单元。
// 该函数会遍历等待队列中的所有调度单元，检查它们是否已准备好被调度，
// 如果准备好，则将这些单元从等待队列移动到就绪队列（Ready Queue）中，
// 以便调度器可以立即尝试调度这些单元。
// 这是 BlockQueue 确保等待单元能够及时响应集群状态变化并进入调度流程的关键方法。
func (p *BlockQueue) flushScheduledUnitsInWaitingQ() {
	// 获取队列锁，确保并发安全
	p.lock.Lock()
	defer p.lock.Unlock()
	
	// 创建一个切片用于存储需要移动的调度单元
	var unitsToMove []*framework.QueuedUnitInfo
	// 创建互斥锁，保护 unitsToMove 切片的并发访问
	var mutex sync.Mutex
	
	// 遍历等待队列中的所有调度单元
	p.waitingQ.Process(func(_ int, _ string, obj interface{}) {
		// 将接口类型转换为调度单元信息类型
		klog.Info("检查等待队列-第二个队列run")
		unitInfo := obj.(*framework.QueuedUnitInfo)
		
		// 检查该调度单元是否已准备好被调度
		if p.readyToBeScheduled(unitInfo) {
			// 如果已准备好被调度，添加到待移动列表中
			mutex.Lock()
			unitsToMove = append(unitsToMove, unitInfo)
			mutex.Unlock()
		}
	})
	
	// 处理所有已准备好被调度的调度单元
	for _, unit := range unitsToMove {
		// 从等待队列中删除该单元
		p.waitingQ.Delete(unit)
		
		// 将该单元添加到就绪队列中，准备进行调度
		p.readyQ.Add(unit)
		
		// 记录调度单元从等待队列转移到就绪队列的详细信息
		klog.InfoS("Flushed scheduling units in the BlockQueue", "subCluster", p.subCluster, "qos", p.qos, "unitKey", unit.UnitKey, "oldQueue", p.waitingQ, "newQueue", p.readyQ)
		
		// 记录调度单元的流入指标，标记为等待完成状态
		p.metricsRecorder.recordIncoming(unit, p.readyQ.String(), godelutil.WaitingComplete)
		
		// 通知所有等待的协程（如调度器主循环），队列状态已更新
		defer p.cond.Broadcast()
	}
}

// flushWaitingPodsList moves pods that can create ScheduleUnit.
// flushWaitingPodsList 刷新阻塞队列（BlockQueue）中等待的 Pod 列表，
// 将等待的 Pod 转换为调度单元并根据调度准备状态移动到相应的子队列中。
// 该函数会遍历等待列表中的所有 Pod，尝试将它们转换为调度单元，
// 然后根据单元是否准备好被调度，将其放入就绪队列或继续留在等待队列。
// 这是 BlockQueue 确保等待的 Pod 能够及时响应集群状态变化的关键方法。
func (p *BlockQueue) flushWaitingPodsList() {
	// 获取队列锁，确保并发安全
	p.lock.Lock()
	defer p.lock.Unlock()

	// 创建一个切片用于存储需要移动的调度单元
	var unitsToMove []*framework.QueuedUnitInfo
	// 创建互斥锁，保护 unitsToMove 切片的并发访问
	var mutex sync.Mutex
	
	// 遍历等待 Pod 列表中的所有项
	p.waitingPodsList.Process(func(_ int, _ string, obj interface{}) {
		// 将等待的 Pod 转换为调度单元
		// 该转换过程可能涉及 PodGroup 信息的获取和处理
		klog.Info("检查等待 Pod 列表-第三个run协程")
		unitInfo := obj.(*waitingPods).transferToPodGroupUnit(p.pcLister, p.pgLister, p.clock)
		if unitInfo != nil {
			// 如果成功转换为调度单元，添加到待移动列表中
			mutex.Lock()
			unitsToMove = append(unitsToMove, unitInfo)
			mutex.Unlock()
		}
	})
	
	// 创建变量用于存储目标队列
	var queue SubQueue
	
	// 处理所有需要移动的调度单元
	for _, unit := range unitsToMove {
		// 从等待列表中删除该单元
		p.waitingPodsList.DeleteByKey(unit.UnitKey)
		
		// 检查该单元是否准备好被调度
		if p.readyToBeScheduled(unit) {
			// 如果准备好被调度，放入就绪队列
			queue = p.readyQ
			// 通知所有等待的协程（如调度器主循环），队列状态已更新
			defer p.cond.Broadcast()
		} else {
			// 如果尚未准备好被调度，继续留在等待队列
			queue = p.waitingQ
		}
		
		// 将单元添加到目标队列
		queue.Add(unit)
		
		// 记录调度单元从等待列表转移到目标队列的详细信息
		klog.InfoS("Flushed scheduling units in the BlockQueue", "subCluster", p.subCluster, "qos", p.qos, "unitKey", unit.UnitKey, "oldQueue", p.waitingPodsList, "newQueue", queue)
		
		// 记录调度单元的流入指标，标记为创建单元成功状态
		p.metricsRecorder.recordIncoming(unit, queue.String(), godelutil.CreateUnitSucceed)
	}
}

// Close closes the priority queue.
func (p *BlockQueue) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()
	close(p.stop)
	p.closed = true
	p.cond.Broadcast()
}

func (p *BlockQueue) CanBeRecycle() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if !p.latestOperationTimestamp.Add(framework.RecycleExpiration).Before(p.clock.Now()) {
		return false
	}
	for _, queue := range []SubQueue{p.waitingPodsList, p.waitingQ, p.readyQ} {
		if queue.Len() > 0 {
			return false
		}
	}
	return true
}

// findAndRemoveUnitFromQueue will find the SubQueue where the unit is located and remove it from the SubQueue (if it exists).
// Unless unit is empty, we must ensure that unit is added back to a SubQueue after calling this function.
func (p *BlockQueue) findAndRemoveUnitFromQueue(unitKey string) (SubQueue, *framework.QueuedUnitInfo, bool) {
	for _, queue := range []SubQueue{p.waitingQ, p.readyQ} {
		if u, exist, _ := queue.GetByKey(unitKey); exist {
			// For the accuracy of metrics, delete the unit before modifying it.
			if err := queue.DeleteByKey(unitKey); err != nil {
				klog.V(4).InfoS("Error occurred when deleting unit in SchedulingQueue.findQueueForUnit", "subCluster", p.subCluster, "qos", p.qos, "unitKey", unitKey, "queue", queue, "err", err)
			}
			return queue, u.(*framework.QueuedUnitInfo), true
		}
	}
	return nil, nil, false
}

// addPodToUnitInfo return the SubQueue and the QueuedUnitInfo if unit info already exists in some queue,
// else returned SubQueue should be nil and QueuedUnitInfo will be the created one
func (p *BlockQueue) addPodToUnitInfo(pod *v1.Pod) (SubQueue, *framework.QueuedUnitInfo, error) {
	unitKey := utils.GetUnitIdentifier(pod)
	queue, unitInfo, exists := p.findAndRemoveUnitFromQueue(unitKey)
	if exists {
		return queue, unitInfo, unitInfo.AddPod(newQueuedPodInfo(pod, p.clock))
	}

	created, err := p.createUnitInfo(pod)
	return nil, created, err
}

func (p *BlockQueue) updatePodInUnitInfo(oldPod, newPod *v1.Pod) (SubQueue, *framework.QueuedUnitInfo, error) {
	unitKey := utils.GetUnitIdentifier(newPod)
	queue, unitInfo, exists := p.findAndRemoveUnitFromQueue(unitKey)
	if !exists {
		created, err := p.createUnitInfo(newPod)
		return nil, created, err
	}

	if oldPodInfo := unitInfo.GetPod(newQueuedPodInfoForLookup(oldPod)); oldPodInfo != nil {
		unitInfo.DeletePod(oldPodInfo)
		unitInfo.AddPod(updatePodInfo(oldPodInfo, newPod))
	} else {
		unitInfo.AddPod(newQueuedPodInfo(newPod, p.clock))
	}
	return queue, unitInfo, nil
}

func (p *BlockQueue) deletePodFromUnitInfo(pod *v1.Pod) (SubQueue, *framework.QueuedUnitInfo, error) {
	unitKey := utils.GetUnitIdentifier(pod)
	queue, unitInfo, exists := p.findAndRemoveUnitFromQueue(unitKey)
	if exists {
		return queue, unitInfo, unitInfo.DeletePod(newQueuedPodInfoForLookup(pod))
	}

	return nil, nil, nil
}

func (p *BlockQueue) createUnitInfo(pod *v1.Pod) (*framework.QueuedUnitInfo, error) {
	pInfo := newQueuedPodInfo(pod, p.clock)
	unitKey := utils.GetUnitIdentifier(pod)
	scheduleUnit, err := utils.CreateScheduleUnit(p.pcLister, p.pgLister, pInfo)
	if err != nil {
		// This means that we haven't got the relevant information about PodGroup or PriorityClass yet.
		return nil, fmt.Errorf("error occurred when create unit for pod %v/%v: %v", pod.Namespace, pod.Name, err)
	}
	if err = scheduleUnit.AddPod(pInfo); err != nil {
		return nil, fmt.Errorf("error occurred when adding pod to unit %v for pod %v/%v: %v", scheduleUnit.GetKey(), pod.Namespace, pod.Name, err)
	}

	// Since `PodGroup` and `Pod` are two different resource types, we can't ensure the order of their arrival.
	// If some pods come before their PodGroup, these pods will be stored in `waitingPodsList`. And then, after the PodGroup is created, there may
	// be a race between `CreateScheduleUnit` (getting PodGroup from local informer cache) and ActivePodGroupUnit (triggered by PodGroup events).
	// e.g.
	//  1. Some pods (PodSet1) come before their PodGroup, they will be stored in `waitingPodsList` since we will failed to create unit for them;
	//  2. And then PodGroup is created successfully and stored in local cache, but corresponding `Add` event is not captured by scheduler;
	//  3. Another several pods (PodSet2) come, these pods will be stored in `waitingQ` since we can successfully create unit for them;
	//  4. And then, PodGroup `Add` event is captured, we will execute `ActivePodGroupUnit`, PodSet2 will be overwritten by PodSet1 during this
	//     process which is not expected;
	// To avoid this, when we create unit successfully for the first time, we propose to check `waitingPodsList` and pick up the pods belonging
	// to the same unit.
	if waitingPodsObj, exist, _ := p.waitingPodsList.GetByKey(unitKey); exist {
		if waitingPodsObj != nil {
			waitingPods := waitingPodsObj.(*waitingPods)
			if err := scheduleUnit.AddPods(waitingPods.GetPods()); err != nil {
				return nil, fmt.Errorf("error occurred when adding waitingPodsList pods to unit %v for pod %v/%v: %v", scheduleUnit.GetKey(), pod.Namespace, pod.Name, err)
			}
		}
		p.waitingPodsList.DeleteByKey(unitKey)
	}
	queuedUnitInfo := framework.NewQueuedUnitInfo(unitKey, scheduleUnit, p.clock)
	return queuedUnitInfo, nil
}

func (p *BlockQueue) readyToBeScheduled(unitInfo *framework.QueuedUnitInfo) bool {
	if unitInfo == nil || unitInfo.ScheduleUnit == nil {
		return false
	}
	return unitInfo.ReadyToBePopulated() || p.cache.GetUnitSchedulingStatus(unitInfo.UnitKey) == unitstatus.ScheduledStatus
}

// Add adds a pod to the ready queue. It should be called only when a new pod
// is added so there is no chance the pod is already in waiting/ready/unschedulable/backoff queues
func (p *BlockQueue) Add(pod *v1.Pod) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.latestOperationTimestamp = p.clock.Now()

	queue, unitInfo, err := p.addPodToUnitInfo(pod)
	if err != nil {
		klog.V(4).InfoS("SchedulingQueue Add, add to waitingPodsList directly", "subCluster", p.subCluster, "qos", p.qos, "pod", klog.KObj(pod), "podUID", pod.GetUID(), "err", err)
		return p.waitingPodsList.Add(pod)
	}
	if queue != nil {
		// The unit has been removed from SubQueue in addPodToUnitInfo.
		klog.V(4).InfoS("SchedulingQueue Add, delete from oldQueue", "subCluster", p.subCluster, "qos", p.qos, "unitKey", unitInfo.UnitKey, "oldQueue", queue)
	}
	switch {
	case p.readyToBeScheduled(unitInfo):
		queue = p.readyQ
		defer p.cond.Broadcast()
	default:
		queue = p.waitingQ
	}
	klog.V(4).InfoS("SchedulingQueue Add, add to newQueue", "subCluster", p.subCluster, "qos", p.qos, "unitKey", unitInfo.UnitKey, "newQueue", queue)
	// TODO: improve metrics
	p.metricsRecorder.recordIncoming(unitInfo, queue.String(), godelutil.PodAdd)
	return queue.Add(unitInfo)
}

// AddUnschedulableIfNotPresent inserts a unit that cannot be scheduled into
// the queue, unless it is already in the queue. Normally, BlockQueue puts
// unschedulable units in `unschedulableQ`. But if there has been a recent move
// request, then the unit is put in `backoffQ`.
func (p *BlockQueue) AddUnschedulableIfNotPresent(unitInfo *framework.QueuedUnitInfo, _ int64) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.latestOperationTimestamp = p.clock.Now()

	if unitInfo == nil || unitInfo.NumPods() == 0 {
		return nil
	}
	var queue SubQueue
	queue, unitInfoInSchedulingQueue, exists := p.findAndRemoveUnitFromQueue(unitInfo.UnitKey)
	if exists {
		klog.V(4).InfoS("SchedulingQueue AddUnschedulable, exist same unit and will merge them", "unitKey", unitInfo.UnitKey, "oldQueue", queue)
		// add new coming pods into current unit
		unitInfo.AddPodsIfNotPresent(unitInfoInSchedulingQueue.GetPods()...)
	}
	// Refresh the timestamp since the unit is re-added.
	unitInfo.Timestamp = p.clock.Now()
	// Add to readyQ anyway, we will check timestamp when pop unit.
	queue = p.readyQ

	klog.V(4).InfoS("SchedulingQueue AddUnschedulable, add to newQueue", "unitKey", unitInfo.UnitKey, "attempts", unitInfo.Attempts, "subCluster", p.subCluster, "qos", p.qos, "newQueue", queue)
	p.metricsRecorder.recordIncoming(unitInfo, queue.String(), godelutil.ScheduleAttemptFailure)
	return queue.Add(unitInfo)
}

// Update updates a pod in the ready/waiting/backoff queue if present. Otherwise, it removes
// the item from the unschedulable queue if pod is updated in a way that it may
// become schedulable and adds the updated one to the ready/waiting queue.
// If pod is not present in any of the queues, it is added to the waiting queue.
func (p *BlockQueue) Update(oldPod, newPod *v1.Pod) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.latestOperationTimestamp = p.clock.Now()

	// If the pod is in the unschedulable queue, updating it may make it schedulable.
	queue, unitInfo, err := p.updatePodInUnitInfo(oldPod, newPod)
	if err != nil {
		klog.V(4).InfoS("SchedulingQueue Update, update to waitingPodsList directly", "subCluster", p.subCluster, "qos", p.qos, "pod", klog.KObj(newPod), "podUID", newPod.GetUID(), "err", err)
		return p.waitingPodsList.Update(oldPod, newPod)
	}

	if p.readyToBeScheduled(unitInfo) {
		queue = p.readyQ
		defer p.cond.Broadcast()
	} else {
		queue = p.waitingQ
	}
	klog.V(4).InfoS("SchedulingQueue Update, add to newQueue", "subCluster", p.subCluster, "qos", p.qos, "unitKey", unitInfo.UnitKey, "newQueue", queue)
	return queue.Add(unitInfo)
}

// Delete deletes the item from either of the two queues. It assumes the pod is
// only in one queue.
func (p *BlockQueue) Delete(pod *v1.Pod) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.latestOperationTimestamp = p.clock.Now()

	var err error
	queue, unitInfo, _ := p.deletePodFromUnitInfo(pod)
	err = p.waitingPodsList.Delete(pod)
	if err != nil {
		klog.V(4).InfoS("Error occurred in SchedulingQueue.Delete", "subCluster", p.subCluster, "qos", p.qos, "queue", p.waitingPodsList, "err", err)
	}
	if queue == nil {
		klog.V(4).InfoS("SchedulingQueue Delete, delete from waitingPodsList directly", "subCluster", p.subCluster, "qos", p.qos, "pod", klog.KObj(pod), "podUID", pod.GetUID())
		return nil
	}

	// The unit has been removed from SubQueue in deletePodFromUnitInfo.
	if unitInfo.NumPods() == 0 {
		klog.V(4).InfoS("SchedulingQueue Delete, delete from oldQueue and won't add back", "subCluster", p.subCluster, "qos", p.qos, "unitKey", unitInfo.UnitKey, "oldQueue", queue)
	} else {
		if queue != p.waitingQ && !p.readyToBeScheduled(unitInfo) {
			klog.V(4).InfoS("SchedulingQueue Delete, move the unit", "subCluster", p.subCluster, "qos", p.qos, "unitKey", unitInfo.UnitKey, "oldQueue", queue, "newQueue", p.waitingQ)
			err = p.waitingQ.Add(unitInfo)
			if err != nil {
				klog.V(4).InfoS("Error occurred when moving unit in SchedulingQueue.Delete", "subCluster", p.subCluster, "qos", p.qos, "queue", p.waitingQ, "err", err)
			}
		} else {
			klog.V(4).InfoS("SchedulingQueue Delete, update the unit", "subCluster", p.subCluster, "qos", p.qos, "unitKey", unitInfo.UnitKey, "oldQueue", queue, "newQueue", queue)
			err = queue.Add(unitInfo)
			if err != nil {
				klog.V(4).InfoS("Error occurred when updating unit in SchedulingQueue.Delete", "subCluster", p.subCluster, "qos", p.qos, "queue", queue, "err", err)
			}
		}
	}
	return err
}

// We removed the unschedulable-related logic from the BlockQueue, so nothing will be done here.
func (p *BlockQueue) AssignedPodAdded(pod *v1.Pod) {}

// We removed the unschedulable-related logic from the BlockQueue, so nothing will be done here.
func (p *BlockQueue) AssignedPodUpdated(pod *v1.Pod) {}

// We removed the unschedulable-related logic from the BlockQueue, so nothing will be done here.
func (p *BlockQueue) MoveAllToActiveOrBackoffQueue(event string) {}

func (p *BlockQueue) ActivePodGroupUnit(unitKey string) {
	p.lock.Lock()
	defer p.lock.Unlock()
	wpObj, exist, _ := p.waitingPodsList.GetByKey(unitKey)
	if !exist || wpObj == nil {
		return
	}
	unitInfo := wpObj.(*waitingPods).transferToPodGroupUnit(p.pcLister, p.pgLister, p.clock)
	if unitInfo == nil {
		return
	}
	var queue SubQueue
	if p.readyToBeScheduled(unitInfo) {
		queue = p.readyQ
		defer p.cond.Broadcast()
	} else {
		queue = p.waitingQ
	}
	klog.V(4).InfoS("SchedulingQueue ActivePodGroupUnit, will move the unit", "subCluster", p.subCluster, "unitKey", unitInfo.UnitKey, "oldQueue", p.waitingPodsList, "newQueue", queue)
	if err := queue.Add(unitInfo); err != nil {
		klog.V(4).InfoS("SchedulingQueue ActivePodGroupUnit, failed to move the unit", "unitKey", unitInfo.UnitKey, "oldQueue", p.waitingPodsList, "newQueue", queue, "err", err)
	} else {
		p.waitingPodsList.DeleteByKey(unitKey)
	}
}

// PopUnit pops a unit for batch scheduling.
func (p *BlockQueue) Pop() (*framework.QueuedUnitInfo, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for p.readyQ.Len() == 0 ||
		p.boHandler.isUnitBackoff(p.readyQ.Peek().(*framework.QueuedUnitInfo)) {
		if p.closed {
			return nil, fmt.Errorf(queueClosed)
		}
		p.cond.Wait()
	}
	obj, err := p.readyQ.Pop()
	if err != nil {
		return nil, err
	}
	unitInfo := obj.(*framework.QueuedUnitInfo)
	unitInfo.Attempts++
	p.schedulingCycle++
	// TODO: improve metrics
	p.metricsRecorder.recordPending(unitInfo, time.Since(unitInfo.Timestamp))
	return unitInfo, nil
}

func (p *BlockQueue) Peek() *framework.QueuedUnitInfo {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if p.readyQ.Len() == 0 {
		return nil
	}
	obj := p.readyQ.Peek()
	if obj == nil {
		return nil
	}
	return obj.(*framework.QueuedUnitInfo)
}

// PendingPods returns all the pending pods in the queue. This function is
// used for debugging purposes in the scheduler cache dumper and comparer.
func (p *BlockQueue) PendingPods() []*v1.Pod {
	p.lock.RLock()
	defer p.lock.RUnlock()
	var result []*v1.Pod
	for _, queue := range []SubQueue{p.waitingPodsList, p.waitingQ, p.readyQ} {
		for _, u := range queue.List() {
			for _, pInfo := range u.(framework.StoredUnit).GetPods() {
				result = append(result, pInfo.Pod)
			}
		}
	}
	return result
}

// NumUnschedulableUnits returns the number of unschedulable pods exist in the SchedulingQueue.
// In BlockQueue we removed the Unschedulable-related logic, so it always returns 0.
func (p *BlockQueue) NumUnschedulableUnits() int {
	return 0
}

// SchedulingCycle returns current scheduling cycle.
func (p *BlockQueue) SchedulingCycle() int64 {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.schedulingCycle
}
