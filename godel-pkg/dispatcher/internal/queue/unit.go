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

	"github.com/kubewharf/godel-scheduler-api/pkg/apis/scheduling/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"

	"github.com/kubewharf/godel-scheduler/pkg/dispatcher/metrics"
	"github.com/kubewharf/godel-scheduler/pkg/framework/api"
)

// UnitInfos 接口定义了 Pod 单元信息管理器的功能
// 负责管理 PodGroup 与 Pod 之间的关系，跟踪 Pod 的调度就绪状态，并维护就绪的 Pod 队列
type UnitInfos interface {
	// AddPodGroup 添加一个新的 PodGroup 到管理器中
	AddPodGroup(pg *v1alpha1.PodGroup)
	// UpdatePodGroup 更新一个已存在的 PodGroup 信息
	UpdatePodGroup(oldPG, newPG *v1alpha1.PodGroup)
	// DeletePodGroup 从管理器中删除一个 PodGroup
	DeletePodGroup(pg *v1alpha1.PodGroup)
	// AddPod 将 Pod 添加到指定的单元中
	AddPod(unitKey string, podKey string)
	// DeletePod 从指定的单元中删除一个 Pod
	DeletePod(unitKey string, podKey string)
	// AddUnSortedPodInfo 添加未排序的 Pod 信息到单元中
	AddUnSortedPodInfo(unitKey string, podInfo *QueuedPodInfo)
	// DeleteUnSortedPodInfo 从单元中删除未排序的 Pod 信息
	DeleteUnSortedPodInfo(unitKey string, podInfo *QueuedPodInfo)
	// Pop 从就绪队列中弹出一个 Pod 信息，通常用于调度流程
	Pop() (*QueuedPodInfo, error)
	// Enqueue 将 Pod 信息加入到队列中
	Enqueue(podInfo *QueuedPodInfo)
	// GetAssignedSchedulerForPodGroupUnit 获取为 PodGroup 单元分配的调度器名称
	GetAssignedSchedulerForPodGroupUnit(pg *v1alpha1.PodGroup) string
	// AssignSchedulerToPodGroupUnit 为 PodGroup 单元分配调度器，forceUpdate 参数控制是否强制更新
	AssignSchedulerToPodGroupUnit(pg *v1alpha1.PodGroup, schedName string, forceUpdate bool) error
	// Run 启动后台运行逻辑，定期检查和更新 Pod 单元的就绪状态
	Run(stop <-chan struct{})
}

// unitInfos 是 UnitInfos 接口的具体实现
// 负责内部状态管理、并发控制和 Pod 就绪队列维护
type unitInfos struct {
	// 互斥锁，用于保护并发访问
	sync.Mutex

	// 存储所有 Pod 单元的信息，以单元键为索引
	units map[string]*unitInfo

	// 就绪的单元 Pod 队列，使用 FIFO 缓存，存放已经满足调度条件的 Pod
	readyUnitPods *cache.FIFO

	// 事件记录器，用于记录调度相关的事件和日志
	recorder events.EventRecorder
}

var _ UnitInfos = &unitInfos{}

func NewUnitInfos(recorder events.EventRecorder) UnitInfos {
	return &unitInfos{
		units:         make(map[string]*unitInfo),
		readyUnitPods: cache.NewFIFO(simpleKeyFunc),
		recorder:      recorder,
	}
}

func simpleKeyFunc(obj interface{}) (string, error) {
	return obj.(*QueuedPodInfo).PodKey, nil
}

// Run 启动 UnitInfos 的后台运行逻辑
// 该方法会在一个独立的 goroutine 中定期执行 populate 操作，以维护和更新 Pod 单元信息
func (uis *unitInfos) Run(stop <-chan struct{}) {
	// 启动一个 goroutine，按照指定的时间间隔（30秒）周期性地执行 populate 函数
	// populate 函数负责从缓存或其他数据源中更新和同步 Pod 单元的相关信息
	// 当 stop 通道接收到信号时，该循环会停止
	go wait.Until(uis.populate, 30*time.Second, stop)
}

// syncPendingMetricsFactory 创建并返回一个函数，用于同步待处理 Pod 和 Unit 的指标
// 该函数负责收集和更新与未排序（待处理）的 Pod 和 Unit 相关的监控指标
func syncPendingMetricsFactory() func(ui *unitInfo) {
	// 用于统计待处理 Pod 的标签结构
	// 包含服务质量等级和子集群信息，用于对指标进行分类
	type PodMetricsLabels struct {
		Qos        string // 服务质量等级 (QoS)
		SubCluster string // 子集群名称
	}

	// 用于统计待处理 Unit 的标签结构
	// 在 Pod 标签基础上增加了 Unit 类型，提供更细粒度的指标分类
	type UnitMetricsLabels struct {
		PodMetricsLabels // 嵌入 Pod 指标标签
		unitType string   // Unit 类型
	}

	// 初始化 Pod 计数器映射，用于累计每个标签组合下的待处理 Pod 数量
	pendingPodsCounter := make(map[PodMetricsLabels]float64)
	// 初始化 Unit 计数器映射，用于累计每个标签组合下的待处理 Unit 数量
	pendingUnitsCounter := make(map[UnitMetricsLabels]float64)

	// 返回一个闭包函数，该函数接收一个 unitInfo 参数并执行指标同步逻辑
	return func(ui *unitInfo) {
		// 获取单元的属性信息
		unitProperty := ui.GetUnitProperty()
		// 如果单元属性为空，则直接返回，不进行指标更新
		if unitProperty == nil {
			return
		}

		// 将接口类型转换为具体的 unitInfoProperty 结构体
		uip := unitProperty.(*unitInfoProperty)
		
		// 创建 Pod 指标标签
		podMetricsLabels := PodMetricsLabels{string(uip.Qos), uip.SubCluster}
		
		// 累加当前单元中未排序 Pod 的数量到对应的计数器中
		pendingPodsCounter[podMetricsLabels] += float64(len(ui.unSortedPods))
		// 设置指标：更新特定 Pod 属性和状态（"waiting"）下的待处理 Pod 数量
		metrics.PendingPodsSet(uip.GetPodProperty(), "waiting", pendingPodsCounter[podMetricsLabels])

		// 初始化 Unit 计数增量为 0
		// 这是为了确保当 Unit 中没有待处理 Pod 时，指标能够正确地反映为 0
		t := 0.0 
		// 如果当前单元中有待处理的 Pod，则将增量设为 1.0
		if len(ui.unSortedPods) > 0 {
			t = 1.0
		}

		// 创建 Unit 指标标签
		unitMetricsLabels := UnitMetricsLabels{podMetricsLabels, uip.UnitType}
		// 累加 Unit 的计数（1 表示有等待状态的 Unit，0 表示没有）
		pendingUnitsCounter[unitMetricsLabels] += t
		// 设置指标：更新特定 Unit 属性和状态（"waiting"）下的待处理 Unit 数量
		metrics.PendingUnitsSet(uip, "waiting", pendingUnitsCounter[unitMetricsLabels])
	}
}

// populate 方法负责检查所有 Pod 单元的调度就绪状态，并根据状态更新队列
// 该方法会定期被调用，以确保 Pod 能够在其依赖条件满足时及时移动到就绪队列
func (uis *unitInfos) populate() {
	// 加锁以确保并发安全
	uis.Lock()
	defer uis.Unlock()

	// 遍历所有 Pod 单元
	for _, ui := range uis.units {
		// 检查 Pod 单元是否准备好被调度
		// 返回就绪状态和相关信息（如未就绪的原因）
		_, isReady := ui.readyToBeDispatched()
		
		if isReady {
			// 如果 Pod 单元已准备好调度，则将其相关的 Pod 移动到就绪队列
			uis.movePodsToReadyQueue(ui)
		} 
	}
}

// movePodsToReadyQueue 将指定单元中已就绪的 Pod 从未排序列表移动到就绪队列
// 这是一个私有函数，假设调用前已经获取了锁（由外部函数负责加锁）
func (uis *unitInfos) movePodsToReadyQueue(ui *unitInfo) {
	// 如果单元中没有未排序的 Pod，则直接返回
	if len(ui.unSortedPods) == 0 {
		return
	}
	
	// 遍历单元中所有未排序的 Pod
	for key, podInfo := range ui.unSortedPods {
		// 记录调试信息，显示将 Pod 添加到就绪队列的操作
		klog.InfoS("DEBUG: 将 Pod 添加到就绪队列", "pod", podInfo.PodKey)
		
		// 尝试将 Pod 信息添加到就绪队列中
		if err := uis.readyUnitPods.Add(podInfo); err == nil {
			// 如果添加成功，则从未排序列表中删除该 Pod
			delete(ui.unSortedPods, key)
		} else {
			// 如果添加失败，记录错误信息
			klog.InfoS("DEBUG: 将 Pod 添加到就绪队列时发生错误", "pod", podInfo.PodKey, "err", err)
		}
	}
}

// TODO: revisit this later
func generateUnitKeyFromPodGroup(pg *v1alpha1.PodGroup) string {
	return pg.Namespace + "/" + pg.Name
}

func (uis *unitInfos) AddPodGroup(pg *v1alpha1.PodGroup) {
	uis.Lock()
	defer uis.Unlock()

	uis.addPodGroup(pg)
}

func (uis *unitInfos) addPodGroup(pg *v1alpha1.PodGroup) {
	unitKey := generateUnitKeyFromPodGroup(pg)
	ui, ok := uis.units[unitKey]
	if !ok {
		ui = NewUnitInfo()
		uis.units[unitKey] = ui
	}
	ui.podGroup = pg

	message, isReady := uis.units[unitKey].readyToBeDispatched()
	if isReady {
		uis.movePodsToReadyQueue(uis.units[unitKey])
	} else {
		podGroup := uis.units[unitKey].podGroup
		if podGroup != nil {
			uis.recorder.Eventf(
				podGroup, nil, v1.EventTypeNormal, "AddOrUpdatePodGroup", "CheckDispatchReadiness", message)
		}
	}
}

func (uis *unitInfos) UpdatePodGroup(oldPG, newPG *v1alpha1.PodGroup) {
	uis.Lock()
	defer uis.Unlock()

	uis.addPodGroup(newPG)
}

func (uis *unitInfos) DeletePodGroup(pg *v1alpha1.PodGroup) {
	uis.Lock()
	defer uis.Unlock()

	unitKey := generateUnitKeyFromPodGroup(pg)
	ui, ok := uis.units[unitKey]
	if !ok {
		klog.InfoS("Failed to find podGroup in unit info", "podGroup", klog.KObj(pg))
		return
	}

	ui.podGroup = nil
	if len(ui.pods) == 0 {
		delete(uis.units, unitKey)
	}
}

func (uis *unitInfos) AddPod(unitKey string, podKey string) {
	uis.Lock()
	defer uis.Unlock()

	if len(unitKey) > 0 {
		if uis.units[unitKey] == nil {
			uis.units[unitKey] = NewUnitInfo()
		}
		uis.units[unitKey].pods[podKey] = struct{}{}

		message, isReady := uis.units[unitKey].readyToBeDispatched()
		if isReady {
			uis.movePodsToReadyQueue(uis.units[unitKey])
		} else {
			podGroup := uis.units[unitKey].podGroup
			if podGroup != nil {
				uis.recorder.Eventf(podGroup, nil, v1.EventTypeNormal, "AddPodKey",
					"CheckDispatchReadiness", fmt.Sprintf("message: %s, pod: %s", message, podKey))
			}
		}
	}
}

func (uis *unitInfos) AddUnSortedPodInfo(unitKey string, podInfo *QueuedPodInfo) {
	uis.Lock()
	defer uis.Unlock()

	if len(unitKey) > 0 {
		if uis.units[unitKey] == nil {
			uis.units[unitKey] = NewUnitInfo()
		}
		uis.units[unitKey].unSortedPods[podInfo.PodKey] = podInfo
		uis.units[unitKey].pods[podInfo.PodKey] = struct{}{}

		message, isReady := uis.units[unitKey].readyToBeDispatched()
		if isReady {
			klog.V(5).InfoS("DEBUG: scheduling unit is ready to be dispatched", "unitKey", unitKey)
			uis.movePodsToReadyQueue(uis.units[unitKey])
		} else {
			klog.V(5).InfoS("DEBUG: scheduling unit is not ready to be dispatched", "unitKey", unitKey)
			podGroup := uis.units[unitKey].podGroup
			if podGroup != nil {
				uis.recorder.Eventf(podGroup, nil, v1.EventTypeNormal, "AddPodInfo", "CheckDispatchReadiness",
					fmt.Sprintf("message: %s, pod: %s", message, podInfo.PodKey))
			}
		}
	}
}

func (uis *unitInfos) DeleteUnSortedPodInfo(unitKey string, podInfo *QueuedPodInfo) {
	uis.Lock()
	defer uis.Unlock()

	if len(unitKey) > 0 && uis.units[unitKey] != nil {
		delete(uis.units[unitKey].unSortedPods, podInfo.PodKey)
	}
}

func (uis *unitInfos) DeletePod(unitKey string, podKey string) {
	uis.Lock()
	defer uis.Unlock()

	if len(unitKey) > 0 && uis.units[unitKey] != nil {
		delete(uis.units[unitKey].pods, podKey)
		delete(uis.units[unitKey].unSortedPods, podKey)

		if len(uis.units[unitKey].pods) == 0 && uis.units[unitKey].podGroup == nil {
			delete(uis.units, unitKey)
		}
	}
}

func (uis *unitInfos) Pop() (*QueuedPodInfo, error) {
	result, err := uis.readyUnitPods.Pop(func(obj interface{}) error { return nil })
	if err == cache.ErrFIFOClosed {
		return nil, fmt.Errorf("ready unit pods FIFO queue closed")
	}

	return result.(*QueuedPodInfo), err
}

func (uis *unitInfos) Enqueue(podInfo *QueuedPodInfo) {
	uis.readyUnitPods.Add(podInfo)
}

func (uis *unitInfos) GetAssignedSchedulerForPodGroupUnit(pg *v1alpha1.PodGroup) string {
	uis.Lock()
	defer uis.Unlock()
	unitKey := generateUnitKeyFromPodGroup(pg)
	if ui, exist := uis.units[unitKey]; exist {
		return ui.scheduler
	}
	return ""
}

func (uis *unitInfos) AssignSchedulerToPodGroupUnit(pg *v1alpha1.PodGroup, schedName string, forceUpdate bool) error {
	uis.Lock()
	defer uis.Unlock()
	unitKey := generateUnitKeyFromPodGroup(pg)
	ui := uis.units[unitKey]
	if ui == nil {
		uis.units[unitKey] = NewUnitInfo()
		ui = uis.units[unitKey]
	}
	if forceUpdate {
		ui.scheduler = schedName
		return nil
	}
	if ui.scheduler != "" && ui.scheduler != schedName {
		return fmt.Errorf("scheduler: %v is ever assigned to unit: %v, so can not set the newly selected scheduler: %v to that unit", ui.scheduler, unitKey, schedName)
	}
	ui.scheduler = schedName

	return nil
}

type unitInfo struct {
	podGroup     *v1alpha1.PodGroup
	pods         map[string]struct{}
	unSortedPods map[string]*QueuedPodInfo
	scheduler    string
	unitProperty api.UnitProperty

	// begin time wait for unit be ready
	waitingTimestamp time.Time
}

var _ api.ObservableUnit = &unitInfo{}

func NewUnitInfo() *unitInfo {
	return &unitInfo{
		pods:             make(map[string]struct{}),
		unSortedPods:     make(map[string]*QueuedPodInfo),
		waitingTimestamp: time.Now(),
	}
}

const (
	MsgNilPodGroup                     string = "DEBUG: pod group is nil"
	MsgPodGroupBeingDeleted            string = "DEBUG: pod group is being deleted"
	MsgPodGroupInPendingOrUnknownPhase string = "DEBUG: pod group is in either pending or unknown phase"
	MsgPodGroupLessThanMinMember       string = "DEBUG: pod group has not yet met the MinMember requirement with numReadyToBeDispatched=%d and minMember=%d"
)

// readyToBeDispatched 检查当前 Pod 单元是否准备好被调度
// 返回：描述信息（如未就绪的原因）和布尔值（是否就绪）
func (ui *unitInfo) readyToBeDispatched() (string, bool) {
	// 检查 PodGroup 是否存在
	if nil == ui.podGroup {
		// 如果 PodGroup 为 nil，记录调试信息并返回未就绪状态
		klog.InfoS(MsgNilPodGroup)
		return MsgNilPodGroup, false
	}

	// 检查 PodGroup 是否正在被删除
	if ui.podGroup.DeletionTimestamp != nil {
		// 如果 PodGroup 正在被删除，则不允许调度属于它的 Pod
		klog.InfoS(MsgPodGroupBeingDeleted, "podGroup", klog.KObj(ui.podGroup))
		return MsgPodGroupBeingDeleted, false
	}

	// 检查 PodGroup 的状态
	// 如果 PodGroup 的状态不是 Pending 或 Unknown，则认为可以调度
	// 这通常意味着 PodGroup 已经被调度器接受并准备就绪
	if ui.podGroup.Status.Phase != v1alpha1.PodGroupPending && ui.podGroup.Status.Phase != v1alpha1.PodGroupUnknown {
		return "", true
	}
	// 如果 PodGroup 仍处于 Pending 或 Unknown 状态，记录调试信息
	klog.InfoS(MsgPodGroupInPendingOrUnknownPhase, "podGroup", klog.KObj(ui.podGroup))

	// 检查 PodGroup 中已就绪的 Pod 数量是否达到最小成员数要求
	numReadyMembers := len(ui.pods) // 获取当前单元中已就绪的 Pod 数量
	minMember := int(ui.podGroup.Spec.MinMember) // 获取 PodGroup 要求的最小成员数
	
	if numReadyMembers < minMember {
		// 如果已就绪的 Pod 数量少于最小成员数，则返回未就绪状态和具体原因
		formattedMsg := fmt.Sprintf(MsgPodGroupLessThanMinMember, numReadyMembers, minMember)
		klog.InfoS(formattedMsg, "podGroup", klog.KObj(ui.podGroup))
		return formattedMsg, false
	}

	// 如果所有检查都通过，返回就绪状态（无错误信息）
	return "", true
}

type unitInfoProperty struct {
	*api.ScheduleUnitProperty
}

func newUnitInfoProperty(ui *unitInfo) *unitInfoProperty {
	if ui == nil || len(ui.unSortedPods) == 0 {
		return nil
	}

	var podProperty *api.PodProperty
	for _, podInfo := range ui.unSortedPods {
		if p := podInfo.GetPodProperty(); p != nil {
			podProperty = p
			break
		}
	}
	if podProperty == nil {
		return nil
	}

	minMember := 0
	if ui.podGroup != nil {
		minMember = int(ui.podGroup.Spec.MinMember)
	}

	return &unitInfoProperty{&api.ScheduleUnitProperty{
		PodProperty: podProperty,
		MinMember:   minMember,
		UnitType:    string(api.PodGroupUnitType),
	}}
}

func (ui *unitInfo) GetUnitProperty() api.UnitProperty {
	if ui.unitProperty != nil {
		return ui.unitProperty
	}

	p := newUnitInfoProperty(ui)
	if p == nil {
		return nil
	}
	ui.unitProperty = p
	return ui.unitProperty
}
