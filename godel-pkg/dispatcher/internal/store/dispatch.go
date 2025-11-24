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

package store

import (
	"math"
	"math/rand"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	podutil "github.com/kubewharf/godel-scheduler/pkg/util/pod"
	unitutil "github.com/kubewharf/godel-scheduler/pkg/util/unit"
)

type podInfo struct {
	podKey    string
	gangID    string
	scheduler string
	// The time pod added to the scheduling queue.
	timeStamp time.Time
}

func NewPodInfo(pod *v1.Pod, scheduler string) *podInfo {
	return &podInfo{
		podKey:    podutil.GetPodKey(pod),
		gangID:    unitutil.GetPodGroupFullName(pod),
		scheduler: scheduler,
		timeStamp: time.Now(),
	}
}

// the pod must be dispatched pod here, so the scheduler annotation has already been set
func (p *podInfo) getScheduler() string {
	return p.scheduler
}

func (p *podInfo) getGangID() string {
	return p.gangID
}

type podStore map[string]sets.String

func (ps podStore) addPod(key, podID string) {
	if val, ok := ps[key]; !ok {
		ps[key] = sets.NewString(podID)
	} else {
		val.Insert(podID)
	}
}

func (ps podStore) removePod(key, podID string) {
	if val, ok := ps[key]; !ok {
		return
	} else {
		val.Delete(podID)
		if val.Len() == 0 {
			delete(ps, key)
		}
	}
}

func (ps podStore) getLeastGroup() string {
	var ret string
	max := math.MaxInt32
	for k, v := range ps {
		if v.Len() < max {
			max = v.Len()
			ret = k
		}
	}
	return ret
}

// TODO: i don't think we should do expiration operations(remove pod directly) in dispatcher
// we should react based on pod events and scheduler liveness changes
// TODO: figure out what we can do if schedulers dies
type DispatchInfo interface {
	AddPod(pod *v1.Pod)
	RemovePod(pod *v1.Pod)
	RemovePodByKey(key string)
	AddPodInAdvance(pod *v1.Pod, scheduler string)
	UpdatePodInAdvance(pod *v1.Pod, scheduler string)
	GetMostIdleSchedulerAndAddPodInAdvance(pod *v1.Pod) string
	AddScheduler(schedulerName string)
	DeleteScheduler(schedulerName string)
	GetPodsOfOneScheduler(schedulerName string) []string
}

type dispatchInfo struct {
	lock            sync.RWMutex
	Pods            map[string]*podInfo
	SchedulerToPods podStore

	Schedulers map[string]struct{}
}

func NewDispatchInfo() DispatchInfo {
	return &dispatchInfo{
		Pods:            make(map[string]*podInfo),
		SchedulerToPods: make(podStore),
		Schedulers:      make(map[string]struct{}),
	}
}

func (dq *dispatchInfo) AddScheduler(schedulerName string) {
	dq.lock.Lock()
	defer dq.lock.Unlock()
	if len(schedulerName) == 0 {
		return
	}
	dq.Schedulers[schedulerName] = struct{}{}
}

func (dq *dispatchInfo) DeleteScheduler(schedulerName string) {
	dq.lock.Lock()
	defer dq.lock.Unlock()

	delete(dq.Schedulers, schedulerName)
}

// TODO: do we need to cache whole pod structs in dispatch info ?  pod key is enough ?
// but since the calling frequency of this function will not be high, it is ok for now
func (dq *dispatchInfo) GetPodsOfOneScheduler(schedulerName string) []string {
	dq.lock.Lock()
	defer dq.lock.Unlock()

	if len(dq.SchedulerToPods[schedulerName]) == 0 {
		return nil
	}

	results := make([]string, 0, len(dq.SchedulerToPods))
	for key := range dq.SchedulerToPods[schedulerName] {
		if pInfo := dq.Pods[key]; pInfo != nil {
			results = append(results, pInfo.podKey)
		}
	}

	return results
}

func (dq *dispatchInfo) addPod(pod *v1.Pod, scheduler string) {
	key, err := cache.MetaNamespaceKeyFunc(pod)
	if err != nil {
		return
	}
	podInfo := NewPodInfo(pod, scheduler)
	dq.Pods[key] = podInfo
	dq.SchedulerToPods.addPod(podInfo.getScheduler(), key)
}

func (dq *dispatchInfo) AddPod(pod *v1.Pod) {
	dq.lock.Lock()
	defer dq.lock.Unlock()
	dq.addPod(pod, pod.Annotations[podutil.SchedulerAnnotationKey])
}

func (dq *dispatchInfo) RemovePod(pod *v1.Pod) {
	dq.lock.Lock()
	defer dq.lock.Unlock()
	dq.removePod(pod)
}

func (dq *dispatchInfo) RemovePodByKey(key string) {
	dq.lock.Lock()
	defer dq.lock.Unlock()
	_, ok := dq.Pods[key]
	if !ok {
		return
	}
	dq.removeFromScheduler(key)
	delete(dq.Pods, key)
}

func (dq *dispatchInfo) removePod(pod *v1.Pod) {
	key, err := cache.MetaNamespaceKeyFunc(pod)
	if err != nil {
		return
	}
	_, ok := dq.Pods[key]
	if !ok {
		return
	}
	dq.removeFromScheduler(key)
	delete(dq.Pods, key)
}

func (dq *dispatchInfo) removeFromScheduler(podID string) {
	if len(podID) == 0 {
		return
	}
	podInfo, ok := dq.Pods[podID]
	if !ok {
		return
	}
	dq.SchedulerToPods.removePod(podInfo.getScheduler(), podID)
}

func (dq *dispatchInfo) AddPodInAdvance(pod *v1.Pod, scheduler string) {
	dq.lock.Lock()
	defer dq.lock.Unlock()
	dq.addPod(pod, scheduler)
}

func (dq *dispatchInfo) UpdatePodInAdvance(pod *v1.Pod, scheduler string) {
	dq.lock.Lock()
	defer dq.lock.Unlock()
	dq.removePod(pod)
	dq.addPod(pod, scheduler)
}

// GetMostIdleSchedulerAndAddPodInAdvance 是 dispatchInfo 结构体的一个方法，用于实现负载均衡。
// 它的目标是找到当前待处理 Pod 数量最少（最空闲）的调度器，并将传入的 Pod 预先分配给它（在 Dispatcher 内部记录）。
// 为了处理存在多个最空闲调度器的情况，该方法使用了水库采样 (Reservoir Sampling) 算法，
// 以确保在多个最空闲调度器中随机选择一个，保证选择的公平性。
// 返回选中的调度器名称，如果没有任何注册的调度器，则返回空字符串。
func (dq *dispatchInfo) GetMostIdleSchedulerAndAddPodInAdvance(pod *v1.Pod) string {
	// 获取写锁，以保证在查找最空闲调度器和更新 Pod 分配信息时的并发安全。
	dq.lock.Lock()
	defer dq.lock.Unlock()

	// 用于存储最终选中的调度器名称。
	result := ""
	// 用于记录当前找到的最小 Pod 数量（最空闲状态）。
	// 初始化为整数最大值，确保第一个调度器的 Pod 数量会小于它。
	max := math.MaxInt32

	// Ref: https://en.wikipedia.org/wiki/reservoir_sampling
	// 水库采样算法的实现部分：
	// randomPoolSize 用于跟踪当前找到的具有相同最小 Pod 数量的调度器数量。
	// 这对于水库采样算法至关重要。
	var randomPoolSize int

	// 遍历所有已注册的调度器。
	for schedulerName := range dq.Schedulers {
		// 获取当前调度器正在处理的 Pod 数量。
		klog.InfoS("当前的调度器名称为","schedulerName",schedulerName)
		cnt := 0
		// 检查该调度器是否有对应的 Pod 队列。
		if dq.SchedulerToPods[schedulerName] != nil {
			// 获取队列中 Pod 的数量。
			cnt = dq.SchedulerToPods[schedulerName].Len()
			klog.Info("更新cnt")
			klog.InfoS("cnt的值为","cnt",cnt)
		}
		klog.Info("跳过更新cnt")
		// 情况一：当前调度器的 Pod 数量比之前记录的最小值还要少。
		if cnt < max {
			// 更新最小 Pod 数量。
			max = cnt
			// 更新选中的调度器名称。
			result = schedulerName
			// 重置随机池大小，因为找到了新的、更空闲的调度器。
			// 此时，这个新找到的调度器是唯一一个具有 `max` 数量 Pod 的调度器。
			randomPoolSize = 1
		} else if cnt == max {
			// 情况二：当前调度器的 Pod 数量等于之前记录的最小值。
			// 这意味着我们找到了另一个同样空闲的调度器。
			// 增加随机池的大小。
			randomPoolSize++
			// 使用水库采样算法的核心逻辑：
			// 生成一个 [0, randomPoolSize) 范围内的随机整数。
			// 如果这个随机数恰好是 0，则将 `result` 更新为当前的 `schedulerName`。
			// 这样可以保证，在所有具有最小 Pod 数量的调度器中，每个被选中的概率都是均等的 (1 / randomPoolSize)。
			if rand.Intn(randomPoolSize) == 0 {
				result = schedulerName
			}
		}
		// 如果 cnt > max，则当前调度器不是最空闲的，跳过。
	}

	// 如果成功找到了一个调度器（result 不为空），则将 Pod 预先分配给它。
	if result != "" {
		// 调用内部方法 addPod 将 Pod 添加到指定调度器的待处理队列映射中。
		// 这是一种“预占”行为，使得在后续的调度决策中，这个 Pod 已经被计入该调度器的负载。
		dq.addPod(pod, result)
	}

	// 返回选中的调度器名称，如果没有任何调度器注册，则返回空字符串。
	return result
}

type OwnerInfo interface {
	AddDispatchedUnboundPod(pod *v1.Pod, schedulerName string)
	SetDispatchedUnboundPod(pod *v1.Pod, schedulerName string) string
	DeleteDispatchedUnboundPod(pod *v1.Pod)
	SelectSchedulerAndSetDispatchedUnboundPod(pod *v1.Pod) string
}

type ownerInfo struct {
	// key is owner name
	ownerToUnboundPods map[string]*ownerPodsInfo
	// key is pod key, value is owner name
	podToOwner map[string]string
	lock       sync.RWMutex
}

type ownerPodsInfo struct {
	schedulerName string
	unBoundPods   sets.String
}

func NewOwnerInfo() *ownerInfo {
	return &ownerInfo{
		ownerToUnboundPods: map[string]*ownerPodsInfo{},
		podToOwner:         map[string]string{},
	}
}

func newOwnerPodsInfo() *ownerPodsInfo {
	return &ownerPodsInfo{
		unBoundPods: sets.NewString(),
	}
}

func (oInfo *ownerInfo) AddDispatchedUnboundPod(pod *v1.Pod, schedulerName string) {
	if schedulerName == "" {
		return
	}
	podKey := podutil.GeneratePodKey(pod)
	podOwner := podutil.GetPodOwnerInfoKey(pod)
	if podOwner == "" {
		return
	}

	oInfo.lock.Lock()
	defer oInfo.lock.Unlock()
	oInfo.addPod(podOwner, podKey, schedulerName)
}

func (oInfo *ownerInfo) SetDispatchedUnboundPod(pod *v1.Pod, schedulerName string) string {
	if schedulerName == "" {
		return schedulerName
	}
	podKey := podutil.GeneratePodKey(pod)
	podOwner := podutil.GetPodOwnerInfoKey(pod)
	if podOwner == "" {
		return schedulerName
	}
	oInfo.lock.Lock()
	defer oInfo.lock.Unlock()
	existingScheduler := oInfo.getOwnerScheduler(podOwner)
	if existingScheduler != "" && existingScheduler != schedulerName {
		klog.InfoS("WARN: Scheduler was ever assigned to pod, so could not set the newly selected scheduler to that owner", "schedulerName", existingScheduler, "pod", klog.KObj(pod), "podKey", podutil.GeneratePodKey(pod), "NewSchedulerName", schedulerName)
		schedulerName = existingScheduler
	}
	oInfo.addPod(podOwner, podKey, schedulerName)
	return schedulerName
}

func (oInfo *ownerInfo) addPod(podOwner, podKey, schedulerName string) {
	if oInfo.ownerToUnboundPods == nil {
		oInfo.ownerToUnboundPods[podOwner] = newOwnerPodsInfo()
	}
	ownerPodsInfo, ok := oInfo.ownerToUnboundPods[podOwner]
	if !ok || ownerPodsInfo == nil {
		oInfo.ownerToUnboundPods[podOwner] = newOwnerPodsInfo()
		ownerPodsInfo = oInfo.ownerToUnboundPods[podOwner]
	}
	ownerPodsInfo.unBoundPods.Insert(podKey)
	ownerPodsInfo.schedulerName = schedulerName

	oInfo.podToOwner[podKey] = podOwner
}

func (oInfo *ownerInfo) DeleteDispatchedUnboundPod(pod *v1.Pod) {
	oInfo.lock.Lock()
	defer oInfo.lock.Unlock()

	podKey := podutil.GeneratePodKey(pod)
	podOwner := oInfo.podToOwner[podKey]
	ownerPodsInfo := oInfo.ownerToUnboundPods[podOwner]
	if ownerPodsInfo != nil {
		ownerPodsInfo.unBoundPods.Delete(podKey)
		if ownerPodsInfo.unBoundPods.Len() == 0 {
			delete(oInfo.ownerToUnboundPods, podOwner)
		}
	}

	delete(oInfo.podToOwner, podKey)
}

func (oInfo *ownerInfo) SelectSchedulerAndSetDispatchedUnboundPod(pod *v1.Pod) string {
	podKey := podutil.GeneratePodKey(pod)
	podOwner := podutil.GetPodOwnerInfoKey(pod)
	if podOwner == "" {
		return ""
	}
	oInfo.lock.Lock()
	defer oInfo.lock.Unlock()
	schedulerName := oInfo.getOwnerScheduler(podOwner)
	if schedulerName != "" {
		oInfo.addPod(podOwner, podKey, schedulerName)
	}
	return schedulerName
}

func (oInfo *ownerInfo) getOwnerScheduler(podOwner string) string {
	ownerPodsInfo := oInfo.ownerToUnboundPods[podOwner]
	if ownerPodsInfo != nil {
		return ownerPodsInfo.schedulerName
	}
	return ""
}
