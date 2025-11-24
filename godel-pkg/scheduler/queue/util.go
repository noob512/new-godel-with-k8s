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
	"reflect"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"

	framework "k8s.io/kubernetes/godel-pkg/framework/api"
	"k8s.io/kubernetes/godel-pkg/plugins/unitqueuesort"
	"k8s.io/kubernetes/godel-pkg/scheduler/apis/config"
	"k8s.io/kubernetes/godel-pkg/util"
	podutil "k8s.io/kubernetes/godel-pkg/util/pod"
	"k8s.io/kubernetes/godel-pkg/util/tracing"
)

// newQueuedPodInfo builds a QueuedPodInfo object.
func newQueuedPodInfo(pod *v1.Pod, clock util.Clock) *framework.QueuedPodInfo {
	now := clock.Now()
	return &framework.QueuedPodInfo{
		Pod:                     pod,
		Timestamp:               now,
		InitialAttemptTimestamp: now,
		QueueSpan:               tracing.NewSpanInfo(framework.ExtractPodProperty(pod).ConvertToTracingTags()),
		OwnerReferenceKey:       podutil.GetPodTemplateKey(pod),
	}
}

// newQueuedPodInfoForLookup builds a QueuedPodInfo object without timestamp.
func newQueuedPodInfoForLookup(pod *v1.Pod) *framework.QueuedPodInfo {
	return &framework.QueuedPodInfo{
		Pod: pod,
	}
}

func updatePodInfo(oldPodInfo interface{}, newPod *v1.Pod) *framework.QueuedPodInfo {
	podInfo := oldPodInfo.(*framework.QueuedPodInfo)
	podInfo.Pod = newPod
	return podInfo
}

// isPodUpdated checks if the pod is updated in a way that it may have become
// schedulable. It drops status of the pod and compares it with old version.
func isPodUpdated(oldPod, newPod *v1.Pod) bool {
	if oldPod == nil || newPod == nil {
		return oldPod != nil || newPod != nil
	}
	strip := func(pod *v1.Pod) *v1.Pod {
		p := pod.DeepCopy()
		p.ResourceVersion = ""
		p.Generation = 0
		p.Status = v1.PodStatus{}
		return p
	}
	return !reflect.DeepEqual(strip(oldPod), strip(newPod))
}

func unitInfoKeyFunc(obj interface{}) (string, error) {
	unitInfo := obj.(*framework.QueuedUnitInfo)
	return unitInfo.UnitKey, nil
}

// MakeNextUnitFunc 是一个高阶函数，用于生成一个从指定调度队列（SchedulingQueue）中弹出下一个待调度单元的闭包函数。
// 返回的函数符合 unitScheduler 调度器所需的 nextUnit 策略接口，每次调用尝试从队列中取出一个可调度的单位（QueuedUnitInfo）。
//
// 参数：
//   - queue: 实现 SchedulingQueue 接口的调度队列实例，负责管理待调度的 Pod 单元（如 Gang、PodGroup 等）。
//
// 返回值：
//   - 一个无参函数，调用时尝试从队列中 Pop 一个单元；若成功，返回 *framework.QueuedUnitInfo；若失败（如队列空或出错），返回 nil。
func MakeNextUnitFunc(queue SchedulingQueue) func() *framework.QueuedUnitInfo {
	return func() *framework.QueuedUnitInfo {
		// 从调度队列中弹出下一个待调度单元（原子操作，通常线程安全）
		unitInfo, err := queue.Pop()
		
		// 【调试日志】临时用于确认是否调用了此函数（开发/排查阶段可移除）
		klog.Info("检查一下是否是这个next函数")

		if err == nil {
			// 成功获取到调度单元，记录日志：即将尝试调度该单元，包含 Pod 数量和唯一标识
			klog.InfoS("Ready to try and schedule the next unit",
				"numberOfPods", unitInfo.NumPods(),
				"unitKey", unitInfo.UnitKey)

			// 遍历该单元中的所有 Pod，完成其“排队阶段”的追踪逻辑：
			for _, info := range unitInfo.GetPods() {
				// 清除 Pod 的当前队列阶段标记（如从 "ReadyQ" 或 "WaitingQ" 变为 "-"）
				info.UpdateQueueStage("-")

				// 如果该 Pod 有活跃的排队追踪 Span（QueueSpan），则结束该 Span
				if info.QueueSpan != nil {
					// 结束 "SchedulerPendingInQueue" 阶段的追踪 Span，
					// 使用 Pod 的上下文和入队时间戳完成埋点，
					// 便于计算 Pod 在调度队列中的等待时长。
					info.QueueSpan.FinishSpan(
						tracing.SchedulerPendingInQueueSpan,
						tracing.GetSpanContextFromPod(info.Pod),
						info.Timestamp,
					)
				}
			}

			// 返回成功弹出的调度单元
			return unitInfo
		}

		// 队列为空或发生错误（如队列关闭、并发冲突等），记录错误并返回 nil
		klog.InfoS("Failed to retrieve the next unit for scheduling", "err", err)
		return nil
	}
}

func alwaysFalse(_, _ interface{}) bool {
	return false
}

func InitUnitQueueSortPlugin(spec *framework.PluginSpec, pluginArgs map[string]*config.PluginConfig) (framework.UnitQueueSortPlugin, error) {
	if spec == nil {
		return nil, fmt.Errorf("queue unit sort plugin not specified")
	}
	plName := spec.GetName()
	factory, ok := UnitSortPluginRegistry[plName]
	if !ok {
		return nil, fmt.Errorf("unregiestered queue unit sort plugin: %v", plName)
	}
	if pluginArgs[plName] != nil {
		return factory(pluginArgs[plName].Args.Object)
	}
	return factory(nil)
}

type SortPluginFactory = func(runtime.Object) (framework.UnitQueueSortPlugin, error)

var UnitSortPluginRegistry = map[string]SortPluginFactory{
	unitqueuesort.FCFSName: unitqueuesort.NewFCFS,
	unitqueuesort.Name:     unitqueuesort.New,
}
