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
	"time"

	"k8s.io/klog/v2"

	"k8s.io/kubernetes/godel-pkg/common/metrics"
)

type PendingQueue interface {
	// AddPodInfo will add pod to queue if not exists. If exists, update it
	AddPodInfo(podInfo *QueuedPodInfo) error
	// UpdatePodInfo will update pod in queue if exists. If not exists, add it
	UpdatePodInfo(podInfo *QueuedPodInfo) error
	// RemovePodInfo will remove pod from the queue if exists
	RemovePodInfo(podInfo *QueuedPodInfo) error
	// Pop will pop pod from the queue
	Pop() ([]*QueuedPodInfo, error)
	// Close the queue
	Close()
}

type PendingFIFO struct {
	fifo *MetricsFIFO
}

var _ = PendingQueue(&PendingFIFO{})

func NewPendingFIFO(metricRecorder metrics.MetricRecorder) *PendingFIFO {
	fifo := &PendingFIFO{
		fifo: NewMetricsFIFO(metricRecorder, func(old, new interface{}) {
			existed, ok := old.(*QueuedPodInfo)
			if !ok {
				klog.InfoS("Failed to parse old object to *QueuedPodInfo", "oldObject", old)
				return
			}
			podInfo, ok := new.(*QueuedPodInfo)
			if !ok {
				klog.InfoS("Failed to parse new object to *QueuedPodInfo", "newObject", new)
				return
			}
			podInfo.Timestamp = existed.Timestamp
			podInfo.InitialAddedTimestamp = existed.InitialAddedTimestamp
		}),
	}

	return fifo
}

// AddPodInfo 将 Pod 信息添加到待处理队列中
// 该方法负责设置时间戳并将其加入内部的 FIFO 队列
func (p *PendingFIFO) AddPodInfo(podInfo *QueuedPodInfo) error {
	// 将 Pod 信息添加到内部的 FIFO 队列中
	// FIFO 队列确保 Pod 按照先进先出的顺序被处理
	return p.fifo.Add(podInfo)
}

func (p *PendingFIFO) UpdatePodInfo(podInfo *QueuedPodInfo) error {
	now := time.Now()
	if podInfo.Timestamp.IsZero() {
		podInfo.Timestamp = now
	}
	if podInfo.InitialAddedTimestamp.IsZero() {
		podInfo.InitialAddedTimestamp = now
	}

	return p.fifo.Update(podInfo)
}

func (p *PendingFIFO) RemovePodInfo(podInfo *QueuedPodInfo) error {
	return p.fifo.Delete(podInfo)
}

func (p *PendingFIFO) Pop() ([]*QueuedPodInfo, error) {
	result, err := p.fifo.Pop()
	if err != nil {
		return nil, err
	}
	return []*QueuedPodInfo{result.(*QueuedPodInfo)}, err
}

func (p *PendingFIFO) Close() {
	p.fifo.Close()
}
