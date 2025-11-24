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

package scheduler_maintainer

import (
	"context"
	"sync"
	"time"

	crdclient "github.com/kubewharf/godel-scheduler-api/pkg/client/clientset/versioned"
	schedulerlister "github.com/kubewharf/godel-scheduler-api/pkg/client/listers/scheduling/v1alpha1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	sche "k8s.io/kubernetes/godel-pkg/dispatcher/internal/scheduler"
)

// SchedulerMaintainer 是一个管理 Godel 调度器实例的结构体。
// 它负责调度器的生命周期管理，包括创建、更新、删除以及与 CRD 的交互。
type SchedulerMaintainer struct {
	// schedulerMux 是一个读写互斥锁，用于保护对 generalSchedulers 映射的并发访问。
	// 任何对 generalSchedulers 的读写操作都需要获取此锁，确保线程安全。
	schedulerMux sync.RWMutex

	// NodePartitionType 指示节点分区的类型，可以是物理分区（physical）或逻辑分区（logical）。
	// 这个字段决定了调度器如何理解和管理集群中的节点分组。
	// TODO: figure out if we need a separate CRD for Node Partition
	//       当前 NodePartitionType 作为一个字符串字段存储在此结构体中。
	//       未来可能需要考虑将其抽象为一个独立的 CRD (Custom Resource Definition)，
	//       以便更灵活地管理和配置不同的分区策略。
	NodePartitionType string

	// crdClient 是一个接口，用于与 Godel 自定义资源（如 Scheduler CRD）进行交互。
	// 它提供了创建、读取、更新、删除和监听这些资源的方法。
	crdClient crdclient.Interface

	// schedulerLister 是一个缓存的只读客户端，用于快速查询本地缓存中的 Scheduler 资源对象。
	// 它通常与一个控制器（如 Informer）配合使用，以保持缓存与 API Server 的同步。
	schedulerLister schedulerlister.SchedulerLister

	// TODO: support customized schedulers
	// customizedSchedulers 是一个存储自定义调度器的映射。
	// 这些调度器可能有特定的要求，例如特定的任务选择器或节点选择器。
	// 满足这些自定义调度器节点选择器的节点，将不再由通用调度器管理。
	// TODO: we may need to specify/adjust preemption policy for this later
	//       将来可能需要为这些自定义调度器指定或调整抢占策略。
	// 注意：此字段当前被注释掉了，表示该功能尚未实现。
	// customizedSchedulers map[string]*sche.GodelScheduler

	// TODO: add a more fine-grained lock for generalSchedulers later if we want to remove schedulerMux
	// generalSchedulers 是一个映射，存储所有通用调度器实例。
	// 通用调度器是指那些不是为特定 Pod 或节点设计的调度器。
	// 这些调度器通常负责处理集群中大部分的 Pod 调度请求。
	// 对此映射的 CRUD (创建、读取、更新、删除) 操作必须是原子的，需要使用 schedulerMux 锁进行保护。
	// TODO: 如果未来希望移除全局的 schedulerMux 锁，可以考虑为此映射添加更细粒度的锁，
	//       例如为每个调度器实例单独加锁，以提高并发性能。
	generalSchedulers map[string]*sche.GodelScheduler
}

// NewSchedulerMaintainer creates a new SchedulerMaintainer struct object
func NewSchedulerMaintainer(crdClient crdclient.Interface, schedulerLister schedulerlister.SchedulerLister) *SchedulerMaintainer {
	return &SchedulerMaintainer{
		NodePartitionType: string(Logical),
		crdClient:         crdClient,
		schedulerLister:   schedulerLister,
		generalSchedulers: make(map[string]*sche.GodelScheduler),
	}
}

// Run runs all necessary workers
func (maintainer *SchedulerMaintainer) Run(stopCh <-chan struct{}) {
	// populate schedulers periodically
	go wait.Until(maintainer.PopulateSchedulers, 1*time.Minute, stopCh)

	go wait.Until(maintainer.SyncUpSchedulersStatus, 30*time.Second, stopCh)

	<-stopCh
}

// PopulateSchedulers will populate existing schedulers to active queue
func (maintainer *SchedulerMaintainer) PopulateSchedulers() {
	schedulers, err := maintainer.schedulerLister.List(labels.Everything())
	if err != nil {
		klog.InfoS("Failed to list schedulers", "err", err)
	}
	// add existing schedulers
	for _, scheduler := range schedulers {
		maintainer.AddScheduler(scheduler)
	}
}

// SyncUpSchedulersStatus will be responsible for syncing up schedulers between active queue and inactive queue
// TODO: we need to handle this scenario: schedulers exists in both active queue and inactive queue
// be careful about the race condition when we handle the scenario above
// we can delete schedulers from one of the queues based on schedulers actual status
func (maintainer *SchedulerMaintainer) SyncUpSchedulersStatus() {
	activeSchedulers := maintainer.GetActiveSchedulers()
	for _, schedulerName := range activeSchedulers {
		scheduler, err := maintainer.schedulerLister.Get(schedulerName)
		if err != nil && !errors.IsNotFound(err) {
			klog.InfoS("Failed to get the schedulers CRD", "schedulerName", schedulerName, "err", err)
			continue
		}
		if errors.IsNotFound(err) {
			maintainer.DeactivateScheduler(schedulerName)
			continue
		}
		if !IsSchedulerActive(scheduler) {
			klog.V(3).InfoS("Started to delete the inactive schedulers", "schedulerName", schedulerName)
			// schedulers is still there and it is not active, delete it.
			err := maintainer.crdClient.SchedulingV1alpha1().Schedulers().Delete(context.TODO(), scheduler.Name, metav1.DeleteOptions{})
			if err != nil {
				klog.InfoS("Failed to delete the inactive schedulers", "schedulerName", scheduler.Name, "err", err)
			}
		}
	}

	inactiveSchedulers := maintainer.GetInactiveSchedulers()
	for _, schedulerName := range inactiveSchedulers {
		scheduler, err := maintainer.schedulerLister.Get(schedulerName)
		if err != nil && !errors.IsNotFound(err) {
			klog.InfoS("Failed to get the schedulers CRD", "schedulerName", schedulerName, "err", err)
			continue
		}
		if errors.IsNotFound(err) {
			klog.V(4).InfoS("The schedulers didn't exist any more", "schedulerName", schedulerName)
			continue
		}
		if IsSchedulerActive(scheduler) {
			maintainer.ActivateScheduler(scheduler.Name)
		} else {
			klog.V(3).InfoS("Started to delete the inactive schedulers", "schedulerName", schedulerName)
			// schedulers is still there and it is not active, delete it.
			err := maintainer.crdClient.SchedulingV1alpha1().Schedulers().Delete(context.TODO(), scheduler.Name, metav1.DeleteOptions{})
			if err != nil {
				klog.InfoS("Failed to delete the inactive schedulers", "schedulerName", scheduler.Name, "err", err)
			}
		}
	}
}

func (maintainer *SchedulerMaintainer) CleanupInActiveSchedulers() {
	// TODO: if number of nodes in inactive schedulers's partition is 0, remove this inactive schedulers
}

func (maintainer *SchedulerMaintainer) SyncupNodePartitionType() {
	// TODO: implement dynamic node partition type switching
	// TODO: based on node resource usage water level ?
}
