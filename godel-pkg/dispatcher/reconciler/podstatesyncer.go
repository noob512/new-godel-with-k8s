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

package reconciler

import (
	"context"
	"time"

	nodelisterv1alpha1 "github.com/kubewharf/godel-scheduler-api/pkg/client/listers/node/v1alpha1"
	schedulerv1alpha1 "github.com/kubewharf/godel-scheduler-api/pkg/client/listers/scheduling/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	listerv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/godel-pkg/dispatcher/internal/store"
	schemaintainer "k8s.io/kubernetes/godel-pkg/dispatcher/scheduler-maintainer"
	podutil "k8s.io/kubernetes/godel-pkg/util/pod"
)

// PodStateReconciler stores all abnormal state pods and try to reset pod state
type PodStateReconciler struct {
	client                   kubernetes.Interface
	podLister                listerv1.PodLister
	nodeLister               listerv1.NodeLister
	schedulerLister          schedulerv1alpha1.SchedulerLister
	nmNodeLister             nodelisterv1alpha1.NMNodeLister
	abnormalPodsQueue        workqueue.Interface
	staleDispatchedPodsQueue workqueue.Interface
	schedulerName            string

	dispatchedPodsStore store.DispatchInfo

	schedulerMaintainer *schemaintainer.SchedulerMaintainer
	populator           *DispatchedPodsPopulator
}

// NewPodStateReconciler 创建一个新的 PodStateReconciler 实例
// PodStateReconciler 负责同步 Pod 在 API 服务器和调度器内部状态之间的差异，确保调度器的内部视图与集群实际状态一致
func NewPodStateReconciler(
	client kubernetes.Interface, // Kubernetes 标准客户端，用于与 API 服务器交互
	podLister listerv1.PodLister, // Pod 列表器，提供缓存的 Pod 数据访问
	nodeLister listerv1.NodeLister, // Node 列表器，提供缓存的 Node 数据访问
	schedulerLister schedulerv1alpha1.SchedulerLister, // Scheduler 列表器，提供缓存的 Scheduler CRD 数据访问
	schedulerName string, // 调度器名称，用于标识当前调度器实例
	dispatchedPodsStore store.DispatchInfo, // 已调度 Pod 的存储，记录调度器内部的 Pod 调度状态
	maintainer *schemaintainer.SchedulerMaintainer, // 调度器维护器，用于管理 Scheduler CRD 状态
) *PodStateReconciler {
	// 创建一个工作队列，用于存放状态过期的已调度 Pod，这些 Pod 需要被重新检查和同步
	staleDispatchedPodsQueue := workqueue.NewNamed("stale-dispatched-pods-queue")
	// 创建已调度 Pod 的填充器，用于从 API 服务器同步已调度的 Pod 信息到内部存储
	populator := NewDispatchedPodsPopulator(schedulerName, podLister, staleDispatchedPodsQueue, maintainer)

	// 返回初始化的 PodStateReconciler 实例
	return &PodStateReconciler{
		// 调度器名称
		schedulerName: schedulerName,
		// Kubernetes 客户端
		client: client,
		// Pod 列表器
		podLister: podLister,
		// Node 列表器
		nodeLister: nodeLister,
		// Scheduler 列表器
		schedulerLister: schedulerLister,
		// NMNode 列表器
		nmNodeLister: nil,
		// 异常 Pod 队列，用于存放状态异常的 Pod，需要进行状态同步处理
		abnormalPodsQueue: workqueue.NewNamed("abnormal-pods-queue"),
		// 状态过期的已调度 Pod 队列
		staleDispatchedPodsQueue: staleDispatchedPodsQueue,
		// 已调度 Pod 填充器
		populator: populator,
		// 调度器维护器
		schedulerMaintainer: maintainer,
		// 已调度 Pod 存储
		dispatchedPodsStore: dispatchedPodsStore,
	}
}

// Run runs pod state syncer worker
func (psr *PodStateReconciler) Run(stop <-chan struct{}) {
	go wait.Until(psr.AbnormalStatePodsSyncer, time.Second, stop)
	go wait.Until(psr.StaleDispatchedPodsSyncer, time.Second, stop)

	go psr.populator.Run(stop)
}

// AbnormalPodsEnqueue adds obj to abnormal queue
func (psr *PodStateReconciler) AbnormalPodsEnqueue(obj interface{}) {
	psr.abnormalPodsQueue.Add(obj)
}

// StaleDispatchedPodsEnqueue adds obj to stale dispatched queue
func (psr *PodStateReconciler) StaleDispatchedPodsEnqueue(obj interface{}) {
	psr.staleDispatchedPodsQueue.Add(obj)
}

func (psr *PodStateReconciler) StaleDispatchedPodsSyncer() {
	workFunc := func() bool {
		podKeyObj, quit := psr.staleDispatchedPodsQueue.Get()
		if quit {
			return true
		}
		defer psr.staleDispatchedPodsQueue.Done(podKeyObj)
		podKey := podKeyObj.(string)

		namespace, name, err := cache.SplitMetaNamespaceKey(podKey)
		if err != nil {
			klog.InfoS("Failed to get namespace & name of the pod from informer", "pod", podKey, "err", err)
			return false
		}

		klog.V(3).InfoS("The StaleDispatchedPodsSyncer started to process", "pod", klog.KRef(namespace, name))

		pod, err := psr.podLister.Pods(namespace).Get(name)
		if err == nil {
			// The pod still exists in informer cache
			if err := psr.updateStaleDispatchedStatePod(pod); err != nil {
				// re-add the pod to the queue
				psr.staleDispatchedPodsQueue.Add(podKey)
			}
			return false
		}
		if !errors.IsNotFound(err) {
			klog.InfoS("Failed to get the pod from informer", "pod", podKey, "err", err)
			// re-add the pod to the queue
			psr.staleDispatchedPodsQueue.Add(podKey)
			return false
		}

		// if err is Not Found, the pod should have been deleted, return directly
		return false
	}

	for {
		if quit := workFunc(); quit {
			klog.InfoS("Shut down the worker queue for the stale dispatched pods syncer")
			return
		}
	}
}

func (psr *PodStateReconciler) updateStaleDispatchedStatePod(pod *corev1.Pod) error {
	if podutil.DispatchedPodOfGodel(pod, psr.schedulerName) {
		schedulerName := pod.Annotations[podutil.SchedulerAnnotationKey]
		if psr.schedulerMaintainer.IsSchedulerInInactiveQueue(schedulerName) || !psr.schedulerMaintainer.SchedulerExist(schedulerName) {
			klog.V(3).InfoS("Reset the dispatched pod to Pending state on inactive/nonexistent scheduler", "pod", klog.KObj(pod), "schedulerName", schedulerName)
			return psr.resetPodToPendingState(pod)
		}
		return nil
	}
	// if pod is not dispatched now, ignore this pod
	return nil
}

// AbnormalStatePodsSyncer tries reset abnormal state pods
func (psr *PodStateReconciler) AbnormalStatePodsSyncer() {
	workFunc := func() bool {
		podKeyObj, quit := psr.abnormalPodsQueue.Get()
		if quit {
			return true
		}
		defer psr.abnormalPodsQueue.Done(podKeyObj)
		podKey := podKeyObj.(string)

		namespace, name, err := cache.SplitMetaNamespaceKey(podKey)
		if err != nil {
			klog.InfoS("Failed to get namespace & name of the pod from informer", "pod", podKey, "err", err)
			return false
		}

		klog.V(3).InfoS("The AbnormalStatePodsSyncer started to process", "pod", klog.KRef(namespace, name))

		pod, err := psr.podLister.Pods(namespace).Get(name)
		if err == nil {
			// The pod still exists in informer cache
			if err := psr.updateAbnormalStatePod(pod); err != nil {
				// re-add the pod to the queue
				psr.abnormalPodsQueue.Add(podKey)
			}
			return false
		}
		if !errors.IsNotFound(err) {
			klog.InfoS("Failed to get the pod from informer", "pod", podKey, "err", err)
			// re-add the pod to the queue
			psr.abnormalPodsQueue.Add(podKey)
			return false
		}

		// if err is Not Found, the pod should have been deleted, return directly
		return false
	}

	for {
		if quit := workFunc(); quit {
			klog.InfoS("Shut down the worker queue for the abnormal state pods syncer")
			return
		}
	}
}

// updatePodState tries to update pod state if it is abnormal
func (psr *PodStateReconciler) updateAbnormalStatePod(pod *corev1.Pod) error {
	abnormal := podutil.AbnormalPodStateOfGodel(pod, psr.schedulerName)
	if abnormal {
		klog.V(3).InfoS("Reset the abnormal pod to Pending state", "pod", klog.KObj(pod))
		// pod is still abnormal
		// blindly resetting to pending state
		// TODO: add more fine-grained checking and resetting operations
		return psr.resetPodToPendingState(pod)
	}
	// pod returns back to normal state, return directly
	return nil
}

// resetPodToPendingState resets pod state to Pending
func (psr *PodStateReconciler) resetPodToPendingState(pod *corev1.Pod) error {
	podClone := pod.DeepCopy()
	if podClone.Annotations == nil {
		podClone.Annotations = make(map[string]string)
	}
	podClone.Annotations[podutil.PodStateAnnotationKey] = string(podutil.PodPending)
	delete(podClone.Annotations, podutil.SchedulerAnnotationKey)
	delete(podClone.Annotations, podutil.AssumedNodeAnnotationKey)
	delete(podClone.Annotations, podutil.NominatedNodeAnnotationKey)
	_, err := psr.client.CoreV1().Pods(podClone.Namespace).Update(context.TODO(), podClone, metav1.UpdateOptions{})
	return err
}
