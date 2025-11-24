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
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	framework "github.com/kubewharf/godel-scheduler/pkg/framework/api"
	godelcache "github.com/kubewharf/godel-scheduler/pkg/scheduler/cache"
	"github.com/kubewharf/godel-scheduler/pkg/util"
	podutil "github.com/kubewharf/godel-scheduler/pkg/util/pod"
)

type FailedTaskReconciler struct {
	schedulerName string
	// client syncs K8S object
	client clientset.Interface

	podLister corelisters.PodLister

	failedPatchTaskQueue workqueue.RateLimitingInterface

	schedulerCache godelcache.SchedulerCache

	stop chan struct{}
}

type FailedPatchTask struct {
	podInfo *framework.CachePodInfo
}

func NewFailedPatchTask(podInfo *framework.CachePodInfo) *FailedPatchTask {
	return &FailedPatchTask{podInfo: podInfo}
}

func NewFailedTaskReconciler(client clientset.Interface, podLister corelisters.PodLister,
	schedulerCache godelcache.SchedulerCache, schedulerName string,
) *FailedTaskReconciler {
	return &FailedTaskReconciler{
		schedulerName:        schedulerName,
		client:               client,
		podLister:            podLister,
		failedPatchTaskQueue: workqueue.NewNamedRateLimitingQueue(workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 5*time.Second), "task-queue"),
		schedulerCache:       schedulerCache,
		stop:                 make(chan struct{}),
	}
}

func (re *FailedTaskReconciler) AddFailedTask(fpt *FailedPatchTask) {
	re.failedPatchTaskQueue.Add(fpt)
}

func (re *FailedTaskReconciler) Run() {
	go wait.Until(re.failedPatchTaskWorker, time.Second, re.stop)
}

func (re *FailedTaskReconciler) Close() {
	close(re.stop)
}

// failedPatchTaskWorker 是一个长时间运行的函数，它启动一个工作循环，
// 用于从失败任务队列中获取任务，并尝试重新对 Pod 执行 Patch 操作。
func (re *FailedTaskReconciler) failedPatchTaskWorker() {
	// 定义一个内部工作函数，它执行单个任务的处理逻辑。
	// 返回 true 表示工作循环应该停止（例如，队列已关闭）。
	workFunc := func() bool {
		// 1. 从队列中获取一个失败的任务。
		//    - Get() 是阻塞的，如果队列为空，它会等待直到有任务可用。
		//    - obj 是队列中的原始对象（FailedPatchTask 指针）。
		//    - quit 是一个布尔值，如果队列被关闭（例如，协调器停止），则为 true。
		obj, quit := re.failedPatchTaskQueue.Get()
		if quit {
			// 如果队列已关闭，返回 true，通知外层循环退出。
			return true
		}

		// 2. 确保在函数退出前调用 Done，告诉队列本次处理结束。
		//    这对于处理失败的任务并决定是否重试至关重要。
		defer re.failedPatchTaskQueue.Done(obj)

		// 3. 将队列中获取的对象转换为具体的 *FailedPatchTask 类型。
		//re.failedPatchTaskQueue.Get()：从 “失败补丁任务队列” 中阻塞获取一个待处理任务（obj）。
		//队列中存储的是 FailedPatchTask 类型的对象（包含需要重新补丁的 Pod 信息）
		fpt := obj.(*FailedPatchTask)

		// 4. 检查任务对象中的 Pod 信息是否完整。
		if fpt.podInfo == nil || fpt.podInfo.Pod == nil {
			// 如果 Pod 信息缺失，记录警告日志，并标记此任务处理完成（不重试）。
			klog.InfoS("WARN: the reserved pod was nil")
			return false // 返回 false 继续处理下一个任务
		}

		// 5. 从缓存或 API Server 获取 Pod 的最新状态。
		latestPod, err := re.podLister.Pods(fpt.podInfo.Pod.Namespace).Get(fpt.podInfo.Pod.Name)
		
		// 6. 检查 Pod 的当前状态，以确定是否应继续尝试 Patch。
		//    例如，如果 Pod 已被删除，则无需重试。
		if goOn := re.checkPodState(latestPod, err, fpt); !goOn {
			// checkPodState 返回 false，表示不应继续，处理完成。
			return false
		}

		// 7. 创建一个 Pod 的深拷贝，以便进行修改。
		//    这是为了避免直接修改从 Lister 获取的共享缓存对象。
		clonedPod := latestPod.DeepCopy()
		// 将失败任务中保存的期望注解（Annotations）应用到克隆的 Pod 对象上。
		clonedPod.Annotations = fpt.podInfo.Pod.Annotations

		// 8. 尝试执行 Patch 操作，将修改后的克隆 Pod 的状态同步到 API Server。
		// try to patch again
		err = util.PatchPod(re.client, latestPod, clonedPod)
		if err != nil {
			// 9. Patch 操作失败。
			klog.InfoS("Failed to patch pod in reconciler", "pod", klog.KObj(fpt.podInfo.Pod), "err", err)

			// 检查错误类型。
			if apierrors.IsNotFound(err) {
				// 如果错误是 "Not Found"，说明 Pod 在尝试 Patch 期间被删除了。
				// 这种情况下，将任务重新添加到队列是没有意义的。
				// Do not add back to task queue again.
				return false // 标记处理完成，不重试。
			}

			// 对于其他类型的错误（如网络问题、冲突等），将任务放回队列尾部，等待下次处理。
			// 这通常会增加重试计数，并可能应用指数退避延迟。
			re.failedPatchTaskQueue.Add(fpt)
			return false // 标记本次处理失败，但循环继续（处理下一个任务）
		} else {
			// 10. Patch 操作成功。
			// TODO: add event ? (可以考虑在此处添加一个 Kubernetes Event，记录操作成功)
		}

		// Patch 成功，此任务处理完成。返回 false 继续处理下一个任务。
		return false
	}

	// 11. 启动无限循环，持续调用 workFunc 来处理队列中的任务。
	for {
		// 调用 workFunc 处理一个任务。
		// 如果 workFunc 返回 true (即 quit 为 true)，则退出循环。
		if quit := workFunc(); quit {
			// 当循环因为队列关闭而退出时，记录日志。
			klog.InfoS("Shut down the patch task worker of the FailedTaskReconciler")
			return // 退出 worker 函数。
		}
		// 如果 workFunc 返回 false，循环继续，处理下一个任务。
	}
}

func (re *FailedTaskReconciler) checkPodState(latestPod *v1.Pod, err error, fpt *FailedPatchTask) (goOn bool) {
	if err != nil {
		if apierrors.IsNotFound(err) {
			// this pod is deleted, forget it from cache if it is still in assumed state in cache
			if err := re.schedulerCache.ForgetPod(fpt.podInfo); err != nil {
				klog.InfoS("Failed to forget pod", "pod", podutil.GetPodKey(fpt.podInfo.Pod), "err", err)
				re.failedPatchTaskQueue.Add(fpt)
			}
			return false
		}
		klog.InfoS("Failed to get pod in reconciler", "pod", klog.KObj(fpt.podInfo.Pod), "err", err)
		re.failedPatchTaskQueue.Add(fpt)
		return false
	}

	if !podutil.DispatchedPodOfGodel(latestPod, re.schedulerName) {
		// pod is not in dispatched state, we need to forget the pod from cache no matter what state it is now.
		// if it is assumed or bound now, the pod will be added to cache and removed from assumed pods map
		// if it is pending now, we need to remove this pod from assumed pod map too.
		assumed, err := re.schedulerCache.IsAssumedPod(fpt.podInfo.Pod)
		if err != nil {
			klog.InfoS("Failed to check if this pod is an assumed pod", "pod", podutil.GetPodKey(fpt.podInfo.Pod), "err", err)
			re.failedPatchTaskQueue.Add(fpt)
			return false
		}
		if assumed {
			if err := re.schedulerCache.ForgetPod(fpt.podInfo); err != nil {
				klog.InfoS("Failed to forget pod", "pod", podutil.GetPodKey(fpt.podInfo.Pod), "err", err)
				re.failedPatchTaskQueue.Add(fpt)
			}
		}

		return false
	}

	return true
}
