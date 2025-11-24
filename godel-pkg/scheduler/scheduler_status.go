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
	"fmt"
	"time"

	"github.com/kubewharf/godel-scheduler-api/pkg/apis/scheduling/v1alpha1"
	godelclient "github.com/kubewharf/godel-scheduler-api/pkg/client/clientset/versioned"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/godel-pkg/util"
)

const (
	// maxUpdateRetries is the number of immediate, successive retries the Scheduler will attempt
	// when renewing the Scheduler status before it waits for the renewal interval before trying again,
	// similar to what we do for node status retries
	maxUpdateRetries = 5
	// sleep is the default interval for retry
	sleep = 100 * time.Millisecond
)

// StatusMaintainer manages creating and renewing the status for this Scheduler
type StatusMaintainer interface {
	Run(stopCh <-chan struct{})
}

type maintainer struct {
	crdClient     godelclient.Interface
	schedulerName string
	renewInterval time.Duration
	clock         clock.Clock
}

// NewSchedulerStatusMaintainer constructs and returns a maintainer
func NewSchedulerStatusMaintainer(clock clock.Clock, client godelclient.Interface, schedulerName string, renewIntervalSeconds int64) StatusMaintainer {
	renewInterval := time.Duration(renewIntervalSeconds) * time.Second
	return &maintainer{
		crdClient:     client,
		schedulerName: schedulerName,
		renewInterval: renewInterval,
		clock:         clock,
	}
}

// Run 启动调度器状态维护器的主循环。
// 它会定期同步调度器的状态信息到自定义资源定义（CRD）中，直到收到停止信号。
func (c *maintainer) Run(stopCh <-chan struct{}) {
	// 检查 CRD 客户端是否已初始化。
	// CRD 客户端用于与存储调度器状态的 CRD 进行交互。
	if c.crdClient == nil {
		// 如果客户端为 nil，记录错误日志并退出程序。
		// 这通常意味着配置错误或依赖注入失败。
		klog.Info("c.crdClient为nil")
		klog.ErrorS(nil, "Exited the scheduler status maintainer because the CRD client was nil", "schedulerName", c.schedulerName)
		// 确保日志被刷新到输出，然后以退出码 1 结束程序。
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}
	// 启动一个无限循环，定期执行同步操作。
	// wait.Until 会按照 c.renewInterval 指定的时间间隔，调用 c.sync 方法。
	// 当 stopCh 通道被关闭时，循环会停止，Run 方法返回。
	klog.Info("c.crdClient不为nil")
	wait.Until(c.sync, c.renewInterval, stopCh)
}

// sync attempts to update the status for Scheduler
// update Status.LastUpdateTime at the moment
// sync 同步调度器的当前状态到其对应的 CRD（自定义资源定义）中。
// 它尝试更新 CRD 对象以反映调度器的最新状态（如健康状况、活跃节点列表等）。
// 如果更新失败，它会记录一条信息日志，然后在下次定时周期重试。
func (c *maintainer) sync() {
	// 调用 ensureSchedulerUpToDate 函数来执行实际的状态更新逻辑。
	// 该函数会使用 c.crdClient 与 API Server 通信，更新与 c.schedulerName 相关的 CRD 状态。
	if err := ensureSchedulerUpToDate(c.crdClient, c.clock, c.schedulerName); err != nil {
		// 如果更新失败（例如网络问题、权限不足、资源冲突等），
		// 记录一条 Info 级别的日志，告知用户更新失败，并将在下一次 sync 周期（由 c.renewInterval 控制）重试。
		klog.InfoS("Failed to update scheduler status, will retry later", "schedulerName", c.schedulerName, "renewInterval", c.renewInterval)
	}
	// 如果更新成功，函数静默返回，等待下一次定时调用。
}

// ensureSchedulerUpToDate try to update scheduler status, if failed, retry after sleep duration, at most maxUpdateRetries
// ensureSchedulerUpToDate 确保调度器的状态在 CRD 中保持最新。
// 它会尝试最多 maxUpdateRetries 次调用 updateSchedulerStatus 来更新状态。
// 如果所有重试都失败，则返回一个错误。
func ensureSchedulerUpToDate(client godelclient.Interface, clock clock.Clock, schedulerName string) error {
	// 循环最多 maxUpdateRetries 次，尝试更新调度器状态。
	for i := 0; i < maxUpdateRetries; i++ {
		// 调用 updateSchedulerStatus 尝试更新调度器在 CRD 中的状态。
		err := updateSchedulerStatus(client, schedulerName)
		klog.Info("更新一次调度器资源")
		if err != nil {
			// 如果更新失败，记录一条 Info 日志，包含错误信息。
			klog.InfoS("Failed to update scheduler, will retry later", "schedulerName", schedulerName, "err", err)
			// 使用 clock.Sleep 暂停 sleep 时间，然后再进行下一次重试。
			// 这可以避免在失败时进行过于频繁的 API 调用。
			clock.Sleep(sleep)
			// 继续下一次循环尝试更新。
			continue
		}
		// 如果更新成功（err 为 nil），则退出函数，返回 nil。
		return nil
	}
	// 如果循环结束仍未成功（即 maxUpdateRetries 次都失败了），
	// 返回一个格式化的错误，表明所有重试都已用尽。
	return fmt.Errorf("failed %d attempts to update scheduler status", maxUpdateRetries)
}

// updateSchedulerStatus tries to update Scheduler status to apiserver, if Scheduler not exists, add new Scheduler to apiserver
// updateSchedulerStatus 更新或创建指定名称的调度器 CRD 对象及其状态。
// 它会尝试获取现有的调度器 CRD，如果存在则更新其状态中的最后更新时间；
// 如果不存在，则创建一个新的调度器 CRD 对象，并随后更新其状态。
func updateSchedulerStatus(client godelclient.Interface, schedulerName string) error {
	// 获取当前时间，用于记录最后更新时间。
	now := metav1.Now()

	//schedulerName+="-my-custom"
	// 尝试从 API Server 获取指定名称的调度器 CRD 对象。
	existed, err := util.GetScheduler(client, schedulerName)
	if err == nil && existed != nil {
		// 如果获取成功且对象存在。
		// 创建一个现有对象的深拷贝，以避免修改原始对象。
		updated := existed.DeepCopy()
		// 更新深拷贝对象的状态，设置最后更新时间为当前时间。
		updated.Status.LastUpdateTime = &now

		// 调用工具函数更新调度器 CRD 的状态子资源。
		if _, err := util.UpdateSchedulerStatus(client, updated); err != nil {
			// 如果更新状态失败，包装错误信息并返回，以便调用方进行重试。
			err = fmt.Errorf("failed to update scheduler %v, will retry later, error is %v", schedulerName, err)
			return err
		}
		// 更新成功，返回 nil。
		return nil
	}

	// 检查获取失败的原因是否是因为对象不存在 (IsNotFound)。
	// 如果不是 NotFound 错误，说明是其他问题（如网络、权限等），记录错误并返回。
	if !apierrors.IsNotFound(err) {
		err = fmt.Errorf("failed to get scheduler %v, will retry later, error is %v", schedulerName, err)
		return err
	}

	// 如果错误是 NotFound (apierrors.IsNotFound(err) 为 true)，
	// 表示调度器 CRD 对象不存在。记录警告日志。
	klog.InfoS("WARN: scheduler was gone, should check this", "schedulerName", schedulerName, "err", err)

	// 准备创建一个新的调度器 CRD 对象。
	schedulerCRD := &v1alpha1.Scheduler{
		ObjectMeta: metav1.ObjectMeta{
			Name: schedulerName, // 设置对象名称。
		},
		// Status 字段可能在创建时是空的，稍后会单独更新。
	}

	// 调用工具函数创建调度器 CRD 对象。
	created, err := util.PostScheduler(client, schedulerCRD)
	if err != nil {
		// 检查创建失败的原因是否是因为对象已存在 (IsAlreadyExists)。
		// 这可能发生在并发场景下。
		if apierrors.IsAlreadyExists(err) {
			// 如果是因为已存在，记录警告日志，但不视为错误，返回 nil。
			// 这可能意味着在检查和创建之间，另一个实例已经创建了该对象。
			klog.InfoS("WARN: skipped register because scheduler already existed", "schedulerName", schedulerName, "err", err)
			return nil
		}
		// 如果是其他创建错误，包装错误信息并返回。
		err = fmt.Errorf("failed to update scheduler %v, will retry later, error is %v", schedulerName, err)
		return err
	}

	// 创建成功。通常，创建对象时无法直接设置 status 子资源。
	// 因此，需要单独更新 status。
	// 更新刚创建对象的状态，设置最后更新时间为当前时间。
	created.Status.LastUpdateTime = &now
	// 调用工具函数更新调度器 CRD 的状态子资源。
	if _, err := util.UpdateSchedulerStatus(client, created); err != nil {
		// 如果更新状态失败，包装错误信息并返回。
		err = fmt.Errorf("failed to update scheduler %v, will retry later, error is %v", schedulerName, err)
		return err
	}
	// 成功创建并更新状态，返回 nil。
	return nil
}
