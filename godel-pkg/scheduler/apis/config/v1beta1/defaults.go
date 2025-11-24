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

package v1beta1

import (
	"net"
	"strconv"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog"

	utilpointer "k8s.io/utils/pointer"

	defaultsconfig "k8s.io/kubernetes/godel-pkg/apis/config"
	"k8s.io/kubernetes/godel-pkg/scheduler/apis/config"
	"k8s.io/kubernetes/godel-pkg/util/tracing"
)

func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_GodelSchedulerConfiguration 为 GodelSchedulerConfiguration 对象设置默认值。
// 该函数会为调度器的各个配置项（如选举、客户端连接、健康检查地址、调试配置、调度器名称、调度策略等）
// 设置合理的默认值，以确保调度器在没有显式配置的情况下也能正常运行。
// 这些默认值通常定义在 config 包中，由常量或函数提供。
// 此函数通常由代码生成工具（如 defaulter-gen）自动生成，并通过 RegisterDefaults 注册到 runtime.Scheme 中。
func SetDefaults_GodelSchedulerConfiguration(obj *GodelSchedulerConfiguration) {
	// 1. LeaderElection & SchedulerRenewIntervalSeconds
	klog.Info("使用v1-beta版本")
	{
		// 应用默认的领导者选举配置
		defaultsconfig.SetDefaultLeaderElectionConfiguration(&obj.LeaderElection)
		// 如果未指定领导者选举资源名称，则使用默认调度器名称
		if len(obj.LeaderElection.ResourceName) == 0 {
			obj.LeaderElection.ResourceName = config.DefaultSchedulerName
		}
		// 如果未指定调度器续期间隔，则使用默认值
		if obj.SchedulerRenewIntervalSeconds == 0 {
			obj.SchedulerRenewIntervalSeconds = config.DefaultRenewIntervalInSeconds
		}
	}
	// 2. ClientConnection and BindSetting
	{
		// 如果未指定客户端连接内容类型，则使用 Protobuf（性能更好）
		if len(obj.ClientConnection.ContentType) == 0 {
			obj.ClientConnection.ContentType = "application/vnd.kubernetes.protobuf"
		}
		// 设置调度器专用的客户端 QPS 和突发限制默认值（而非通用默认值）
		if obj.ClientConnection.QPS == 0.0 {
			obj.ClientConnection.QPS = config.DefaultClientConnectionQPS
		}
		if obj.ClientConnection.Burst == 0 {
			obj.ClientConnection.Burst = config.DefaultClientConnectionBurst
		}
		// 处理健康检查绑定地址的默认值和格式化
		// 规则：1. 空值 -> 默认地址 0.0.0.0:port；2. 仅有端口 ":1234" -> 0.0.0.0:1234；3. 仅有IP -> IP:默认端口；4. 其他错误 -> 默认地址
		if len(obj.HealthzBindAddress) == 0 {
			obj.HealthzBindAddress = config.DefaultBindAddress
		} else {
			if host, port, err := net.SplitHostPort(obj.HealthzBindAddress); err == nil {
				if len(host) == 0 {
					host = config.DefaultGodelSchedulerAddress // 如果主机部分为空，使用默认主机地址
				}
				hostPort := net.JoinHostPort(host, port)
				obj.HealthzBindAddress = hostPort
			} else {
				// 解析地址失败时，检查是否为有效 IP
				if host := net.ParseIP(obj.HealthzBindAddress); host != nil {
					hostPort := net.JoinHostPort(obj.HealthzBindAddress, strconv.Itoa(config.DefaultInsecureSchedulerPort))
					obj.HealthzBindAddress = hostPort
				} else {
					// TODO: 在 godelschedulerconfig 中，此处应返回错误而不是强制使用默认值
					obj.HealthzBindAddress = config.DefaultBindAddress
				}
			}
		}

		// 处理指标绑定地址，逻辑与健康检查地址相同
		if len(obj.MetricsBindAddress) == 0 {
			obj.MetricsBindAddress = config.DefaultBindAddress
		} else {
			if host, port, err := net.SplitHostPort(obj.MetricsBindAddress); err == nil {
				if len(host) == 0 {
					host = config.DefaultGodelSchedulerAddress
				}
				hostPort := net.JoinHostPort(host, port)
				obj.MetricsBindAddress = hostPort
			} else {
				// 解析地址失败时，检查是否为有效 IP
				if host := net.ParseIP(obj.MetricsBindAddress); host != nil {
					hostPort := net.JoinHostPort(obj.MetricsBindAddress, strconv.Itoa(config.DefaultInsecureSchedulerPort))
					obj.MetricsBindAddress = hostPort
				} else {
					// TODO: 在 godelschedulerconfig 中，此处应返回错误而不是强制使用默认值
					obj.MetricsBindAddress = config.DefaultBindAddress
				}
			}
		}
	}
	// 3. DebuggingConfiguration
	{
		// 默认启用性能分析（profiling），便于调试和性能监控
		if obj.EnableProfiling == nil {
			enableProfiling := true
			obj.EnableProfiling = &enableProfiling
		}

		// 如果启用了性能分析，则默认也启用竞争分析（contention profiling）
		if *obj.EnableProfiling && obj.EnableContentionProfiling == nil {
			enableContentionProfiling := true
			obj.EnableContentionProfiling = &enableContentionProfiling
		}
	}

	// 4. Godel Scheduler
	{
		// 如果未指定 Godel 调度器名称，则使用默认值
		if len(obj.GodelSchedulerName) == 0 {
			obj.GodelSchedulerName = config.DefaultGodelSchedulerName
		}
		// 如果未指定调度器名称，则使用默认值（使用指针以支持 nil 检查）
		if obj.SchedulerName == nil {
			defaultValue := config.DefaultSchedulerName
			obj.SchedulerName = &defaultValue
		}
		// 如果未指定子集群键，则使用默认值
		if obj.SubClusterKey == nil {
			defaultValue := config.DefaultSubClusterKey
			obj.SubClusterKey = &defaultValue
		}
		// 如果未指定追踪器配置，则使用默认的无操作追踪器
		if obj.Tracer == nil {
			obj.Tracer = tracing.DefaultNoopOptions()
		}
		// 如果未指定预约超时时间或设置为非正值，则使用默认值
		if obj.ReservationTimeOutSeconds <= 0 {
			obj.ReservationTimeOutSeconds = config.DefaultReservationTimeOutSeconds
		}
	}
	// 5. Godel Profiles
	{
		// 如果未指定默认调度策略配置，则创建一个空的配置
		if obj.DefaultProfile == nil {
			obj.DefaultProfile = &GodelSchedulerProfile{}
		}
		// 为默认策略配置设置各项默认值
		if obj.DefaultProfile.PercentageOfNodesToScore == nil {
			percentageOfNodesToScore := int32(config.DefaultPercentageOfNodesToScore)
			obj.DefaultProfile.PercentageOfNodesToScore = &percentageOfNodesToScore
		}
		if obj.DefaultProfile.IncreasedPercentageOfNodesToScore == nil {
			increasedPercentageOfNodesToScore := int32(config.DefaultIncreasedPercentageOfNodesToScore)
			obj.DefaultProfile.IncreasedPercentageOfNodesToScore = &increasedPercentageOfNodesToScore
		}
		if obj.DefaultProfile.UnitInitialBackoffSeconds == nil {
			defaultUnitInitialBackoffInSeconds := int64(config.DefaultUnitInitialBackoffInSeconds)
			obj.DefaultProfile.UnitInitialBackoffSeconds = &defaultUnitInitialBackoffInSeconds
		}
		if obj.DefaultProfile.UnitMaxBackoffSeconds == nil {
			defaultUnitMaxBackoffInSeconds := int64(config.DefaultUnitMaxBackoffInSeconds)
			obj.DefaultProfile.UnitMaxBackoffSeconds = &defaultUnitMaxBackoffInSeconds
		}
		if obj.DefaultProfile.AttemptImpactFactorOnPriority == nil {
			attemptImpactFactorOnPriority := config.DefaultAttemptImpactFactorOnPriority
			obj.DefaultProfile.AttemptImpactFactorOnPriority = &attemptImpactFactorOnPriority
		}
		// 如果未设置是否禁用抢占，则默认不禁用（即允许抢占）
		if obj.DefaultProfile.DisablePreemption == nil {
			obj.DefaultProfile.DisablePreemption = utilpointer.BoolPtr(config.DefaultDisablePreemption)
		}
		// 如果未设置是否启用阻塞队列，则默认不启用
		if obj.DefaultProfile.BlockQueue == nil {
			obj.DefaultProfile.BlockQueue = utilpointer.BoolPtr(config.DefaultBlockQueue)
		}
		// 如果未指定最大等待删除持续时间，则使用默认值
		if obj.DefaultProfile.MaxWaitingDeletionDuration == 0 {
			obj.DefaultProfile.MaxWaitingDeletionDuration = config.DefaultMaxWaitingDeletionDuration
		}
	}
}
