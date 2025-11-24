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

package config

import (
	"net"
	"strconv"

	defaultsconfig "github.com/kubewharf/godel-scheduler/pkg/apis/config"
	"github.com/kubewharf/godel-scheduler/pkg/util/tracing"
)

const (
	DefaultSchedulerName               = "godel-scheduler"
	DefaultClientConnectionContentType = "application/vnd.kubernetes.protobuf"
	DefaultClientConnectionQPS         = 10000.0
	DefaultClientConnectionBurst       = 10000
	DefaultInsecureBinderPort          = 10351

	DispatcherDefaultLockObjectName = "dispatcher"
)

// SetDefaults 为 GodelDispatcherConfiguration 设置默认值
// 该函数确保配置对象中的各个字段都有合理的默认值，以保证调度器能够正常运行
func SetDefaults(cfg *GodelDispatcherConfiguration) {
	// 设置客户端连接的内容类型
	if len(cfg.ClientConnection.ContentType) == 0 {
		cfg.ClientConnection.ContentType = DefaultClientConnectionContentType
	}

	// 设置调度器名称
	if cfg.SchedulerName == nil {
		defaultValue := DefaultSchedulerName
		cfg.SchedulerName = &defaultValue
	}

	// 设置追踪器
	if cfg.Tracer == nil {
		cfg.Tracer = tracing.DefaultNoopOptions()
	}

	// 调度器对 QPS(每秒查询率)/Burst(突发量) 有自己的偏好，设置特定的默认值，而不是通用设置
	if cfg.ClientConnection.QPS == 0.0 {
		cfg.ClientConnection.QPS = DefaultClientConnectionQPS
	}
	if cfg.ClientConnection.Burst == 0 {
		cfg.ClientConnection.Burst = DefaultClientConnectionBurst
	}

	// 构建默认绑定地址 (0.0.0.0:默认端口)
	defaultBindAddress := net.JoinHostPort("0.0.0.0", strconv.Itoa(DefaultInsecureBinderPort))

	// 设置健康检查服务的绑定地址
	if len(cfg.HealthzBindAddress) == 0 {
		// 如果未设置健康检查地址，则使用默认地址
		cfg.HealthzBindAddress = defaultBindAddress
	} else {
		// 如果已设置健康检查地址，则进行解析和标准化处理
		if host, port, err := net.SplitHostPort(cfg.HealthzBindAddress); err == nil {
			// 如果解析成功，确保主机部分不为空（为空则设为 0.0.0.0）
			if len(host) == 0 {
				host = "0.0.0.0"
			}
			hostPort := net.JoinHostPort(host, port)
			cfg.HealthzBindAddress = hostPort
		} else {
			// 如果解析失败，检查是否为有效的 IP 地址
			if host := net.ParseIP(cfg.HealthzBindAddress); host != nil {
				// 如果是 IP 地址，使用该 IP 与默认端口组合
				hostPort := net.JoinHostPort(cfg.HealthzBindAddress, strconv.Itoa(DefaultInsecureBinderPort))
				cfg.HealthzBindAddress = hostPort
			} else {
				// 如果都不是，回退到默认地址
				// TODO: 在 godelschedulerconfig 中我们应该让这个错误抛出，而不是用默认值覆盖
				cfg.HealthzBindAddress = defaultBindAddress
			}
		}
	}

	// 设置指标服务的绑定地址
	// 逻辑与健康检查地址类似，确保指标服务地址的格式正确
	if len(cfg.MetricsBindAddress) == 0 {
		cfg.MetricsBindAddress = defaultBindAddress
	} else {
		if host, port, err := net.SplitHostPort(cfg.MetricsBindAddress); err == nil {
			if len(host) == 0 {
				host = "0.0.0.0"
			}
			hostPort := net.JoinHostPort(host, port)
			cfg.MetricsBindAddress = hostPort
		} else {
			// 解析失败时，同样尝试解析为 IP 地址
			if host := net.ParseIP(cfg.MetricsBindAddress); host != nil {
				hostPort := net.JoinHostPort(cfg.MetricsBindAddress, strconv.Itoa(DefaultInsecureBinderPort))
				cfg.MetricsBindAddress = hostPort
			} else {
				// 否则使用默认地址
				// TODO: 在 godelschedulerconfig 中我们应该让这个错误抛出，而不是用默认值覆盖
				cfg.MetricsBindAddress = defaultBindAddress
			}
		}
	}

	// 使用默认的 LeaderElectionConfiguration 选项
	defaultsconfig.SetDefaultLeaderElectionConfiguration(&cfg.LeaderElection)
	// 设置 Leader 选举的资源名称
	if len(cfg.LeaderElection.ResourceName) == 0 {
		cfg.LeaderElection.ResourceName = DispatcherDefaultLockObjectName
	}

	// 默认启用调度器的性能分析功能
	if cfg.EnableProfiling == nil {
		enableProfiling := true
		cfg.EnableProfiling = &enableProfiling
	}

	// 如果启用了性能分析，则默认启用竞争分析（用于检测锁竞争等性能瓶颈）
	if *cfg.EnableProfiling && cfg.EnableContentionProfiling == nil {
		enableContentionProfiling := true
		cfg.EnableContentionProfiling = &enableContentionProfiling
	}
}
