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
	godelclient "github.com/kubewharf/godel-scheduler-api/pkg/client/clientset/versioned"
	crdinformers "github.com/kubewharf/godel-scheduler-api/pkg/client/informers/externalversions"
	apiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"

	dispatcherconfig "k8s.io/kubernetes/godel-pkg/dispatcher/config"
	cmdutil "k8s.io/kubernetes/godel-pkg/util/cmd"
)

// Config 定义了 Dispatcher 应用程序的配置结构
// 该结构包含了运行 Dispatcher 所需的各种客户端、Informer、事件广播器、领导者选举配置等
type Config struct {
	// Client 是 Kubernetes 核心 API 的客户端接口，用于与标准的 Kubernetes 资源（如 Pod、Node、Service 等）进行交互
	Client clientset.Interface
	
	// InformerFactory 是 Kubernetes 核心资源的共享 Informer 工厂
	// 用于创建和管理标准 Kubernetes 资源的 Informer，提供缓存和事件监听功能
	InformerFactory informers.SharedInformerFactory

	// godel crd client & informer
	// GodelCrdClient 是 Godel 自定义资源定义 (CRD) 的客户端接口
	// 用于与 Godel 调度系统特有的自定义资源（如 Scheduler、PodGroup 等）进行交互
	GodelCrdClient godelclient.Interface
	
	// GodelCrdInformerFactory 是 Godel CRD 资源的共享 Informer 工厂
	// 用于创建和管理 Godel 自定义资源的 Informer，提供缓存和事件监听功能
	GodelCrdInformerFactory crdinformers.SharedInformerFactory

	// DispatcherConfig 包含了 Godel Dispatcher 的具体配置参数
	DispatcherConfig dispatcherconfig.GodelDispatcherConfiguration

	// EventBroadcaster 是事件广播器的适配器，兼容 core.v1.Event 和 events.v1beta1.Event
	// 用于向 Kubernetes API 服务器发送事件，记录调度过程中的重要状态变化
	// 根据注释，此字段将在事件从 core API 迁移到 events API 后被移除
	// 更多信息请参见: https://github.com/kubernetes/enhancements/blob/master/keps/sig-instrumentation/383-new-event-api-ga-graduation/README.md
	EventBroadcaster cmdutil.EventBroadcasterAdapter

	// LeaderElection 存储领导者选举的配置
	// 如果为 nil，则表示不启用领导者选举功能
	LeaderElection *leaderelection.LeaderElectionConfig

	// InsecureServing 用于配置不安全端口（HTTP）的服务信息
	// 如果为 nil，则禁用不安全端口的服务
	InsecureServing *apiserver.DeprecatedInsecureServingInfo
	
	// InsecureMetricsServing 用于配置独立的指标服务在不安全端口（HTTP）上的服务信息
	// 如果为 nil，则不启用独立的不安全指标服务
	InsecureMetricsServing *apiserver.DeprecatedInsecureServingInfo

	// LoopbackClientConfig 是一个特权循环回路连接的配置
	// 用于从调度器内部向 API 服务器发起请求，通常用于需要较高权限的操作
	LoopbackClientConfig *restclient.Config
}

type completedConfig struct {
	*Config
}

type CompletedConfig struct {
	*completedConfig
}

func (c *Config) Complete() CompletedConfig {
	cc := completedConfig{c}
	return CompletedConfig{&cc}
}
