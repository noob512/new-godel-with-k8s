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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kubewharf/godel-scheduler/pkg/scheduler/apis/config"
)

// GroupName is the group name used in this package
const GroupName = "godelscheduler.config.kubewharf.io"

// SchemeGroupVersion is group version used to register these objects
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

var (
	// localSchemeBuilder 是一个 Scheme 构建器，用于收集本 API 包中需要注册到 runtime.Scheme 的初始化函数。
	// 它包含了类型注册（addKnownTypes）和默认值设置函数注册（addDefaultingFuncs）等操作，
	// 这些函数将在 Scheme 构建时按顺序执行。
	localSchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes, addDefaultingFuncs)

	// AddToScheme 是一个全局函数，供外部（如主程序或测试）调用，
	// 用于将本 API 组（及其所有支持的版本）的类型和相关逻辑（如默认值、转换函数）注册到指定的 runtime.Scheme 中。
	// 通常在程序初始化阶段（例如 init() 函数中）被调用，以确保 API 类型在序列化、反序列化和默认值处理时可用。
	AddToScheme = localSchemeBuilder.AddToScheme
)

// addKnownTypes registers known types to the given scheme
// addKnownTypes 将 Godel 调度器 v1beta1 版本中定义的所有配置类型注册到给定的 runtime.Scheme 中。
// 这些类型主要用于调度器配置文件（如 YAML）的反序列化，以及内部对象与外部版本之间的转换。
// 注册的类型包括主配置结构 GodelSchedulerConfiguration 及其引用的各类插件参数（如节点资源匹配、拓扑分布、亲和性等）。
// 该函数通常由 Scheme 构建器在初始化阶段调用，不应被直接使用。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&GodelSchedulerConfiguration{},                    // 主调度器配置结构
		&config.InterPodAffinityArgs{},                   // Pod 间亲和性/反亲和性插件参数
		&config.NodeLabelArgs{},                          // 节点标签插件参数
		&config.NodeResourcesFitArgs{},                   // 节点资源适配插件参数
		&config.NodeResourcesAffinityArgs{},              // 节点资源亲和性插件参数
		&config.PodTopologySpreadArgs{},                  // Pod 拓扑分布插件参数
		&config.RequestedToCapacityRatioArgs{},           // 请求与容量比值插件参数
		&config.ServiceAffinityArgs{},                    // 服务亲和性插件参数
		&config.NodeResourcesLeastAllocatedArgs{},        // 节点资源最少分配插件参数
		&config.NodeResourcesMostAllocatedArgs{},         // 节点资源最多分配插件参数
		&config.StartRecentlyArgs{},                      // 最近启动时间插件参数
		&config.NodeResourcesBalancedAllocatedArgs{},     // 节点资源平衡分配插件参数
		&config.LocalStoragePoolCheckerArgs{},            // 本地存储池检查插件参数
		&config.LoadAwareArgs{},                          // 负载感知插件参数
	)
	return nil
}
