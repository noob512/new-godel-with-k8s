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

// Package scheme 定义了 Godel 调度器配置 API 的 runtime.Scheme 实例，
// 并注册了所有支持的配置 API 版本（如 v1beta1）。
// 该 Scheme 用于配置对象的序列化、反序列化、默认值设置和版本转换。
package scheme

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
	godelschedulerconfig "github.com/kubewharf/godel-scheduler/pkg/scheduler/apis/config"
	godelschedulerconfigv1beta1 "github.com/kubewharf/godel-scheduler/pkg/scheduler/apis/config/v1beta1"
)

var (
	// Scheme 是一个全局的 runtime.Scheme 实例，
	// 用于注册 Godel 调度器所有配置相关的 API 类型（例如 GodelSchedulerConfiguration）。
	// 它支持跨版本的序列化/反序列化和类型转换。
	Scheme = runtime.NewScheme()

	// Codecs 基于 Scheme 提供编码（encode）和解码（decode）功能，
	// 启用了严格模式（strict mode），在反序列化时会拒绝未知字段，
	// 有助于提高配置解析的健壮性和安全性。
	Codecs = serializer.NewCodecFactory(Scheme, serializer.EnableStrict)
)

// init 函数在包初始化时自动调用，
// 将 Godel 调度器支持的配置 API 类型注册到全局 Scheme 中。
func init() {
	klog.Info("使用这个scheme——init函数")
	AddToScheme(Scheme)
}

// AddToScheme 将 Godel 调度器所有已知版本的配置 API 类型注册到给定的 scheme 中。
// 目前包括 internal（内部）版本和 v1beta1 版本。
// 同时设置 v1beta1 为该 API 组的优先版本，用于默认序列化和版本协商。
func AddToScheme(scheme *runtime.Scheme) {
	klog.Info("使用函数-1")
	// 注册 internal 版本的配置类型（通常用于内部逻辑处理）
	utilruntime.Must(godelschedulerconfig.AddToScheme(scheme))
	// 注册 v1beta1 版本的配置类型（用于外部 YAML/JSON 配置）
	utilruntime.Must(godelschedulerconfigv1beta1.AddToScheme(scheme))
	// 设置 v1beta1 为该组的默认优先版本
	utilruntime.Must(scheme.SetVersionPriority(
		godelschedulerconfigv1beta1.SchemeGroupVersion,
	))
}