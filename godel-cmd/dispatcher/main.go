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

// Package main 定义了 Godel 调度器的 Dispatcher 组件的主入口点。
// Dispatcher 是 Godel 调度器架构中的一个核心组件，负责接收待调度的 Pod，
// 并根据节点分区和负载均衡策略将它们分发给合适的 Scheduler 实例进行处理。
package main

import (
	"math/rand" // 导入 rand 包，用于生成随机数
	"os"        // 导入 os 包，用于操作系统相关的功能（如程序退出）
	"time"      // 导入 time 包，用于时间相关的功能（如获取当前时间）

	"github.com/spf13/pflag"                              // 导入 spf13/pflag 包，用于解析命令行参数
	cliflag "k8s.io/component-base/cli/flag"              // 导入 k8s.io 的 CLI flag 工具包
	"k8s.io/component-base/logs"                          // 导入 k8s.io 的日志初始化和管理包
	_ "k8s.io/component-base/metrics/prometheus/clientgo" // 导入 prometheus 客户端，用于指标收集 (副作用导入)
	"k8s.io/klog/v2"                                      // 导入 klog/v2 用于日志记录

	"k8s.io/kubernetes/godel-cmd/dispatcher/app" // 导入 app 包，其中包含构建 Dispatcher 命令行应用的逻辑
)

// main 函数是程序的入口点。
// 它负责初始化随机数种子、设置命令行参数解析、初始化日志系统，
// 创建并执行 Dispatcher 的 Cobra 命令。
func main() {
	// 使用当前时间的纳秒级时间戳作为随机数种子，以确保每次运行程序时生成的随机数序列不同。
	// rand.Seed 是旧版函数，Go 1.20+ 推荐使用 rand.NewSource，但此处可能为兼容性保留。
	rand.Seed(time.Now().UnixNano())

	// 添加一条信息级别的日志，表示 Dispatcher 程序已开始启动。
	// 这是程序启动后打印的第一条日志，用于确认程序已开始执行。
	klog.Info("删去所有钩子函数")

	// 调用 app 包中的 NewDispatcherCommand 函数，创建一个 Cobra Command 对象。
	// 这个 Command 对象定义了 Dispatcher 的命令行接口、子命令、参数和执行逻辑。
	command := app.NewDispatcherCommand()

	// 设置 pflag 命令行参数解析器的标准化函数。
	// cliflag.WordSepNormalizeFunc 用于将命令行参数中的下划线 (_) 规范化为连字符 (-)。
	// 例如，--my_flag 会被解析为 --my-flag。
	pflag.CommandLine.SetNormalizeFunc(cliflag.WordSepNormalizeFunc)

	// 初始化日志系统。
	// 这会根据命令行参数或默认配置设置日志的输出格式、级别等。
	logs.InitLogs()
	// 使用 defer 确保在 main 函数退出前，将所有缓存中的日志内容刷新到输出目标（如文件或标准输出）。
	defer logs.FlushLogs()

	// 执行 Cobra Command 对象。
	// 这会解析命令行参数，并调用与参数匹配的子命令或默认的执行逻辑。
	// 如果执行过程中发生错误，err 将不为 nil。
	if err := command.Execute(); err != nil {
		// 如果 command.Execute() 返回错误，则调用 os.Exit(1) 退出程序，并返回退出码 1。
		// 退出码 1 通常表示程序非正常退出。
		os.Exit(1)
	}
}
