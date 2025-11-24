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

package app

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	goruntime "runtime"

	"github.com/spf13/cobra"

	"k8s.io/kubernetes/godel-pkg/dispatcher"
	godeldispatcherconfig "k8s.io/kubernetes/godel-pkg/dispatcher/config"
	cmdutil "k8s.io/kubernetes/godel-pkg/util/cmd"
	routeutil "k8s.io/kubernetes/godel-pkg/util/route"
	"k8s.io/kubernetes/godel-pkg/version/verflag"
	"k8s.io/kubernetes/godel-cmd/dispatcher/app/config"
	"k8s.io/kubernetes/godel-cmd/dispatcher/app/options"
	"k8s.io/kubernetes/godel-cmd/scheduler/app/util/configz"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapifilters "k8s.io/apiserver/pkg/endpoints/filters"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericfilters "k8s.io/apiserver/pkg/server/filters"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/mux"
	"k8s.io/apiserver/pkg/server/routes"
	"k8s.io/client-go/tools/events"
	//cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/cli/globalflag"
	"k8s.io/component-base/metrics/legacyregistry"
	//"k8s.io/component-base/term"
	"k8s.io/klog/v2"
)

const ComponentName = "dispatcher"

// NewDispatcherCommand 创建并返回一个 Cobra Command 对象，该对象代表了 Godel Dispatcher 的主命令行应用。
// 这个命令负责解析启动参数、初始化配置，并最终启动 Dispatcher 的主逻辑循环。
func NewDispatcherCommand() *cobra.Command {
	// 1. 初始化命令行选项 (Options)
	// 调用 options.NewOptions() 创建一个 Options 结构体实例，该结构体包含了 Dispatcher 所有可配置的参数（如配置文件路径、日志级别、调度器ID等）。
	opts, err := options.NewOptions() //这里需要修改
	if err != nil {
		// 如果初始化选项失败（例如，某些默认值设置错误），则打印错误信息到标准错误输出并退出程序。
		fmt.Fprintf(os.Stderr, "unable to initialize command options: %v\n", err)
		os.Exit(1)
	}

	// 2. 创建 Cobra Command
	cmd := &cobra.Command{
		// Use 定义了命令的使用方式，通常是命令的名称。ComponentName 应该是一个常量，例如 "godel-dispatcher"。
		Use: ComponentName,
		// Run 定义了当命令被成功解析且没有子命令时执行的函数。
		// 这里将执行 runCommand 函数，并传入 Cobra Command 对象、解析后的选项 opts 和命令行参数 args。
		Run: func(cmd *cobra.Command, args []string) {
			if err := runCommand(cmd, opts, args); err != nil {
				// 如果 runCommand 执行过程中发生错误，则打印错误信息到标准错误输出并退出程序。
				fmt.Fprintf(os.Stderr, "%v\n", err)
				os.Exit(1)
			}
		},
	}

	// 3. 绑定命令行标志 (Flags)
	fs := cmd.Flags() // 获取 Command 的 FlagSet
	// 从 opts 获取所有已定义的命名 FlagSets (例如 "godel", "logging", "global" 等)。
	namedFlagSets := opts.Flags()
	// 将 Kubernetes 全局标志 (如 --kubeconfig, --v 等) 添加到 "global" FlagSet 中，并将其添加到主命令。
	globalflag.AddGlobalFlags(namedFlagSets.FlagSet("global"), cmd.Name())
	// 将版本标志 (如 --version) 添加到 "global" FlagSet。
	verflag.AddFlags(namedFlagSets.FlagSet("global"))
	// 将所有命名的 FlagSets 都添加到主命令的 Flags 中，这样用户就可以通过命令行传入这些参数。
	for _, f := range namedFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}
	// 6. 返回构建好的 Command 对象
	return cmd
}

// runCommand 是 Cobra Command 的执行函数，负责初始化日志、验证选项、
// 处理配置文件写入请求，并最终启动 Dispatcher 的主运行循环。
// 这是 Dispatcher 命令行应用的核心执行逻辑。
func runCommand(cmd *cobra.Command, opts *options.Options, args []string) error {
	// 1. 初始化日志系统
	// 根据 Cobra Command 的 FlagSet (包含命令行参数) 初始化 klog。
	// 这会处理像 --v (日志级别) 这样的标志。
	cmdutil.InitKlogV2WithV1Flags(cmd.Flags())

	// 3. 检查位置参数
	// Dispatcher 命令不接受任何位置参数 (args)，如果提供了则报错。
	if len(args) != 0 {
		fmt.Fprint(os.Stderr, "arguments are not supported\n")
		// 注意：这里只打印错误信息，但没有返回错误或退出，这可能导致后续逻辑执行失败。
		// 通常这里应该返回一个错误。
	}

	// 4. 验证选项
	// 调用 opts.Validate() 验证所有从命令行或配置文件解析出的选项是否有效。
	// 如果验证失败，errs 将包含一个或多个错误。
	if errs := opts.Validate(); len(errs) > 0 {
		// 将多个错误聚合为一个错误返回。
		return utilerrors.NewAggregate(errs)
	}

	// 6. 构建完整配置
	// 调用 opts.Config() 方法，根据验证后的选项 opts 创建一个 *server.Config 对象。
	// 这个对象包含了启动 Dispatcher 服务器所需的所有配置信息。
	c, err := opts.Config()
	if err != nil {
		return err // 如果配置构建失败，则返回错误。
	}
	// 调用 Complete() 方法，对配置进行最后的处理和填充，得到一个完全可用的配置对象 cc。
	cc := c.Complete()
	klog.InfoS("一个完全可用的配置对象 cc", "cc", cc)

	// 7. 创建上下文并启动
	// 创建一个可取消的 context.Context，用于控制 Dispatcher 的生命周期。
	ctx, cancel := context.WithCancel(context.Background())
	// 使用 defer 确保在 runCommand 函数退出时调用 cancel()，
	// 从而通知 Dispatcher 停止运行。
	defer cancel()

	// 调用 Run 函数，传入上下文 ctx 和完整的配置 cc。
	// Run 函数是 Dispatcher 实际启动和运行的地方。
	return Run(ctx, cc)
}

// Run 函数是 Godel Dispatcher 组件的主运行循环。
// 它负责初始化各种客户端、事件记录器、健康检查服务器、指标服务器，
// 启动 Informer 缓存同步，并根据配置决定是否启用领导者选举来运行 Dispatcher。
// 该函数通常在一个 goroutine 中调用，并会阻塞直到上下文被取消或发生错误。
func Run(ctx context.Context, cc config.CompletedConfig) error {
	// 2. 创建 Dispatcher 实例
	// 使用完成的配置 (cc) 中的各种客户端和 Informer 来创建 Dispatcher 核心对象。
	dispatcher := dispatcher.New(
		ctx.Done(),                             // 传递上下文的取消信号通道，用于通知 Dispatcher 停止
		cc.Client,                              // Kubernetes 核心 API 客户端
		cc.GodelCrdClient,                      // Godel 自定义资源定义 (CRD) 客户端
		cc.InformerFactory.Core().V1().Pods(),  // Pod Informer
		cc.InformerFactory.Core().V1().Nodes(), // Node Informer
		cc.GodelCrdInformerFactory.Scheduling().V1alpha1().Schedulers(), // Godel Scheduler CRD Informer
		*cc.DispatcherConfig.SchedulerName,                              // Dispatcher 管理的调度器名称
		getEventRecorder(&cc),                                           // 事件记录器，用于向 API Server 发送事件
	)

	// 3. 启动事件广播器
	// EventBroadcaster 用于将事件发送到 Kubernetes API Server。
	// StartRecordingToSink 启动一个 goroutine 来监听事件并将其发送出去。
	cc.EventBroadcaster.StartRecordingToSink(ctx.Done())

	// 6. 启动 Informer 并等待缓存同步
	// 启动所有核心 API Informer (Pod, Node, PriorityClass 等)
	cc.InformerFactory.Start(ctx.Done())
	// 等待核心 API Informer 的缓存同步完成，确保本地缓存与 API Server 一致。
	cc.InformerFactory.WaitForCacheSync(ctx.Done())

	// 启动所有 Godel CRD Informer (Scheduler, NMNode, PodGroup 等)
	cc.GodelCrdInformerFactory.Start(ctx.Done())
	// 等待 Godel CRD Informer 的缓存同步完成。
	cc.GodelCrdInformerFactory.WaitForCacheSync(ctx.Done())

	// 7. 定义运行逻辑
	// 定义一个匿名函数 run，封装了 Dispatcher 的实际运行逻辑。
	// 这样做是为了在领导者选举和非领导者选举模式下复用相同的启动代码。
	run := func(ctx context.Context) {

		// 启动 Dispatcher 的核心逻辑循环。
		// 这会启动 Pod 监听、调度器发现、分发循环等。
		dispatcher.Run(ctx)
		// 阻塞，直到 ctx 被取消（即收到停止信号）。
		<-ctx.Done()
	}

	// 9. 非领导者选举模式
	// 如果没有启用领导者选举，则直接运行 Dispatcher。
	run(ctx)

	// 如果 run(ctx) 返回（例如 ctx 被取消），则返回一个错误信息。
	// 在非领导者选举模式下，正常退出通常意味着程序收到了停止信号。
	return fmt.Errorf("finished without leader elect")
}

// buildHandlerChain wraps the given handler with the standard filters.
func buildHandlerChain(handler http.Handler, authn authenticator.Request, authz authorizer.Authorizer) http.Handler {
	requestInfoResolver := &apirequest.RequestInfoFactory{}

	handler = genericapifilters.WithRequestInfo(handler, requestInfoResolver)
	handler = genericapifilters.WithCacheControl(handler)
	handler = genericfilters.WithPanicRecovery(handler, requestInfoResolver)

	return handler
}

func installMetricHandler(pathRecorderMux *mux.PathRecorderMux) {
	configz.InstallHandler(pathRecorderMux)

	//lint:ignore SA1019 See the Metrics Stability Migration KEP
	defaultMetricsHandler := legacyregistry.Handler().ServeHTTP
	pathRecorderMux.HandleFunc("/metrics", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == "DELETE" {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.Header().Set("X-Content-Type-Options", "nosniff")
			io.WriteString(w, "metrics reset\n")
			return
		}
		defaultMetricsHandler(w, req)
	})
}

// newMetricsHandler builds a metrics server from the config.
func newMetricsHandler(config *godeldispatcherconfig.GodelDispatcherConfiguration) http.Handler {
	pathRecorderMux := mux.NewPathRecorderMux(ComponentName)
	installMetricHandler(pathRecorderMux)
	if *config.EnableProfiling {
		routes.Profiling{}.Install(pathRecorderMux)
		if *config.EnableContentionProfiling {
			goruntime.SetBlockProfileRate(1)
		}
		routeutil.DebugFlags{}.Install(pathRecorderMux, "v", routeutil.StringFlagHandler(routeutil.GlogSetter, routeutil.GlogGetter))
	}
	return pathRecorderMux
}

// newHealthzHandler creates a healthz server from the config, and will also
// embed the metrics handler if the healthz and metrics address configurations
// are the same.
func newHealthzHandler(config *godeldispatcherconfig.GodelDispatcherConfiguration, separateMetrics bool, checks ...healthz.HealthChecker) http.Handler {
	pathRecorderMux := mux.NewPathRecorderMux(ComponentName)
	healthz.InstallHandler(pathRecorderMux, checks...)
	if !separateMetrics {
		installMetricHandler(pathRecorderMux)
	}
	if *config.EnableProfiling {
		routes.Profiling{}.Install(pathRecorderMux)
		if *config.EnableContentionProfiling {
			goruntime.SetBlockProfileRate(1)
		}
		routeutil.DebugFlags{}.Install(pathRecorderMux, "v", routeutil.StringFlagHandler(routeutil.GlogSetter, routeutil.GlogGetter))
	}
	return pathRecorderMux
}

func getEventRecorder(cc *config.CompletedConfig) events.EventRecorder {
	return cc.EventBroadcaster.NewRecorder(ComponentName)
}
