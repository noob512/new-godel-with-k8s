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
	"time"

	"github.com/spf13/cobra"

	schedulerserverconfig "github.com/kubewharf/godel-scheduler/cmd/scheduler/app/config"
	"github.com/kubewharf/godel-scheduler/cmd/scheduler/app/options"
	"github.com/kubewharf/godel-scheduler/cmd/scheduler/app/util/configz"
	godelscheduler "github.com/kubewharf/godel-scheduler/pkg/scheduler"
	godelschedulerconfig "github.com/kubewharf/godel-scheduler/pkg/scheduler/apis/config"
	cmdutil "github.com/kubewharf/godel-scheduler/pkg/util/cmd"
	routeutil "github.com/kubewharf/godel-scheduler/pkg/util/route"
	"github.com/kubewharf/godel-scheduler/pkg/util/tracing"
	"github.com/kubewharf/godel-scheduler/pkg/version/verflag"

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

const ComponentName = "scheduler"

// NewGodelSchedulerCmd 创建一个新的 cobra 命令用于 godel 调度器组件。
// 它使用其用法、描述、标志和执行逻辑初始化命令。
func NewGodelSchedulerCmd() *cobra.Command {
	// 初始化命令选项。如果无法创建选项，则退出。
	opts, err := options.NewOptions()
	klog.Info("开始scheduler的选项创建")
	if err != nil {
		fmt.Fprintf(os.Stderr, "无法初始化命令选项: %v\n", err)
		os.Exit(1)
	}

	// 为调度器创建主 cobra 命令。
	godelSchedulerCmd := &cobra.Command{
		// 命令名称，通常在别处定义为常量。
		Use: ComponentName,
		// godel 调度器目的的详细描述：
		// 字节跳动使用 YARN 处理离线工作负载，使用 Kubernetes 处理在线工作负载。
		// godel 调度器旨在通过在未充分利用的在线和流式工作负载资源上混部批处理工作负载来提高硬件利用率。
		Long: `Bytedance's current infrastructure runs two primary resource 
management scheduling system, YARN for offline (batch and streaming) workloads 
and Kubernetes for online, long running workloads. Both systems individually 
provide a comprehensive list of features and production scale reliability. 
In recent years, Bytedance's business has grown significantly and during the 
period of pandemic growth has been exponential. In this hyper growth phase, 
infrastructure server fleet has increased in parallel but overall utilization 
of that hardware is not par during off-peak load. Primary goal for godel 
scheduler is to harvest the underutilized resources from the online and 
streaming workloads by collocating the batch workloads.`,
		// 定义运行命令时执行的函数。
		// 它使用命令、选项和参数调用 runCommand。
		// 如果 runCommand 返回错误，则打印错误并退出。
		Run: func(cmd *cobra.Command, args []string) {
			if err := runCommand(cmd, opts, args); err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				os.Exit(1)
			}
		},
	}

	// 获取命令的标志集。
	fs := godelSchedulerCmd.Flags()
	// 从选项中获取命名标志集。
	namedFlagSets := opts.Flags()
	// 向全局标志集添加版本标志。
	verflag.AddFlags(namedFlagSets.FlagSet("global"))
	// 向命令添加全局标志。
	globalflag.AddGlobalFlags(namedFlagSets.FlagSet("global"), godelSchedulerCmd.Name())
	// 将选项中的所有标志集添加到命令的标志集中。
	for _, f := range namedFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}

	// // 用法消息的格式字符串。
	// usageFmt := "Usage:\n  %s\n"
	// // 获取终端大小以格式化帮助输出。
	// cols, _, _ := term.TerminalSize(godelSchedulerCmd.OutOrStdout())
	
	// // 设置自定义用法函数，以便在请求用法时格式化输出。
	// godelSchedulerCmd.SetUsageFunc(func(cmd *cobra.Command) error {
	// 	// 打印用法行。
	// 	fmt.Fprintf(cmd.OutOrStderr(), usageFmt, cmd.UseLine())
	// 	// 使用基于终端宽度的格式打印可用标志部分。
	// 	cliflag.PrintSections(cmd.OutOrStderr(), namedFlagSets, cols)
	// 	return nil
	// })
	
	// // 设置自定义帮助函数，以便在请求帮助时格式化输出。
	// godelSchedulerCmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
	// 	// 打印长描述，后跟用法行。
	// 	fmt.Fprintf(cmd.OutOrStdout(), "%s\n\n"+usageFmt, cmd.Long, cmd.UseLine())
	// 	// 使用基于终端宽度的格式打印可用标志部分。
	// 	cliflag.PrintSections(cmd.OutOrStdout(), namedFlagSets, cols)
	// })
	
	// 标记 "config" 标志期望具有特定扩展名的文件名。
	godelSchedulerCmd.MarkFlagFilename("config", "yaml", "yml", "json")

	// 返回完全配置的 cobra 命令。
	return godelSchedulerCmd
}

// runCommand 是调度器组件的主执行函数，负责解析命令行参数、验证配置、写入配置文件（如指定），
// 并最终启动调度器运行。
func runCommand(cmd *cobra.Command, opts *options.Options, args []string) error {
	// 初始化 klog 日志系统，使其兼容 v1 和 v2 的日志标志。
	cmdutil.InitKlogV2WithV1Flags(cmd.Flags())

	// 如果用户请求显示版本信息（如 --version），则打印版本并退出。
	verflag.PrintAndExitIfRequested()

	// 调度器命令不接受位置参数，若传入则报错提示。
	if len(args) != 0 {
		fmt.Fprint(os.Stderr, "arguments are not supported\n")
	}

	// 验证命令行选项的合法性，若存在错误则返回聚合错误。
	if errs := opts.Validate(); len(errs) > 0 {
		return utilerrors.NewAggregate(errs)
	}

	// 如果指定了 WriteConfigTo（如 --write-config-to），则生成并写入配置文件，然后退出。
	// if len(opts.WriteConfigTo) > 0 {
	// 	// 创建一个空的调度器配置结构体。
	// 	c := &schedulerserverconfig.Config{}
	// 	// 将命令行选项应用到配置结构体中。
	// 	if err := opts.ApplyTo(c); err != nil {
	// 		return err
	// 	}
	// 	// 将 ComponentConfig 部分写入指定的文件。
	// 	if err := options.WriteConfigFile(opts.WriteConfigTo, &c.ComponentConfig); err != nil {
	// 		return err
	// 	}
	// 	// 记录写入成功的日志（日志级别为 V(1)）。
	// 	klog.V(1).InfoS("Wrote configuration", "file", opts.WriteConfigTo)
	// 	return nil
	// }

	// 根据命令行选项构建完整的配置对象。
	c, err := opts.Config()
	if err != nil {
		return err
	}

	// 补全配置（例如填充默认值），得到最终可运行的配置。
	cc := c.Complete()

	// 将配置注册到 configz（用于通过 /configz 端点暴露运行时配置）。
	if cz, err := configz.New("componentconfig"); err == nil {
		cz.Set(cc.ComponentConfig)
	} else {
		return fmt.Errorf("unable to register configz: %s", err)
	}

	// 创建一个可取消的上下文，用于控制调度器的生命周期。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 函数退出时取消上下文，确保资源释放。

	// 启动调度器主循环。
	return Run(ctx, cc)
}

// Run 是调度器的主运行函数，负责初始化并启动 Godel 调度器实例。
// 它会根据配置启动健康检查、指标服务、Informer 缓存同步，并处理 Leader 选举逻辑（如启用）。
func Run(ctx context.Context, cc schedulerserverconfig.CompletedConfig) error {
	// 验证 Tracer（如 OpenTelemetry）配置是否合法。
	err := cc.ComponentConfig.Tracer.Validate()
	if err != nil {
		return err
	}

	// 获取用于记录调度事件的事件记录器（EventRecorder）。
	eventRecorder := getEventRecorder(&cc)

	// 创建 Godel 调度器实例，传入调度器名称、客户端、Informer 工厂、上下文取消信号、事件记录器等关键依赖。
	sched, err := godelscheduler.New(
		cc.ComponentConfig.GodelSchedulerName,               // Godel 调度器实例名称（用于区分多调度器）
		cc.ComponentConfig.SchedulerName,                    // Kubernetes 中调度器的标识名（对应 Pod.spec.schedulerName）
		cc.Client,                                           // 核心 Kubernetes 资源客户端
		cc.GodelCrdClient,                                   // Godel 自定义资源客户端
		cc.InformerFactory,                                  // 核心资源 Informer 工厂
		cc.GodelCrdInformerFactory,                          // Godel CRD Informer 工厂
		cc.KatalystCrdInformerFactory,                       // Katalyst CRD Informer 工厂
		ctx.Done(),                                          // 用于感知上下文取消的 channel
		eventRecorder,                                       // 事件记录器
		time.Duration(cc.ComponentConfig.ReservationTimeOutSeconds)*time.Second, // 预占资源超时时间
		godelscheduler.WithDefaultProfile(cc.ComponentConfig.DefaultProfile),    // 默认调度配置文件
		godelscheduler.WithSubClusterProfiles(cc.ComponentConfig.SubClusterProfiles), // 子集群调度配置
		godelscheduler.WithRenewInterval(cc.ComponentConfig.SchedulerRenewIntervalSeconds), // 调度器租约续期间隔
		godelscheduler.WithSubClusterKey(*cc.ComponentConfig.SubClusterKey),     // 用于识别子集群的标签键
	)
	if err != nil {
		return err
	}

	// 启动事件广播器，将事件推送到 API Server。
	cc.EventBroadcaster.StartRecordingToSink(ctx.Done())

	// // 准备健康检查项（healthz checks）。
	// var checks []healthz.HealthChecker
	// if *cc.ComponentConfig.LeaderElection.LeaderElect {
	// 	// 若启用 Leader 选举，添加 WatchDog 检查（确保 Leader 保活）。
	// 	checks = append(checks, cc.LeaderElection.WatchDog)
	// }

	// // 启动非安全（HTTP）的健康检查服务（如 /healthz）。
	// if cc.InsecureServing != nil {
	// 	separateMetrics := cc.InsecureMetricsServing != nil
	// 	handler := buildHandlerChain(newHealthzHandler(&cc.ComponentConfig, separateMetrics, checks...), nil, nil)
	// 	if err := cc.InsecureServing.Serve(handler, 0, ctx.Done()); err != nil {
	// 		return fmt.Errorf("failed to start healthz server: %v", err)
	// 	}
	// }

	// // 启动非安全（HTTP）的指标服务（如 /metrics）。
	// if cc.InsecureMetricsServing != nil {
	// 	handler := buildHandlerChain(newMetricsHandler(&cc.ComponentConfig), nil, nil)
	// 	if err := cc.InsecureMetricsServing.Serve(handler, 0, ctx.Done()); err != nil {
	// 		return fmt.Errorf("failed to start metrics server: %v", err)
	// 	}
	// }

	// // 启动安全（HTTPS）的服务（包含健康检查、/configz、/debug 等），并启用认证与授权。
	// if cc.SecureServing != nil {
	// 	handler := buildHandlerChain(
	// 		newHealthzHandler(&cc.ComponentConfig, false, checks...),
	// 		cc.Authentication.Authenticator,   // 请求身份认证器
	// 		cc.Authorization.Authorizer,      // 请求权限授权器
	// 	)
	// 	// TODO: 当前未处理 stoppedCh，未来可能需要监听服务停止事件。
	// 	if _, _, err := cc.SecureServing.Serve(handler, 0, ctx.Done()); err != nil {
	// 		return fmt.Errorf("failed to start secure server: %v", err)
	// 	}
	// }

	// 启动所有 Informer（开始监听 API Server 资源变化）。
	cc.InformerFactory.Start(ctx.Done())
	cc.GodelCrdInformerFactory.Start(ctx.Done())
	//cc.KatalystCrdInformerFactory.Start(ctx.Done())

	// 等待所有 Informer 的本地缓存同步完成，确保调度器启动时拥有最新资源视图。
	cc.InformerFactory.WaitForCacheSync(ctx.Done())
	cc.GodelCrdInformerFactory.WaitForCacheSync(ctx.Done())
	//cc.KatalystCrdInformerFactory.WaitForCacheSync(ctx.Done())

	// 定义实际运行调度器的逻辑（包含 tracing 初始化和调度主循环）。
	run := func(ctx context.Context) {
		// 初始化分布式追踪（如 OpenTelemetry），组件名为 ComponentName。
		closer := tracing.NewTracer(ComponentName, cc.ComponentConfig.Tracer)
		defer closer.Close() // 确保退出时关闭追踪器

		// 启动调度器主循环。
		sched.Run(ctx)
	}

	// 如果启用了 Leader 选举，则通过 LeaderElector 管理调度器生命周期。
	// if cc.LeaderElection != nil {
	// 	// 设置 Leader 选举回调：
	// 	cc.LeaderElection.Callbacks = leaderelection.LeaderCallbacks{
	// 		OnStartedLeading: func(ctx context.Context) {
	// 			// 成为 Leader 后启动调度器。
	// 			run(ctx)
	// 		},
	// 		OnStoppedLeading: func() {
	// 			// 失去 Leader 身份时：
	// 			select {
	// 			case <-ctx.Done():
	// 				// 若是因上下文取消（如收到 SIGTERM），正常退出。
	// 				klog.InfoS("Requested to terminate. Exiting")
	// 				klog.FlushAndExit(klog.ExitFlushTimeout, 0)
	// 			default:
	// 				// 若是因租约丢失（如网络分区），异常退出。
	// 				klog.ErrorS(nil, "Lost leader election")
	// 				klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	// 			}
	// 		},
	// 	}

	// 	// 创建并运行 LeaderElector。
	// 	leaderElector, err := leaderelection.NewLeaderElector(*cc.LeaderElection)
	// 	if err != nil {
	// 		return fmt.Errorf("couldn't create leader elector: %v", err)
	// 	}
	// 	leaderElector.Run(ctx)

	// 	// 正常情况下不会执行到这里（除非 Leader 选举退出），返回错误表示失去租约。
	// 	return fmt.Errorf("lost lease")
	// }

	// 若未启用 Leader 选举，则直接运行调度器（适用于单实例场景）。
	run(ctx)

	// 此行理论上不会到达（因为 sched.Run 是阻塞的），保留用于静态检查。
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
func newMetricsHandler(config *godelschedulerconfig.GodelSchedulerConfiguration) http.Handler {
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
func newHealthzHandler(config *godelschedulerconfig.GodelSchedulerConfiguration, separateMetrics bool, checks ...healthz.HealthChecker) http.Handler {
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

func getEventRecorder(cc *schedulerserverconfig.CompletedConfig) events.EventRecorder {
	return cc.EventBroadcaster.NewRecorder(cc.ComponentConfig.GodelSchedulerName)
}
