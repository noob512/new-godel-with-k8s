/*
Copyright 2014 The Kubernetes Authors.

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

// Package app implements a Server object for running the scheduler.
package app

import (
	"context"
	"fmt"
	"net/http"
	"os"
	goruntime "runtime"

	"github.com/spf13/cobra"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapifilters "k8s.io/apiserver/pkg/endpoints/filters"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server"
	genericfilters "k8s.io/apiserver/pkg/server/filters"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/mux"
	"k8s.io/apiserver/pkg/server/routes"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	//"k8s.io/client-go/tools/leaderelection"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/cli/globalflag"
	"k8s.io/component-base/configz"
	"k8s.io/component-base/logs"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/term"
	"k8s.io/component-base/version"
	"k8s.io/component-base/version/verflag"
	"k8s.io/klog/v2"
	schedulerserverconfig "k8s.io/kubernetes/cmd/kube-scheduler/app/config"
	"k8s.io/kubernetes/cmd/kube-scheduler/app/options"
	"k8s.io/kubernetes/pkg/scheduler"
	kubeschedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/latest"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics/resources"
	"k8s.io/kubernetes/pkg/scheduler/profile"
)

func init() {
	utilruntime.Must(logs.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
}

// Option configures a framework.Registry.
type Option func(runtime.Registry) error

// NewSchedulerCommand creates a *cobra.Command object with default parameters and registryOptions
func NewSchedulerCommand(registryOptions ...Option) *cobra.Command {
	opts := options.NewOptions()

	cmd := &cobra.Command{
		Use: "kube-scheduler",
		Long: `The Kubernetes scheduler is a control plane process which assigns
Pods to Nodes. The scheduler determines which Nodes are valid placements for
each Pod in the scheduling queue according to constraints and available
resources. The scheduler then ranks each valid Node and binds the Pod to a
suitable Node. Multiple different schedulers may be used within a cluster;
kube-scheduler is the reference implementation.
See [scheduling](https://kubernetes.io/docs/concepts/scheduling-eviction/)
for more information about scheduling and the kube-scheduler component.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCommand(cmd, opts, registryOptions...)
		},
		Args: func(cmd *cobra.Command, args []string) error {
			for _, arg := range args {
				if len(arg) > 0 {
					return fmt.Errorf("%q does not take any arguments, got %q", cmd.CommandPath(), args)
				}
			}
			return nil
		},
	}

	nfs := opts.Flags
	verflag.AddFlags(nfs.FlagSet("global"))
	globalflag.AddGlobalFlags(nfs.FlagSet("global"), cmd.Name(), logs.SkipLoggingConfigurationFlags())
	fs := cmd.Flags()
	for _, f := range nfs.FlagSets {
		fs.AddFlagSet(f)
	}

	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	cliflag.SetUsageAndHelpFunc(cmd, *nfs, cols)

	if err := cmd.MarkFlagFilename("config", "yaml", "yml", "json"); err != nil {
		klog.ErrorS(err, "Failed to mark flag filename")
	}

	return cmd
}

// runCommand runs the scheduler.
// runCommand 执行调度器的主要命令逻辑。
// 它处理标志验证、日志设置、上下文取消以及核心的设置/运行循环。
func runCommand(cmd *cobra.Command, opts *options.Options, registryOptions ...Option) error {
	// 如果请求了版本信息（例如通过 --version 标志），则打印版本并退出。
	klog.InfoS("自定义的k8s+godel调度器")
	verflag.PrintAndExitIfRequested()

	// 尽快激活日志记录，然后使用最终的日志配置显示标志。
	// 这确保后续的日志消息使用最终的日志设置。
	// 如果日志验证失败，则将错误打印到标准错误输出并以状态 1 退出。
	if err := opts.Logs.ValidateAndApply(utilfeature.DefaultFeatureGate); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	// 打印所有命令行标志的最终值到标准输出。
	// 这有助于调试并提供对活动配置的可见性。
	cliflag.PrintFlags(cmd.Flags())

	// 创建一个可以取消的根上下文。
	// 'cancel' 函数被延迟执行，以确保当函数退出时上下文被取消，释放相关资源。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动一个 goroutine 来处理操作系统信号（如 SIGTERM, SIGINT）以实现优雅关闭。
	// 当通过 stopCh 通道接收到信号时，根上下文被取消，
	// 这会将取消传播到监听上下文的其他组件。
	go func() {
		stopCh := server.SetupSignalHandler()
		<-stopCh
		cancel()
	}()

	// 执行调度器的初始设置。
	// 这通常涉及解析配置、初始化组件（如调度器缓存）、
	// 设置用于监视 API 对象（如 Pod、Node）的 informer，并创建调度器实例。
	// 返回的 'cc' 可能是配置/控制器上下文，'sched' 是调度器对象。
	cc, sched, err := Setup(ctx, opts, registryOptions...)
	if err != nil {
		return err
	}

	// 启动调度器的主要执行循环。
	// 此函数运行调度器的核心逻辑（如将 Pod 绑定到节点），直到上下文被取消。
	// 它会阻塞，直到上下文完成（例如由于信号取消）。
	return Run(ctx, cc, sched)
}

// Run executes the scheduler based on the given configuration. It only returns on error or when context is done.
// Run 函数启动调度器的主要执行循环。
// 它负责记录版本信息、启动事件广播器、设置健康检查、启动 Informers、等待缓存同步，
// 并根据是否启用领导者选举来决定如何运行调度器。
func Run(ctx context.Context, cc *schedulerserverconfig.CompletedConfig, sched *scheduler.Scheduler) error {
	// 为了帮助调试，立即记录 Kubernetes 调度器的版本信息。
	klog.InfoS("Starting Kubernetes Scheduler", "version", version.Get())

	// 记录当前的 Golang 运行时设置。
	klog.InfoS("Golang settings", "GOGC", os.Getenv("GOGC"), "GOMAXPROCS", os.Getenv("GOMAXPROCS"), "GOTRACEBACK", os.Getenv("GOTRACEBACK"))


	// 启动事件广播器，开始向 API 服务器发送事件。
	cc.EventBroadcaster.StartRecordingToSink(ctx.Done())


	// 启动所有 Informers，开始监听 Kubernetes API 资源的变化。
	cc.InformerFactory.Start(ctx.Done())
	// DynInformerFactory 在测试中可能是 nil。
	if cc.DynInformerFactory != nil {
		cc.DynInformerFactory.Start(ctx.Done())
	}

	// 等待所有 Informer 的缓存同步完成。
	// 这确保调度器在开始调度之前拥有最新的集群状态。
	cc.InformerFactory.WaitForCacheSync(ctx.Done())
	// DynInformerFactory 在测试中可能是 nil。
	if cc.DynInformerFactory != nil {
		cc.DynInformerFactory.WaitForCacheSync(ctx.Done())
	}
	
	cc.GodelCrdInformerFactory.Start(ctx.Done())
	cc.GodelCrdInformerFactory.WaitForCacheSync(ctx.Done())

	// 启动调度器的主运行循环。
	sched.Run(ctx)
	// 如果调度器的 Run 方法返回（通常不应该），则返回错误。
	return fmt.Errorf("finished without leader elect")
}

// buildHandlerChain wraps the given handler with the standard filters.
func buildHandlerChain(handler http.Handler, authn authenticator.Request, authz authorizer.Authorizer) http.Handler {
	requestInfoResolver := &apirequest.RequestInfoFactory{}
	failedHandler := genericapifilters.Unauthorized(scheme.Codecs)

	handler = genericapifilters.WithAuthorization(handler, authz, scheme.Codecs)
	handler = genericapifilters.WithAuthentication(handler, authn, failedHandler, nil)
	handler = genericapifilters.WithRequestInfo(handler, requestInfoResolver)
	handler = genericapifilters.WithCacheControl(handler)
	handler = genericfilters.WithHTTPLogging(handler)
	handler = genericfilters.WithPanicRecovery(handler, requestInfoResolver)

	return handler
}

func installMetricHandler(pathRecorderMux *mux.PathRecorderMux, informers informers.SharedInformerFactory, isLeader func() bool) {
	configz.InstallHandler(pathRecorderMux)
	pathRecorderMux.Handle("/metrics", legacyregistry.HandlerWithReset())

	resourceMetricsHandler := resources.Handler(informers.Core().V1().Pods().Lister())
	pathRecorderMux.HandleFunc("/metrics/resources", func(w http.ResponseWriter, req *http.Request) {
		if !isLeader() {
			return
		}
		resourceMetricsHandler.ServeHTTP(w, req)
	})
}

// newHealthzAndMetricsHandler creates a healthz server from the config, and will also
// embed the metrics handler.
func newHealthzAndMetricsHandler(config *kubeschedulerconfig.KubeSchedulerConfiguration, informers informers.SharedInformerFactory, isLeader func() bool, checks ...healthz.HealthChecker) http.Handler {
	pathRecorderMux := mux.NewPathRecorderMux("kube-scheduler")
	healthz.InstallHandler(pathRecorderMux, checks...)
	installMetricHandler(pathRecorderMux, informers, isLeader)
	if config.EnableProfiling {
		routes.Profiling{}.Install(pathRecorderMux)
		if config.EnableContentionProfiling {
			goruntime.SetBlockProfileRate(1)
		}
		routes.DebugFlags{}.Install(pathRecorderMux, "v", routes.StringFlagPutHandler(logs.GlogSetter))
	}
	return pathRecorderMux
}

func getRecorderFactory(cc *schedulerserverconfig.CompletedConfig) profile.RecorderFactory {
	return func(name string) events.EventRecorder {
		return cc.EventBroadcaster.NewRecorder(name)
	}
}

// WithPlugin creates an Option based on plugin name and factory. Please don't remove this function: it is used to register out-of-tree plugins,
// hence there are no references to it from the kubernetes scheduler code base.
func WithPlugin(name string, factory runtime.PluginFactory) Option {
	return func(registry runtime.Registry) error {
		return registry.Register(name, factory)
	}
}

// Setup creates a completed config and a scheduler based on the command args and options
// Setup 函数负责初始化和配置调度器的核心组件。
// 它会设置默认配置、验证选项、完成配置、注册外部插件、创建事件记录器工厂，并最终实例化调度器对象。
func Setup(ctx context.Context, opts *options.Options, outOfTreeRegistryOptions ...Option) (*schedulerserverconfig.CompletedConfig, *scheduler.Scheduler, error) {
	// 获取最新的默认组件配置，并将其设置到 opts 中。
	// latest.Default() 会返回一个包含默认值的 KubeSchedulerConfiguration 对象。
	if cfg, err := latest.Default(); err != nil {
		return nil, nil, err
	} else {
		opts.ComponentConfig = cfg
	}

	// 验证从命令行标志和配置文件加载的选项。
	// 如果有任何验证错误，则聚合这些错误并返回。
	if errs := opts.Validate(); len(errs) > 0 {
		return nil, nil, utilerrors.NewAggregate(errs)
	}

	// 基于已验证的选项 opts 创建基础配置 c。
	// 这个过程通常涉及初始化客户端、Informer 工厂等。
	c, err := opts.Config()
	if err != nil {
		return nil, nil, err
	}

	// 获取已完成的配置 cc。
	// Complete() 方法会填充配置中未设置的字段，使用默认值或从其他来源推断值。
	// 最终得到一个完全填充且准备就绪的配置对象。
	cc := c.Complete()

	// 创建一个用于注册外部（out-of-tree）插件的注册表。
	// 这允许用户在不修改 Kubernetes 核心代码的情况下添加自定义插件。
	outOfTreeRegistry := make(runtime.Registry)
	// 遍历提供的注册选项，并将它们应用到外部注册表。
	// 每个 Option 函数负责向注册表中添加特定的插件类型或实现。
	for _, option := range outOfTreeRegistryOptions {
		if err := option(outOfTreeRegistry); err != nil {
			return nil, nil, err
		}
	}

	// 获取事件记录器工厂，用于在调度过程中记录事件。
	// 这对于调试和监控调度决策非常有用。
	recorderFactory := getRecorderFactory(&cc)

	// 用于存储经过框架处理后完成的调度配置文件。
	// 这些配置文件可能包含默认插件设置或从配置中加载的特定插件设置。
	completedProfiles := make([]kubeschedulerconfig.KubeSchedulerProfile, 0)

	// 使用完成的配置和注册表创建新的调度器实例。
	// 传入了大量的配置项和工厂函数，以定制调度器的行为。
	sched, err := scheduler.New(
		cc.GodelComponentConfig.GodelSchedulerName,
		cc.GodelComponentConfig.SchedulerName,
		cc.GodelCrdClient,
		cc.GodelCrdInformerFactory,
		cc.Client,                    // Kubernetes API 服务器客户端
		cc.InformerFactory,                                    // 标准资源的 Informer 工厂
		cc.DynInformerFactory,                                 // 动态资源的 Informer 工厂
		recorderFactory,                                       // 事件记录器工厂
		ctx.Done(),                                            // 上下文完成通道，用于调度器优雅关闭
		scheduler.WithComponentConfigVersion(cc.ComponentConfig.TypeMeta.APIVersion), // 设置组件配置版本
		scheduler.WithKubeConfig(cc.KubeConfig),               // 设置 kubeconfig
		scheduler.WithProfiles(cc.ComponentConfig.Profiles...), // 设置调度配置文件
		scheduler.WithPercentageOfNodesToScore(cc.ComponentConfig.PercentageOfNodesToScore), // 设置评分节点百分比
		scheduler.WithFrameworkOutOfTreeRegistry(outOfTreeRegistry), // 设置外部插件注册表
		scheduler.WithPodMaxBackoffSeconds(cc.ComponentConfig.PodMaxBackoffSeconds), // 设置 Pod 最大退避时间
		scheduler.WithPodInitialBackoffSeconds(cc.ComponentConfig.PodInitialBackoffSeconds), // 设置 Pod 初始退避时间
		scheduler.WithPodMaxInUnschedulablePodsDuration(cc.PodMaxInUnschedulablePodsDuration), // 设置 Pod 在未调度Pod列表中的最大持续时间
		scheduler.WithExtenders(cc.ComponentConfig.Extenders...), // 设置调度扩展器
		scheduler.WithParallelism(cc.ComponentConfig.Parallelism), // 设置并行度
		// 捕获经过框架处理后的配置文件，以便记录日志。
		scheduler.WithBuildFrameworkCapturer(func(profile kubeschedulerconfig.KubeSchedulerProfile) {
			// 配置文件在框架实例化期间被处理，以设置默认插件和配置。捕获它们用于日志记录。
			completedProfiles = append(completedProfiles, profile)
		}),
	)
	if err != nil {
		return nil, nil, err
	}

	// 如果指定了写入配置的路径，则将组件配置和完成的配置文件写入到该路径。
	// 这对于调试或生成配置模板很有用。
	if err := options.LogOrWriteConfig(opts.WriteConfigTo, &cc.ComponentConfig, completedProfiles); err != nil {
		return nil, nil, err
	}

	// 返回完成的配置对象和创建的调度器实例。
	return &cc, sched, nil
}
