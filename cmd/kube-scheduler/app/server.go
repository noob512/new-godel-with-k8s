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
	"time"
)

func init() {
	utilruntime.Must(logs.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
}

// Option configures a framework.Registry.
type Option func(runtime.Registry) error

// NewSchedulerCommand creates a *cobra.Command object with default parameters and registryOptions
// 此函数创建一个带有默认参数和注册选项的 *cobra.Command 对象
func NewSchedulerCommand(registryOptions ...Option) *cobra.Command {
	//1.需要进入该函数中修改一些初始配置选项
	// (Comment 1) 此处是修改初始配置选项的地方（例如，设置默认值或应用特定逻辑）
	opts := options.NewOptions() // 创建一个包含默认配置选项的结构体实例 opts

	cmd := &cobra.Command{ // 初始化 cobra Command 结构体
		Use: "kube-scheduler", // 定义命令的使用方式，这里是 'kube-scheduler'
		Long: `The Kubernetes scheduler is a control plane process which assigns
Pods to Nodes. The scheduler determines which Nodes are valid placements for
each Pod in the scheduling queue according to constraints and available
resources. The scheduler then ranks each valid Node and binds the Pod to a
suitable Node. Multiple different schedulers may be used within a cluster;
kube-scheduler is the reference implementation.
See [scheduling](https://kubernetes.io/docs/concepts/scheduling-eviction/  )
for more information about scheduling and the kube-scheduler component.`,
		// 定义命令的详细描述，说明了 kube-scheduler 的作用：将 Pod 分配给 Node，
		// 根据约束和可用资源确定有效的放置位置，对有效 Node 进行排名并绑定 Pod。
		// 提供了关于调度和 kube-scheduler 组件的文档链接。
		RunE: func(cmd *cobra.Command, args []string) error { // 定义命令执行时的核心逻辑
			return runCommand(cmd, opts, registryOptions...) // 调用 runCommand 函数，传入当前命令、配置选项和注册选项
		},
		Args: func(cmd *cobra.Command, args []string) error { // 定义对命令参数的验证逻辑
			for _, arg := range args { // 遍历传入的参数
				if len(arg) > 0 { // 如果存在任何参数
					return fmt.Errorf("%q does not take any arguments, got %q", cmd.CommandPath(), args) // 返回错误，因为该命令不接受参数
				}
			}
			return nil // 如果没有参数，则验证通过
		},
	}

	nfs := opts.Flags // 获取 opts 结构体中预先定义好的 FlagSet 集合
	verflag.AddFlags(nfs.FlagSet("global")) // 添加版本查询相关的全局标志 (e.g., --version)
	globalflag.AddGlobalFlags(nfs.FlagSet("global"), cmd.Name(), logs.SkipLoggingConfigurationFlags()) // 添加通用的全局标志 (e.g., --help, --logtostderr)，并可能排除日志配置相关标志
	fs := cmd.Flags() // 获取当前 cobra 命令的 FlagSet
	for _, f := range nfs.FlagSets { // 遍历 opts 中的所有 FlagSet
		fs.AddFlagSet(f) // 将 opts 中定义的每一个 FlagSet 添加到当前命令的 FlagSet 中，使命令可以接收这些标志作为输入
	}

	cols, _, _ := term.TerminalSize(cmd.OutOrStdout()) // 获取终端宽度，用于格式化帮助信息输出
	cliflag.SetUsageAndHelpFunc(cmd, *nfs, cols) // 设置命令的 Usage 和 Help 信息打印函数

	if err := cmd.MarkFlagFilename("config", "yaml", "yml", "json"); err != nil { // 标记 'config' 标志的值应被视为文件名，并指定允许的扩展名
		klog.ErrorS(err, "Failed to mark flag filename") // 如果标记失败，则记录错误日志
	}

	return cmd // 返回构建好的 cobra Command 对象
}

// runCommand runs the scheduler.
func runCommand(cmd *cobra.Command, opts *options.Options, registryOptions ...Option) error {
	verflag.PrintAndExitIfRequested()

	// Activate logging as soon as possible, after that
	// show flags with the final logging configuration.
	if err := opts.Logs.ValidateAndApply(utilfeature.DefaultFeatureGate); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	cliflag.PrintFlags(cmd.Flags())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		stopCh := server.SetupSignalHandler()
		<-stopCh
		cancel()
	}()

	//主要的修改集中在这里面
	cc, sched, err := Setup(ctx, opts, registryOptions...)
	if err != nil {
		return err
	}

	return Run(ctx, cc, sched)
}

// Run 根据给定的配置执行调度器。仅在出现错误或上下文完成时返回。
func Run(ctx context.Context, cc *schedulerserverconfig.CompletedConfig, sched *scheduler.Scheduler) error {
	// 为了帮助调试，立即记录版本信息
	klog.InfoS("Starting Kubernetes Scheduler", "version", version.Get())

	// 记录 Golang 运行时设置
	klog.InfoS("Golang settings", "GOGC", os.Getenv("GOGC"), "GOMAXPROCS", os.Getenv("GOMAXPROCS"), "GOTRACEBACK", os.Getenv("GOTRACEBACK"))

	// 准备事件广播器。
	cc.EventBroadcaster.StartRecordingToSink(ctx.Done())

	// 启动所有 informer。
	cc.InformerFactory.Start(ctx.Done())
	klog.Info("所有informer启动成功")
	// DynInformerFactory 在测试中可能为 nil。
	if cc.DynInformerFactory != nil {
		cc.DynInformerFactory.Start(ctx.Done())
	}

	// 等待所有缓存同步后再进行调度。
	klog.Info("等待informer同步")
	cc.InformerFactory.WaitForCacheSync(ctx.Done())
	klog.Info("寻常informer同步完成")
	// DynInformerFactory 在测试中可能为 nil。
	if cc.DynInformerFactory != nil {
		cc.DynInformerFactory.WaitForCacheSync(ctx.Done())
	}

	// 启动 Godel CRD informer 工厂
	klog.Info("启动crd-informer同步")
	cc.GodelCrdInformerFactory.Start(ctx.Done())
	// 等待 Godel CRD informer 缓存同步
	cc.GodelCrdInformerFactory.WaitForCacheSync(ctx.Done())
	klog.Info("crd-informer同步完成")

	// 运行调度器
	sched.Run(ctx)
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
// Setup creates a completed config and a scheduler based on the command args and options
func Setup(ctx context.Context, opts *options.Options, outOfTreeRegistryOptions ...Option) (*schedulerserverconfig.CompletedConfig, *scheduler.Scheduler, error) {
	klog.Info("1")
	if cfg, err := latest.Default(); err != nil {
		return nil, nil, err
	} else {
		opts.ComponentConfig = cfg
	}
	klog.Info("2")
	if errs := opts.Validate(); len(errs) > 0 {
		return nil, nil, utilerrors.NewAggregate(errs)
	}
	klog.Info("3")
	c, err := opts.Config()
	//1.在这个进行修改，主要是为了将之前创建的option传递给c
	if err != nil {
		return nil, nil, err
	}

	// Get the completed config
	cc := c.Complete()

	outOfTreeRegistry := make(runtime.Registry)
	for _, option := range outOfTreeRegistryOptions {
		if err := option(outOfTreeRegistry); err != nil {
			return nil, nil, err
		}
	}
	klog.InfoS("cc.GodelComponentConfig.SchedulerName", "name", *cc.GodelComponentConfig.SchedulerName)
	klog.InfoS("cc.ComponentConfig.SchedulerIndex", "index", int(cc.ComponentConfig.SchedulerIndex))

	recorderFactory := getRecorderFactory(&cc)
	completedProfiles := make([]kubeschedulerconfig.KubeSchedulerProfile, 0)
	// Create the scheduler.
	//2.在这个new函数中修改，主要是给scheduler额外添加一些属性
	sched, err := scheduler.New(
		cc.GodelComponentConfig.GodelSchedulerName,
		cc.GodelComponentConfig.SchedulerName,
		cc.GodelCrdClient,
		cc.GodelCrdInformerFactory,
		cc.Client,
		cc.InformerFactory,
		cc.DynInformerFactory,
		recorderFactory,
		ctx.Done(),
		scheduler.WithComponentConfigVersion(cc.ComponentConfig.TypeMeta.APIVersion),
		scheduler.WithKubeConfig(cc.KubeConfig),
		scheduler.WithProfiles(cc.ComponentConfig.Profiles...),
		scheduler.WithPercentageOfNodesToScore(cc.ComponentConfig.PercentageOfNodesToScore),
		scheduler.WithFrameworkOutOfTreeRegistry(outOfTreeRegistry),
		scheduler.WithPodMaxBackoffSeconds(cc.ComponentConfig.PodMaxBackoffSeconds),
		scheduler.WithPodInitialBackoffSeconds(cc.ComponentConfig.PodInitialBackoffSeconds),
		scheduler.WithPodMaxInUnschedulablePodsDuration(cc.PodMaxInUnschedulablePodsDuration),
		scheduler.WithExtenders(cc.ComponentConfig.Extenders...),
		scheduler.WithParallelism(cc.ComponentConfig.Parallelism),
		scheduler.WithBuildFrameworkCapturer(func(profile kubeschedulerconfig.KubeSchedulerProfile) {
			// Profiles are processed during Framework instantiation to set default plugins and configurations. Capturing them for logging
			completedProfiles = append(completedProfiles, profile)
		}),
		// 备选调度和分区同步配置
		scheduler.WithNumBackupNodes(int(cc.ComponentConfig.NumBackupNodes)),
		scheduler.WithBackupUpdateStrategy(string(cc.ComponentConfig.BackupUpdateStrategy)),
		scheduler.WithEnableSecondaryReserve(cc.ComponentConfig.EnableSecondaryReserve),
		scheduler.WithSyncMode(string(cc.ComponentConfig.SyncMode)),
		scheduler.WithScheduleStrategy(string(cc.ComponentConfig.ScheduleStrategy)),
		scheduler.WithNumPartitions(int(cc.ComponentConfig.NumPartitions)),
		scheduler.WithSchedulerIndex(int(cc.ComponentConfig.SchedulerIndex)),
		scheduler.WithSyncGap(time.Duration(cc.ComponentConfig.SyncGapSeconds)*time.Second),
	)
	if err != nil {
		return nil, nil, err
	}
	if err := options.LogOrWriteConfig(opts.WriteConfigTo, &cc.ComponentConfig, completedProfiles); err != nil {
		return nil, nil, err
	}

	return &cc, sched, nil
}

// parseUpdateStrategy 将配置字符串转换为 UpdateStrategy 枚举
func parseUpdateStrategy(strategy string) string {
	switch strategy {
	case "first":
		return "first"
	case "all":
		return "all"
	case "p":
		return "p"
	case "p-slot":
		return "p-slot"
	default:
		return "p"
	}
}

// parseSyncMode 将配置字符串转换为 SyncMode 枚举
func parseSyncMode(mode string) string {
	switch mode {
	case "globSync":
		return "globSync"
	case "sameSync":
		return "sameSync"
	case "diffSync":
		return "diffSync"
	default:
		return "diffSync"
	}
}

// parseScheduleStrategy 将配置字符串转换为 ScheduleStrategy 枚举
func parseScheduleStrategy(strategy string) string {
	switch strategy {
	case "quality":
		return "quality"
	case "latency":
		return "latency"
	default:
		return "latency"
	}
}
