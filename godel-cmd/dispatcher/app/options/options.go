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

package options

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	godelclient "github.com/kubewharf/godel-scheduler-api/pkg/client/clientset/versioned"
	godelclientscheme "github.com/kubewharf/godel-scheduler-api/pkg/client/clientset/versioned/scheme"
	crdinformers "github.com/kubewharf/godel-scheduler-api/pkg/client/informers/externalversions"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	apiserveroptions "k8s.io/apiserver/pkg/server/options"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	clientsetscheme "k8s.io/client-go/kubernetes/scheme"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	cliflag "k8s.io/component-base/cli/flag"
	componentbaseconfig "k8s.io/component-base/config/v1alpha1"
	"k8s.io/klog/v2"

	dispatcherconfig "k8s.io/kubernetes/godel-pkg/dispatcher/config"
	"k8s.io/kubernetes/godel-pkg/dispatcher/config/validation"
	cmdutil "k8s.io/kubernetes/godel-pkg/util/cmd"
	dispatcherappconfig "k8s.io/kubernetes/godel-cmd/dispatcher/app/config"
)

const DefaultLeaderElectionName = "dispatcher"

type Options struct {
	DispatcherConfig dispatcherconfig.GodelDispatcherConfiguration

	// ConfigFile is the location of the scheduler server's configuration file.
	ConfigFile string

	// WriteConfigTo is the path where the default configuration will be written.
	WriteConfigTo string

	Master string

	CombinedInsecureServing *CombinedInsecureServingOptions
}

// NewOptions returns default dispatcher app options.
func NewOptions() (*Options, error) {
	klog.Info("创造options")
	cfg, err := newDefaultDispatcherConfig()
	if err != nil {
		return nil, err
	}

	hhost, hport, err := splitHostIntPort(cfg.HealthzBindAddress)
	if err != nil {
		return nil, err
	}

	o := &Options{
		CombinedInsecureServing: &CombinedInsecureServingOptions{
			Healthz: (&apiserveroptions.DeprecatedInsecureServingOptions{
				BindNetwork: "tcp",
			}).WithLoopback(),
			Metrics: (&apiserveroptions.DeprecatedInsecureServingOptions{
				BindNetwork: "tcp",
			}).WithLoopback(),
			BindPort:    hport,
			BindAddress: hhost,
		},
		DispatcherConfig: *cfg,
	}

	return o, nil
}

func splitHostIntPort(s string) (string, int, error) {
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		return "", 0, err
	}
	portInt, err := strconv.Atoi(port)
	if err != nil {
		return "", 0, err
	}
	return host, portInt, err
}

func newDefaultDispatcherConfig() (*dispatcherconfig.GodelDispatcherConfiguration, error) {
	cfg := dispatcherconfig.GodelDispatcherConfiguration{}

	dispatcherconfig.SetDefaults(&cfg)
	return &cfg, nil
}

// Flags returns flags for a specific scheduler by section name
func (o *Options) Flags() (nfs cliflag.NamedFlagSets) {
	fs := nfs.FlagSet("misc")
	fs.StringVar(&o.ConfigFile, "config", o.ConfigFile, "The path to the configuration file. Flags override values in this file.")
	fs.StringVar(&o.WriteConfigTo, "write-config-to", o.WriteConfigTo, "If set, write the configuration values to this file and exit.")
	fs.StringVar(&o.Master, "master", o.Master, "The address of the Kubernetes API server (overrides any value in kubeconfig)")
	fs.StringVar(&o.DispatcherConfig.ClientConnection.Kubeconfig, "kubeconfig", o.DispatcherConfig.ClientConnection.Kubeconfig, "path to kubeconfig file with authorization and master location information.")
	fs.Float32Var(&o.DispatcherConfig.ClientConnection.QPS, "kube-api-qps", o.DispatcherConfig.ClientConnection.QPS, "QPS to use while talking with kubernetes apiserver. This parameter is ignored if a config file is specified in --config.")
	fs.Int32Var(&o.DispatcherConfig.ClientConnection.Burst, "kube-api-burst", o.DispatcherConfig.ClientConnection.Burst, "burst to use while talking with kubernetes apiserver. This parameter is ignored if a config file is specified in --config.")
	fs.StringVar(o.DispatcherConfig.SchedulerName, "scheduler-name", *o.DispatcherConfig.SchedulerName, "components will deal with pods that pod.Spec.SchedulerName is equal to scheduler-name / is default-scheduler or empty.")

	o.CombinedInsecureServing.AddFlags(nfs.FlagSet("insecure serving"))
	o.DispatcherConfig.Tracer.AddFlags(nfs.FlagSet("tracer"))

	BindFlags(&o.DispatcherConfig.LeaderElection, nfs.FlagSet("leader election"))
	utilfeature.DefaultMutableFeatureGate.AddFlag(nfs.FlagSet("generic"))
	return nfs
}

// ApplyTo applies the dispatcher options to the given dispatcher app configuration.
func (o *Options) ApplyTo(c *dispatcherappconfig.Config) error {
	c.DispatcherConfig = o.DispatcherConfig
	if err := o.CombinedInsecureServing.ApplyTo(c, &c.DispatcherConfig); err != nil {
		return err
	}
	return nil
}

// Validate validates all the required options.
func (o *Options) Validate() []error {
	var errs []error
	if err := validation.ValidateGodelDispatcherConfiguration(&o.DispatcherConfig).ToAggregate(); err != nil {
		errs = append(errs, err.Errors()...)
	}
	return errs
}

// Config return a scheduler config object
// Config 方法根据 Options 构建并返回一个 dispatcherappconfig.Config 对象
// 该方法负责初始化调度器所需的各种客户端、事件广播器、Informer 工厂以及领导者选举配置
func (o *Options) Config() (*dispatcherappconfig.Config, error) {
	// 创建一个空的配置对象
	c := &dispatcherappconfig.Config{}
	// 将 Options 中的配置应用到 Config 对象上
	if err := o.ApplyTo(c); err != nil {
		return nil, err
	}

	// 创建 Kubernetes 客户端集合：核心客户端、领导者选举客户端、事件客户端、Godel CRD 客户端
	client, eventClient, godelCrdClient, err := createClients(c.DispatcherConfig.ClientConnection, o.Master, c.DispatcherConfig.LeaderElection.RenewDeadline.Duration)
	if err != nil {
		return nil, err
	}

	// 创建事件广播器，用于向 Kubernetes API 服务器发送事件
	c.EventBroadcaster = cmdutil.NewEventBroadcasterAdapter(eventClient)

	// 将创建的客户端赋值给配置对象
	c.Client = client
	// 创建核心资源的 Informer 工厂，用于监听和缓存 Kubernetes 核心资源的变化
	c.InformerFactory = cmdutil.NewInformerFactory(client, 0)
	// 将 Godel CRD 客户端赋值给配置对象
	c.GodelCrdClient = godelCrdClient

	// 创建 Godel CRD 资源的 Informer 工厂，用于监听和缓存自定义资源定义的变化
	c.GodelCrdInformerFactory = crdinformers.NewSharedInformerFactory(c.GodelCrdClient, 0)
	// TODO:(godel) 如果无用则删除
	// c.EventClient = eventClient.EventsV1beta1()
	// c.CoreEventClient = eventClient.CoreV1()

	// 将领导者选举配置赋值给配置对象
	c.LeaderElection = nil

	// 返回最终构建好的配置对象
	return c, nil
}

// makeLeaderElectionConfig builds a leader election configuration. It will
// create a new resource lock associated with the configuration.
func makeLeaderElectionConfig(config componentbaseconfig.LeaderElectionConfiguration, client clientset.Interface, recorder record.EventRecorder) (*leaderelection.LeaderElectionConfig, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("unable to get hostname: %v", err)
	}
	// add a uniquifier so that two processes on the same host don't accidentally both become active
	id := hostname + "_" + string(uuid.NewUUID())

	rl, err := resourcelock.New(config.ResourceLock,
		config.ResourceNamespace,
		config.ResourceName,
		client.CoreV1(),
		client.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: recorder,
		})
	if err != nil {
		return nil, fmt.Errorf("couldn't create resource lock: %v", err)
	}

	return &leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: config.LeaseDuration.Duration,
		RenewDeadline: config.RenewDeadline.Duration,
		RetryPeriod:   config.RetryPeriod.Duration,
		WatchDog:      leaderelection.NewLeaderHealthzAdaptor(time.Second * 20),
		Name:          DefaultLeaderElectionName,
	}, nil
}

// createClients 创建并返回多个 Kubernetes 客户端实例
// 返回：核心 API 客户端、领导者选举客户端、事件客户端、Godel CRD 客户端
func createClients(config componentbaseconfig.ClientConnectionConfiguration, masterOverride string, timeout time.Duration) (clientset.Interface, clientset.Interface, godelclient.Interface, error) {
	// 检查是否同时未指定 kubeconfig 文件和 master 地址，如果是则发出警告
	if len(config.Kubeconfig) == 0 && len(masterOverride) == 0 {
		klog.InfoS("WARN: 未指定 --kubeconfig 或 --master 参数。使用默认 API 客户端。这可能无法正常工作")
	}

	klog.InfoS("查看是否配备config")

	// 创建基础的 kubeconfig 配置
	// 首先加载指定的 kubeconfig 文件，然后覆盖 Master 地址（如果提供了 masterOverride）
	kubeConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: config.Kubeconfig},                                 // 指定 kubeconfig 文件路径
		&clientcmd.ConfigOverrides{ClusterInfo: clientcmdapi.Cluster{Server: masterOverride}}).ClientConfig() // 覆盖集群服务器地址
	if err != nil {
		return nil, nil, nil, err
	}

	// 应用客户端连接配置参数到 kubeConfig
	kubeConfig.DisableCompression = true                      // 禁用压缩以提高性能
	kubeConfig.AcceptContentTypes = config.AcceptContentTypes // 设置接受的内容类型
	kubeConfig.ContentType = config.ContentType               // 设置内容类型
	kubeConfig.QPS = config.QPS                               // 设置每秒查询率
	kubeConfig.Burst = int(config.Burst)                      // 设置突发请求数

	// 创建核心 API 客户端，用于与标准 Kubernetes API 交互，添加 "dispatcher" 用户代理
	client, err := clientset.NewForConfig(restclient.AddUserAgent(kubeConfig, "dispatcher"))
	if err != nil {
		return nil, nil, nil, err
	}

	// 将 Godel 客户端 Scheme 添加到标准客户端 Scheme 中，确保能够处理 Godel 自定义资源
	utilruntime.Must(godelclientscheme.AddToScheme(clientsetscheme.Scheme))

	// 创建事件客户端，用于发送和管理 Kubernetes 事件
	eventClient, err := clientset.NewForConfig(kubeConfig)
	if err != nil {
		return nil, nil, nil, err
	}

	// 为 Godel CRD 客户端创建新的 kubeconfig 配置
	// 这里再次创建配置是为了避免与之前的配置相互影响
	crdKubeConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: config.Kubeconfig},
		&clientcmd.ConfigOverrides{ClusterInfo: clientcmdapi.Cluster{Server: masterOverride}}).ClientConfig()
	if err != nil {
		return nil, nil, nil, err
	}

	// 应用基本的连接配置到 CRD 客户端配置
	crdKubeConfig.DisableCompression = true // 禁用压缩
	crdKubeConfig.QPS = config.QPS          // 设置 QPS
	// TODO: 考虑让配置结构体使用 int 而不是 int32？
	crdKubeConfig.Burst = int(config.Burst) // 设置突发请求数

	// 创建 Godel CRD 客户端，用于与 Godel 自定义资源定义进行交互，添加 "dispatcher" 用户代理
	godelCrdClient, err := godelclient.NewForConfig(restclient.AddUserAgent(crdKubeConfig, "dispatcher"))
	if err != nil {
		return nil, nil, nil, err
	}

	// 返回所有创建的客户端
	return client, eventClient, godelCrdClient, nil
}
