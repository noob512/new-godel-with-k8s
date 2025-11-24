/*
Copyright 2018 The Kubernetes Authors.

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
	"time"
	"math"

	godelclient "github.com/kubewharf/godel-scheduler-api/pkg/client/clientset/versioned"
	godelschedulerconfig "k8s.io/kubernetes/godel-pkg/scheduler/apis/config"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	apiserveroptions "k8s.io/apiserver/pkg/server/options"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	cliflag "k8s.io/component-base/cli/flag"
	componentbaseconfig "k8s.io/component-base/config"
	"k8s.io/component-base/config/options"
	"k8s.io/component-base/logs"
	"k8s.io/component-base/metrics"
	schedulerappconfig "k8s.io/kubernetes/cmd/kube-scheduler/app/config"
	"k8s.io/kubernetes/pkg/scheduler"
	kubeschedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	netutils "k8s.io/utils/net"
)

// Options has all the params needed to run a Scheduler
type Options struct {
	godelClient godelclient.Interface
	GodelComponentConfig godelschedulerconfig.GodelSchedulerConfiguration
	SchedulerRenewIntervalSeconds int64
	// 以下字段是为了向后兼容而保留的，计划在未来版本中移除。
	// UnitMaxBackoffSeconds 指定调度器在处理失败时的最大退避时间（秒）。
	UnitMaxBackoffSeconds int64
	// UnitInitialBackoffSeconds 指定调度器在处理失败时的初始退避时间（秒）。
	UnitInitialBackoffSeconds int64
	// DisablePreemption 控制是否禁用调度器的抢占（Preemption）功能。
	// 如果为 true，则调度器不会尝试驱逐低优先级 Pod 来为高优先级 Pod 腾出空间。
	DisablePreemption bool
	// AttemptImpactFactorOnPriority 是一个影响优先级计算的因子，用于在调度决策中考虑潜在的抢占影响。
	AttemptImpactFactorOnPriority float64
	//----------------------------------------------------
	// The default values.
	ComponentConfig *kubeschedulerconfig.KubeSchedulerConfiguration

	SecureServing  *apiserveroptions.SecureServingOptionsWithLoopback
	Authentication *apiserveroptions.DelegatingAuthenticationOptions
	Authorization  *apiserveroptions.DelegatingAuthorizationOptions
	Metrics        *metrics.Options
	Logs           *logs.Options
	Deprecated     *DeprecatedOptions
	LeaderElection *componentbaseconfig.LeaderElectionConfiguration

	// ConfigFile is the location of the scheduler server's configuration file.
	ConfigFile string

	// WriteConfigTo is the path where the default configuration will be written.
	WriteConfigTo string

	Master string

	// Flags hold the parsed CLI flags.
	Flags *cliflag.NamedFlagSets
}
//--------------------------------------------------------------------
const (
	// DefaultUnitInitialBackoffInSeconds is the default value for the initial backoff duration
	// for unschedulable units. To change the default podInitialBackoffDurationSeconds used by the
	// scheduler, update the ComponentConfig value in defaults.go
	DefaultUnitInitialBackoffInSeconds = 10
	// DefaultUnitMaxBackoffInSeconds is the default value for the max backoff duration
	// for unschedulable units. To change the default unitMaxBackoffDurationSeconds used by the
	// scheduler, update the ComponentConfig value in defaults.go
	DefaultUnitMaxBackoffInSeconds = 300
	// DefaultDisablePreemption is the default value for the option to disable preemption ability
	// for unschedulable pods.
	DefaultDisablePreemption        = true
	CandidateSelectPolicyBest       = "Best"
	CandidateSelectPolicyBetter     = "Better"
	CandidateSelectPolicyRandom     = "Random"
	BetterPreemptionPolicyAscending = "Ascending"
	BetterPreemptionPolicyDichotomy = "Dichotomy"
	// DefaultBlockQueue is the default value for the option to use block queue for SchedulingQueue.
	DefaultBlockQueue = false
	// DefaultPodUpgradePriorityInMinutes is the default upgrade priority duration for godel sort.
	DefaultPodUpgradePriorityInMinutes = 5
	// DefaultGodelSchedulerName defines the name of default scheduler.
	DefaultGodelSchedulerName = "my-cus-godel-scheduler"
	// DefaultRenewIntervalInSeconds is the default value for the renew interval duration for scheduler.
	DefaultRenewIntervalInSeconds = 30

	// DefaultSchedulerName is default high level scheduler name
	DefaultSchedulerName = "godel-scheduler"

	// DefaultClientConnectionQPS is default scheduler qps
	DefaultClientConnectionQPS = 10000.0
	// DefaultClientConnectionBurst is default scheduler burst
	DefaultClientConnectionBurst = 10000

	// DefaultIDC is default idc name for godel scheduler
	DefaultIDC = "lq"
	// DefaultCluster is default cluster name for godel scheduler
	DefaultCluster = "default"
	// DefaultTracer is default tracer name for godel scheduler

	DefaultSubClusterKey = ""

	// DefaultAttemptImpactFactorOnPriority is the default attempt factors used by godel sort
	DefaultAttemptImpactFactorOnPriority = 10.0

	DefaultMaxWaitingDeletionDuration = 120

	DefaultReservationTimeOutSeconds = 60
)

const (
	// DefaultPercentageOfNodesToScore defines the percentage of nodes of all nodes
	// that once found feasible, the scheduler stops looking for more nodes.
	// A value of 0 means adaptive, meaning the scheduler figures out a proper default.
	DefaultPercentageOfNodesToScore = 0

	DefaultIncreasedPercentageOfNodesToScore = 0

	// MaxCustomPriorityScore is the max score UtilizationShapePoint expects.
	MaxCustomPriorityScore int64 = 10

	// MaxTotalScore is the maximum total score.
	MaxTotalScore int64 = math.MaxInt64

	// MaxWeight defines the max weight value allowed for custom PriorityPolicy
	MaxWeight = MaxTotalScore / MaxCustomPriorityScore
)

func newDefaultComponentConfig() (*godelschedulerconfig.GodelSchedulerConfiguration, error) {
	cfg := godelschedulerconfig.GodelSchedulerConfiguration{}

	SetDefaults_GodelSchedulerConfiguration(&cfg)
	return &cfg, nil
}

// SetDefaults_GodelSchedulerConfiguration 为 Godel 调度器配置结构体设置合理的默认值。
// 该函数确保即使用户未显式配置某些字段，调度器也能以安全、合理的默认行为运行。
func SetDefaults_GodelSchedulerConfiguration(obj *godelschedulerconfig.GodelSchedulerConfiguration) {
	

	// 2. 客户端连接（ClientConnection）与绑定地址（Healthz/Metrics）配置
	{
		// 默认使用 Protobuf 格式与 Kubernetes API Server 通信，以提升性能
		if len(obj.ClientConnection.ContentType) == 0 {
			obj.ClientConnection.ContentType = "application/vnd.kubernetes.protobuf"
		}

		// 调度器对 QPS 和 Burst 有特定需求，设置专属默认值（高于通用客户端）
		if obj.ClientConnection.QPS == 0.0 {
			obj.ClientConnection.QPS = DefaultClientConnectionQPS
		}
		if obj.ClientConnection.Burst == 0 {
			obj.ClientConnection.Burst = DefaultClientConnectionBurst
		}
	}

	// 3. 调试相关配置（Profiling）
	{
		// 默认启用性能分析（pprof）
		if obj.EnableProfiling == nil {
			enableProfiling := true
			obj.EnableProfiling = &enableProfiling
		}

		// 若启用了性能分析，则默认也启用竞争分析（contention profiling）
		if *obj.EnableProfiling && obj.EnableContentionProfiling == nil {
			enableContentionProfiling := true
			obj.EnableContentionProfiling = &enableContentionProfiling
		}
	}

	// 4. Godel 调度器核心配置
	{
		// 若未设置 Godel 调度器名称，使用默认名称
		if len(obj.GodelSchedulerName) == 0 {
			obj.GodelSchedulerName = "my-cus-k8s"+DefaultGodelSchedulerName //这个可以自定义
		}

		// 设置 Kubernetes 侧使用的调度器名称（用于 Pod.Spec.SchedulerName）
		if obj.SchedulerName == nil {
			defaultValue := DefaultSchedulerName
			//这个必需与Kubernetes 中调度器的标识名（对应 Pod.spec.schedulerName）一致
			obj.SchedulerName = &defaultValue
		}

		// 若未配置追踪器（Tracer），使用无操作（No-op）默认选项


		// 设置子集群标识的标签键（用于多集群调度）
		if obj.SubClusterKey == nil {
			defaultValue := DefaultSubClusterKey
			obj.SubClusterKey = &defaultValue
		}

		// 设置资源预留（Reservation）的超时时间（秒），若未配置或非法则使用默认值
		if obj.ReservationTimeOutSeconds <= 0 {
			obj.ReservationTimeOutSeconds = DefaultReservationTimeOutSeconds
		}
	}

	// 5. Godel 调度器默认 Profile（调度策略配置）
	{
		// 若未设置默认 Profile，创建一个空的
		if obj.DefaultProfile == nil {
			obj.DefaultProfile = &godelschedulerconfig.GodelSchedulerProfile{}
		}

		// 设置默认调度时要评分的节点百分比（用于性能优化）
		if obj.DefaultProfile.PercentageOfNodesToScore == nil {
			percentageOfNodesToScore := int32(DefaultPercentageOfNodesToScore)
			obj.DefaultProfile.PercentageOfNodesToScore = &percentageOfNodesToScore
		}

		// 设置负载较高时增加的节点评分百分比
		if obj.DefaultProfile.IncreasedPercentageOfNodesToScore == nil {
			increasedPercentageOfNodesToScore := int32(DefaultIncreasedPercentageOfNodesToScore)
			obj.DefaultProfile.IncreasedPercentageOfNodesToScore = &increasedPercentageOfNodesToScore
		}

		// 设置调度单元（如 Pod）重试的初始退避时间（秒）
		if obj.DefaultProfile.UnitInitialBackoffSeconds == nil {
			defaultUnitInitialBackoffInSeconds := int64(DefaultUnitInitialBackoffInSeconds)
			obj.DefaultProfile.UnitInitialBackoffSeconds = &defaultUnitInitialBackoffInSeconds
		}

		// 设置调度单元重试的最大退避时间（秒）
		if obj.DefaultProfile.UnitMaxBackoffSeconds == nil {
			defaultUnitMaxBackoffInSeconds := int64(DefaultUnitMaxBackoffInSeconds)
			obj.DefaultProfile.UnitMaxBackoffSeconds = &defaultUnitMaxBackoffInSeconds
		}

		// 设置重试次数对调度优先级的影响因子
		if obj.DefaultProfile.AttemptImpactFactorOnPriority == nil {
			attemptImpactFactorOnPriority := DefaultAttemptImpactFactorOnPriority
			obj.DefaultProfile.AttemptImpactFactorOnPriority = &attemptImpactFactorOnPriority
		}

		// 默认不禁用抢占（Preemption）
		if obj.DefaultProfile.DisablePreemption == nil {
			value := true
			obj.DefaultProfile.DisablePreemption = &value
		}

		// 默认不禁用阻塞队列（BlockQueue）功能
		if obj.DefaultProfile.DisablePreemption == nil {
			value := false
			obj.DefaultProfile.DisablePreemption = &value
		}

		// 设置等待删除 Pod 的最大容忍时长（用于资源回收）
		if obj.DefaultProfile.MaxWaitingDeletionDuration == 0 {
			obj.DefaultProfile.MaxWaitingDeletionDuration = DefaultMaxWaitingDeletionDuration
		}

		// 设置候选节点选择策略（如随机选择）
		if obj.DefaultProfile.CandidatesSelectPolicy == nil {
			value := CandidateSelectPolicyRandom
			obj.DefaultProfile.CandidatesSelectPolicy = &value
		}

		// 设置更优节点选择策略列表（用于抢占决策，如升序、二分查找等）
		if obj.DefaultProfile.BetterSelectPolicies == nil {
			obj.DefaultProfile.BetterSelectPolicies = &godelschedulerconfig.StringSlice{
				BetterPreemptionPolicyAscending,
				BetterPreemptionPolicyDichotomy,
			}
		}
	}
}



//-----------------------------------------------------------------------

// NewOptions returns default scheduler app options.
func NewOptions() *Options {
	cfg, err := newDefaultComponentConfig()
	if err != nil {
		return nil
	}
	//-----------------------------------------------
	o := &Options{
		GodelComponentConfig:          *cfg,
		SecureServing:  apiserveroptions.NewSecureServingOptions().WithLoopback(),
		Authentication: apiserveroptions.NewDelegatingAuthenticationOptions(),
		Authorization:  apiserveroptions.NewDelegatingAuthorizationOptions(),
		Deprecated: &DeprecatedOptions{
			PodMaxInUnschedulablePodsDuration: 5 * time.Minute,
		},
		LeaderElection: &componentbaseconfig.LeaderElectionConfiguration{
			LeaderElect:       true,
			LeaseDuration:     metav1.Duration{Duration: 15 * time.Second},
			RenewDeadline:     metav1.Duration{Duration: 10 * time.Second},
			RetryPeriod:       metav1.Duration{Duration: 2 * time.Second},
			ResourceLock:      "leases",
			ResourceName:      "kube-scheduler",
			ResourceNamespace: "kube-system",
		},
		Metrics: metrics.NewOptions(),
		Logs:    logs.NewOptions(),
	}

	o.Authentication.TolerateInClusterLookupFailure = true
	o.Authentication.RemoteKubeConfigFileOptional = true
	o.Authorization.RemoteKubeConfigFileOptional = true

	// Set the PairName but leave certificate directory blank to generate in-memory by default
	o.SecureServing.ServerCert.CertDirectory = ""
	o.SecureServing.ServerCert.PairName = "kube-scheduler"
	o.SecureServing.BindPort = kubeschedulerconfig.DefaultKubeSchedulerPort

	o.initFlags()

	return o
}

// ApplyDeprecated obtains the deprecated CLI args and set them to `o.ComponentConfig` if specified.
func (o *Options) ApplyDeprecated() {
	if o.Flags == nil {
		return
	}
	// Obtain deprecated CLI args. Set them to cfg if specified in command line.
	deprecated := o.Flags.FlagSet("deprecated")
	if deprecated.Changed("profiling") {
		o.ComponentConfig.EnableProfiling = o.Deprecated.EnableProfiling
	}
	if deprecated.Changed("contention-profiling") {
		o.ComponentConfig.EnableContentionProfiling = o.Deprecated.EnableContentionProfiling
	}
	if deprecated.Changed("kubeconfig") {
		o.ComponentConfig.ClientConnection.Kubeconfig = o.Deprecated.Kubeconfig
	}
	if deprecated.Changed("kube-api-content-type") {
		o.ComponentConfig.ClientConnection.ContentType = o.Deprecated.ContentType
	}
	if deprecated.Changed("kube-api-qps") {
		o.ComponentConfig.ClientConnection.QPS = o.Deprecated.QPS
	}
	if deprecated.Changed("kube-api-burst") {
		o.ComponentConfig.ClientConnection.Burst = o.Deprecated.Burst
	}
	if deprecated.Changed("lock-object-namespace") {
		o.ComponentConfig.LeaderElection.ResourceNamespace = o.Deprecated.ResourceNamespace
	}
	if deprecated.Changed("lock-object-name") {
		o.ComponentConfig.LeaderElection.ResourceName = o.Deprecated.ResourceName
	}
}

// ApplyLeaderElectionTo obtains the CLI args related with leaderelection, and override the values in `cfg`.
// Then the `cfg` object is injected into the `options` object.
func (o *Options) ApplyLeaderElectionTo(cfg *kubeschedulerconfig.KubeSchedulerConfiguration) {
	if o.Flags == nil {
		return
	}
	// Obtain CLI args related with leaderelection. Set them to `cfg` if specified in command line.
	leaderelection := o.Flags.FlagSet("leader election")
	if leaderelection.Changed("leader-elect") {
		cfg.LeaderElection.LeaderElect = o.LeaderElection.LeaderElect
	}
	if leaderelection.Changed("leader-elect-lease-duration") {
		cfg.LeaderElection.LeaseDuration = o.LeaderElection.LeaseDuration
	}
	if leaderelection.Changed("leader-elect-renew-deadline") {
		cfg.LeaderElection.RenewDeadline = o.LeaderElection.RenewDeadline
	}
	if leaderelection.Changed("leader-elect-retry-period") {
		cfg.LeaderElection.RetryPeriod = o.LeaderElection.RetryPeriod
	}
	if leaderelection.Changed("leader-elect-resource-lock") {
		cfg.LeaderElection.ResourceLock = o.LeaderElection.ResourceLock
	}
	if leaderelection.Changed("leader-elect-resource-name") {
		cfg.LeaderElection.ResourceName = o.LeaderElection.ResourceName
	}
	if leaderelection.Changed("leader-elect-resource-namespace") {
		cfg.LeaderElection.ResourceNamespace = o.LeaderElection.ResourceNamespace
	}

	o.ComponentConfig = cfg
}

// initFlags initializes flags by section name.
func (o *Options) initFlags() {
	if o.Flags != nil {
		return
	}

	nfs := cliflag.NamedFlagSets{}
	fs := nfs.FlagSet("misc")
	fs.StringVar(&o.ConfigFile, "config", o.ConfigFile, "The path to the configuration file.")
	fs.StringVar(&o.WriteConfigTo, "write-config-to", o.WriteConfigTo, "If set, write the configuration values to this file and exit.")
	fs.StringVar(&o.Master, "master", o.Master, "The address of the Kubernetes API server (overrides any value in kubeconfig)")

	o.SecureServing.AddFlags(nfs.FlagSet("secure serving"))
	o.Authentication.AddFlags(nfs.FlagSet("authentication"))
	o.Authorization.AddFlags(nfs.FlagSet("authorization"))
	o.Deprecated.AddFlags(nfs.FlagSet("deprecated"))
	options.BindLeaderElectionFlags(o.LeaderElection, nfs.FlagSet("leader election"))
	utilfeature.DefaultMutableFeatureGate.AddFlag(nfs.FlagSet("feature gate"))
	o.Metrics.AddFlags(nfs.FlagSet("metrics"))
	o.Logs.AddFlags(nfs.FlagSet("logs"))

	o.Flags = &nfs
}

// ApplyTo applies the scheduler options to the given scheduler app configuration.
func (o *Options) ApplyTo(c *schedulerappconfig.Config) error {
	if len(o.ConfigFile) == 0 {
		// If the --config arg is not specified, honor the deprecated as well as leader election CLI args.
		o.ApplyDeprecated()
		o.ApplyLeaderElectionTo(o.ComponentConfig)
		c.ComponentConfig = *o.ComponentConfig
	} else {
		cfg, err := loadConfigFromFile(o.ConfigFile)
		if err != nil {
			return err
		}
		// If the --config arg is specified, honor the leader election CLI args only.
		o.ApplyLeaderElectionTo(cfg)

		if err := validation.ValidateKubeSchedulerConfiguration(cfg); err != nil {
			return err
		}

		c.ComponentConfig = *cfg
	}

	if err := o.SecureServing.ApplyTo(&c.SecureServing, &c.LoopbackClientConfig); err != nil {
		return err
	}
	if o.SecureServing != nil && (o.SecureServing.BindPort != 0 || o.SecureServing.Listener != nil) {
		if err := o.Authentication.ApplyTo(&c.Authentication, c.SecureServing, nil); err != nil {
			return err
		}
		if err := o.Authorization.ApplyTo(&c.Authorization); err != nil {
			return err
		}
	}
	o.Metrics.Apply()

	// Apply value independently instead of using ApplyDeprecated() because it can't be configured via ComponentConfig.
	if o.Deprecated != nil {
		c.PodMaxInUnschedulablePodsDuration = o.Deprecated.PodMaxInUnschedulablePodsDuration
	}

	return nil
}

// Validate validates all the required options.
func (o *Options) Validate() []error {
	var errs []error

	if err := validation.ValidateKubeSchedulerConfiguration(o.ComponentConfig); err != nil {
		errs = append(errs, err.Errors()...)
	}
	errs = append(errs, o.SecureServing.Validate()...)
	errs = append(errs, o.Authentication.Validate()...)
	errs = append(errs, o.Authorization.Validate()...)
	errs = append(errs, o.Metrics.Validate()...)

	return errs
}

// Config return a scheduler config object
func (o *Options) Config() (*schedulerappconfig.Config, error) {
	if o.SecureServing != nil {
		if err := o.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, []net.IP{netutils.ParseIPSloppy("127.0.0.1")}); err != nil {
			return nil, fmt.Errorf("error creating self-signed certificates: %v", err)
		}
	}

	c := &schedulerappconfig.Config{}
	if err := o.ApplyTo(c); err != nil {
		return nil, err
	}

	// Prepare kube config.
	kubeConfig, err := createKubeConfig(c.ComponentConfig.ClientConnection, o.Master)
	if err != nil {
		return nil, err
	}

	// Prepare kube clients.
	client, eventClient, err := createClients(kubeConfig)
	if err != nil {
		return nil, err
	}

	c.EventBroadcaster = events.NewEventBroadcasterAdapter(eventClient)

	// Set up leader election if enabled.
	var leaderElectionConfig *leaderelection.LeaderElectionConfig
	if c.ComponentConfig.LeaderElection.LeaderElect {
		// Use the scheduler name in the first profile to record leader election.
		schedulerName := corev1.DefaultSchedulerName
		if len(c.ComponentConfig.Profiles) != 0 {
			schedulerName = c.ComponentConfig.Profiles[0].SchedulerName
		}
		coreRecorder := c.EventBroadcaster.DeprecatedNewLegacyRecorder(schedulerName)
		leaderElectionConfig, err = makeLeaderElectionConfig(c.ComponentConfig.LeaderElection, kubeConfig, coreRecorder)
		if err != nil {
			return nil, err
		}
	}

	c.Client = client
	c.KubeConfig = kubeConfig
	c.InformerFactory = scheduler.NewInformerFactory(client, 0)
	dynClient := dynamic.NewForConfigOrDie(kubeConfig)
	c.DynInformerFactory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(dynClient, 0, corev1.NamespaceAll, nil)
	c.LeaderElection = leaderElectionConfig

	return c, nil
}

// makeLeaderElectionConfig builds a leader election configuration. It will
// create a new resource lock associated with the configuration.
func makeLeaderElectionConfig(config componentbaseconfig.LeaderElectionConfiguration, kubeConfig *restclient.Config, recorder record.EventRecorder) (*leaderelection.LeaderElectionConfig, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("unable to get hostname: %v", err)
	}
	// add a uniquifier so that two processes on the same host don't accidentally both become active
	id := hostname + "_" + string(uuid.NewUUID())

	rl, err := resourcelock.NewFromKubeconfig(config.ResourceLock,
		config.ResourceNamespace,
		config.ResourceName,
		resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: recorder,
		},
		kubeConfig,
		config.RenewDeadline.Duration)
	if err != nil {
		return nil, fmt.Errorf("couldn't create resource lock: %v", err)
	}

	return &leaderelection.LeaderElectionConfig{
		Lock:            rl,
		LeaseDuration:   config.LeaseDuration.Duration,
		RenewDeadline:   config.RenewDeadline.Duration,
		RetryPeriod:     config.RetryPeriod.Duration,
		WatchDog:        leaderelection.NewLeaderHealthzAdaptor(time.Second * 20),
		Name:            "kube-scheduler",
		ReleaseOnCancel: true,
	}, nil
}

// createKubeConfig creates a kubeConfig from the given config and masterOverride.
// TODO remove masterOverride when CLI flags are removed.
func createKubeConfig(config componentbaseconfig.ClientConnectionConfiguration, masterOverride string) (*restclient.Config, error) {
	kubeConfig, err := clientcmd.BuildConfigFromFlags(masterOverride, config.Kubeconfig)
	if err != nil {
		return nil, err
	}

	kubeConfig.DisableCompression = true
	kubeConfig.AcceptContentTypes = config.AcceptContentTypes
	kubeConfig.ContentType = config.ContentType
	kubeConfig.QPS = config.QPS
	kubeConfig.Burst = int(config.Burst)

	return kubeConfig, nil
}

// createClients creates a kube client and an event client from the given kubeConfig
func createClients(kubeConfig *restclient.Config) (clientset.Interface, clientset.Interface, error) {
	client, err := clientset.NewForConfig(restclient.AddUserAgent(kubeConfig, "scheduler"))
	if err != nil {
		return nil, nil, err
	}

	eventClient, err := clientset.NewForConfig(kubeConfig)
	if err != nil {
		return nil, nil, err
	}

	return client, eventClient, nil
}
