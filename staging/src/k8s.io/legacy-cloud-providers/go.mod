// This is a generated file. Do not edit directly.

module k8s.io/legacy-cloud-providers

go 1.16

require (
	cloud.google.com/go v0.81.0
	github.com/Azure/azure-sdk-for-go v55.0.0+incompatible
	github.com/Azure/go-autorest/autorest v0.11.18
	github.com/Azure/go-autorest/autorest/adal v0.9.13
	github.com/Azure/go-autorest/autorest/mocks v0.4.1
	github.com/Azure/go-autorest/autorest/to v0.4.0
	github.com/Azure/go-autorest/autorest/validation v0.1.0 // indirect
	github.com/GoogleCloudPlatform/k8s-cloud-provider v1.16.1-0.20210702024009-ea6160c1d0e3
	github.com/aws/aws-sdk-go v1.38.49
	github.com/dnaeon/go-vcr v1.0.1 // indirect
	github.com/gofrs/uuid v4.0.0+incompatible // indirect
	github.com/golang/mock v1.6.0
	github.com/google/go-cmp v0.6.0
	github.com/gophercloud/gophercloud v0.1.0
	github.com/mitchellh/mapstructure v1.4.1
	github.com/rubiojr/go-vhd v0.0.0-20200706105327-02e210299021
	github.com/stretchr/objx v0.2.0 // indirect
	github.com/stretchr/testify v1.11.1
	github.com/vmware/govmomi v0.20.3
	golang.org/x/crypto v0.0.0-20220214200702-86341886e292
	golang.org/x/oauth2 v0.8.0
	google.golang.org/api v0.46.0
	gopkg.in/gcfg.v1 v1.2.0
	gopkg.in/warnings.v0 v0.1.1 // indirect
	k8s.io/api v0.0.0
	k8s.io/apimachinery v0.0.0
	k8s.io/apiserver v0.0.0
	k8s.io/client-go v0.0.0
	k8s.io/cloud-provider v0.0.0
	k8s.io/component-base v0.0.0
	k8s.io/csi-translation-lib v0.0.0
	k8s.io/klog/v2 v2.60.1
	k8s.io/mount-utils v0.0.0
	k8s.io/utils v0.0.0-20220210201930-3a6ce19ff2f9
	sigs.k8s.io/yaml v1.3.0
)

replace (
	github.com/fsnotify/fsnotify => github.com/fsnotify/fsnotify v1.4.9
	github.com/go-logr/logr => github.com/go-logr/logr v1.2.0
	github.com/go-openapi/jsonpointer => github.com/go-openapi/jsonpointer v0.19.5
	github.com/go-openapi/jsonreference => github.com/go-openapi/jsonreference v0.19.5
	github.com/go-openapi/swag => github.com/go-openapi/swag v0.19.14
	github.com/golang/protobuf => github.com/golang/protobuf v1.5.2
	github.com/google/go-cmp => github.com/google/go-cmp v0.5.5
	github.com/google/gofuzz => github.com/google/gofuzz v1.1.0
	github.com/google/uuid => github.com/google/uuid v1.1.2
	github.com/kr/pretty => github.com/kr/pretty v0.2.1
	github.com/mailru/easyjson => github.com/mailru/easyjson v0.7.6
	github.com/spf13/afero => github.com/spf13/afero v1.6.0
	github.com/spf13/pflag => github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify => github.com/stretchr/testify v1.7.0
	golang.org/x/net => golang.org/x/net v0.0.0-20220127200216-cd36cc0744dd
	golang.org/x/oauth2 => golang.org/x/oauth2 v0.0.0-20211104180415-d3ed0bb246c8
	golang.org/x/sys => golang.org/x/sys v0.0.0-20220209214540-3681064d5158
	golang.org/x/term => golang.org/x/term v0.0.0-20210927222741-03fcf44c2211
	golang.org/x/text => golang.org/x/text v0.3.7
	golang.org/x/time => golang.org/x/time v0.0.0-20220210224613-90d013bbcef8
	golang.org/x/tools => golang.org/x/tools v0.1.10-0.20220218145154-897bd77cd717
	google.golang.org/genproto => google.golang.org/genproto v0.0.0-20220107163113-42d7afdf6368
	google.golang.org/grpc => google.golang.org/grpc v1.40.0
	google.golang.org/protobuf => google.golang.org/protobuf v1.27.1
	gopkg.in/yaml.v3 => gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	k8s.io/api => ../api
	k8s.io/apimachinery => ../apimachinery
	k8s.io/apiserver => ../apiserver
	k8s.io/client-go => ../client-go
	k8s.io/cloud-provider => ../cloud-provider
	k8s.io/component-base => ../component-base
	k8s.io/component-helpers => ../component-helpers
	k8s.io/controller-manager => ../controller-manager
	k8s.io/csi-translation-lib => ../csi-translation-lib
	k8s.io/legacy-cloud-providers => ../legacy-cloud-providers
	k8s.io/mount-utils => ../mount-utils
	sigs.k8s.io/json => sigs.k8s.io/json v0.0.0-20211208200746-9f7c6b3444d2
	sigs.k8s.io/structured-merge-diff/v4 => sigs.k8s.io/structured-merge-diff/v4 v4.2.1
	sigs.k8s.io/yaml => sigs.k8s.io/yaml v1.2.0
)
