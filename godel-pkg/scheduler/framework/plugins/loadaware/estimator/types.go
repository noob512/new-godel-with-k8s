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

package estimator

import (
	corev1 "k8s.io/api/core/v1"

	framework "k8s.io/kubernetes/godel-pkg/framework/api"
	"k8s.io/kubernetes/godel-pkg/scheduler/apis/config"
	"k8s.io/kubernetes/godel-pkg/scheduler/framework/handle"
	podutil "k8s.io/kubernetes/godel-pkg/util/pod"
)

type FactoryFn func(args *config.LoadAwareArgs, handle handle.PodFrameworkHandle) (Estimator, error)

var Estimators = map[string]FactoryFn{
	DefaultEstimatorName:    NewDefaultEstimator,
	NodeMetricEstimatorName: NewNodeMetricEstimator,
}

type Estimator interface {
	Name() string
	ValidateNode(nodeInfo framework.NodeInfo, resourceType podutil.PodResourceType) *framework.Status
	EstimatePod(pod *corev1.Pod) (*framework.Resource, error)
	EstimateNode(info framework.NodeInfo, resourceType podutil.PodResourceType) (*framework.Resource, error)
}

func NewEstimator(args *config.LoadAwareArgs, handle handle.PodFrameworkHandle) (Estimator, error) {
	factoryFn := Estimators[args.Estimator]
	if factoryFn == nil {
		factoryFn = NewDefaultEstimator
	}
	return factoryFn(args, handle)
}
