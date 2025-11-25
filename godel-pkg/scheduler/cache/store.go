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

package cache

import (
	commonstore "k8s.io/kubernetes/godel-pkg/common/store"
	nodestore "k8s.io/kubernetes/godel-pkg/scheduler/cache/commonstores/node_store"
	podstore "k8s.io/kubernetes/godel-pkg/scheduler/cache/commonstores/pod_store"
)

// ATTENTION: The stores should be called in a certain order.
var orderedStoreNames = []commonstore.StoreName{
	nodestore.Name, // NodeStore be placed second to last.
	podstore.Name,  // PodStore must be placed at the end.
}
