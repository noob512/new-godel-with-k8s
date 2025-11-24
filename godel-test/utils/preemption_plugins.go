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

package utils

import (
	framework "k8s.io/kubernetes/godel-pkg/framework/api"
	"k8s.io/kubernetes/godel-pkg/scheduler/apis/config"
	"k8s.io/kubernetes/godel-pkg/scheduler/framework/handle"
	"k8s.io/kubernetes/godel-pkg/scheduler/framework/preemption-plugins/searching/newlystartedprotectionchecker"
	"k8s.io/kubernetes/godel-pkg/scheduler/framework/preemption-plugins/searching/pdbchecker"
	"k8s.io/kubernetes/godel-pkg/scheduler/framework/preemption-plugins/searching/podlauncherchecker"
	"k8s.io/kubernetes/godel-pkg/scheduler/framework/preemption-plugins/searching/preemptibilitychecker"
	"k8s.io/kubernetes/godel-pkg/scheduler/framework/preemption-plugins/searching/priorityvaluechecker"
)

func GetPreemptionRelatedPlugins(fh handle.PodFrameworkHandle) (*framework.PluginCollection, framework.PluginMap, error) {
	podLauncherChecker, _ := podlauncherchecker.NewPodLauncherChecker(nil, fh)
	preemptibilityChecker, _ := preemptibilitychecker.NewPreemptibilityChecker(nil, fh)
	newlyStartedProtectionChecker, _ := newlystartedprotectionchecker.NewNewlyStartedProtectionChecker(nil, fh)
	priorityValueChecker, _ := priorityvaluechecker.NewPriorityValueChecker(nil, fh)
	pdbChecker, _ := pdbchecker.NewPDBChecker(nil, fh)

	pluginCollections := &framework.PluginCollection{
		Searchings: []*framework.VictimSearchingPluginCollectionSpec{
			framework.NewVictimSearchingPluginCollectionSpec([]config.Plugin{{podlauncherchecker.PodLauncherCheckerName, 0}}, false, false, false),
			framework.NewVictimSearchingPluginCollectionSpec([]config.Plugin{{preemptibilitychecker.PreemptibilityCheckerName, 0}}, false, false, false),
			framework.NewVictimSearchingPluginCollectionSpec([]config.Plugin{{newlystartedprotectionchecker.NewlyStartedProtectionCheckerName, 0}}, false, false, false),
			framework.NewVictimSearchingPluginCollectionSpec([]config.Plugin{
				{priorityvaluechecker.PriorityValueCheckerName, 0},
			}, false, false, true),
			framework.NewVictimSearchingPluginCollectionSpec([]config.Plugin{{pdbchecker.PDBCheckerName, 0}}, false, false, false),
		},
	}

	pluginMap := framework.PluginMap{
		podlauncherchecker.PodLauncherCheckerName:                       podLauncherChecker,
		preemptibilitychecker.PreemptibilityCheckerName:                 preemptibilityChecker,
		newlystartedprotectionchecker.NewlyStartedProtectionCheckerName: newlyStartedProtectionChecker,
		priorityvaluechecker.PriorityValueCheckerName:                   priorityValueChecker,
		pdbchecker.PDBCheckerName:                                       pdbChecker,
	}

	return pluginCollections, pluginMap, nil
}
