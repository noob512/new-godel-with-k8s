/*
Copyright 2021 The Kubernetes Authors.

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

package v1beta2

import (
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/kube-scheduler/config/v1beta2"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
)

var (
	// pluginArgConversionScheme is a scheme with internal and v1beta2 registered,
	// used for defaulting/converting typed PluginConfig Args.
	// Access via getPluginArgConversionScheme()
	pluginArgConversionScheme     *runtime.Scheme
	initPluginArgConversionScheme sync.Once
)

func GetPluginArgConversionScheme() *runtime.Scheme {
	initPluginArgConversionScheme.Do(func() {
		// set up the scheme used for plugin arg conversion
		pluginArgConversionScheme = runtime.NewScheme()
		utilruntime.Must(AddToScheme(pluginArgConversionScheme))
		utilruntime.Must(config.AddToScheme(pluginArgConversionScheme))
	})
	return pluginArgConversionScheme
}

func Convert_v1beta2_KubeSchedulerConfiguration_To_config_KubeSchedulerConfiguration(in *v1beta2.KubeSchedulerConfiguration, out *config.KubeSchedulerConfiguration, s conversion.Scope) error {
	if err := autoConvert_v1beta2_KubeSchedulerConfiguration_To_config_KubeSchedulerConfiguration(in, out, s); err != nil {
		return err
	}

	// 手动转换新增的备选调度和分区同步配置字段
	if in.NumBackupNodes != nil {
		out.NumBackupNodes = *in.NumBackupNodes
	}
	if in.BackupUpdateStrategy != nil {
		out.BackupUpdateStrategy = *in.BackupUpdateStrategy
	}
	if in.EnableSecondaryReserve != nil {
		out.EnableSecondaryReserve = *in.EnableSecondaryReserve
	}
	if in.SyncMode != nil {
		out.SyncMode = *in.SyncMode
	}
	if in.ScheduleStrategy != nil {
		out.ScheduleStrategy = *in.ScheduleStrategy
	}
	if in.NumPartitions != nil {
		out.NumPartitions = *in.NumPartitions
	}
	if in.SchedulerIndex != nil {
		out.SchedulerIndex = *in.SchedulerIndex
	}
	if in.SyncGapSeconds != nil {
		out.SyncGapSeconds = *in.SyncGapSeconds
	}

	return convertToInternalPluginConfigArgs(out)
}

// convertToInternalPluginConfigArgs converts PluginConfig#Args into internal
// types using a scheme, after applying defaults.
func convertToInternalPluginConfigArgs(out *config.KubeSchedulerConfiguration) error {
	scheme := GetPluginArgConversionScheme()
	for i := range out.Profiles {
		prof := &out.Profiles[i]
		for j := range prof.PluginConfig {
			args := prof.PluginConfig[j].Args
			if args == nil {
				continue
			}
			if _, isUnknown := args.(*runtime.Unknown); isUnknown {
				continue
			}
			internalArgs, err := scheme.ConvertToVersion(args, config.SchemeGroupVersion)
			if err != nil {
				return fmt.Errorf("converting .Profiles[%d].PluginConfig[%d].Args into internal type: %w", i, j, err)
			}
			prof.PluginConfig[j].Args = internalArgs
		}
	}
	return nil
}

func Convert_config_KubeSchedulerConfiguration_To_v1beta2_KubeSchedulerConfiguration(in *config.KubeSchedulerConfiguration, out *v1beta2.KubeSchedulerConfiguration, s conversion.Scope) error {
	if err := autoConvert_config_KubeSchedulerConfiguration_To_v1beta2_KubeSchedulerConfiguration(in, out, s); err != nil {
		return err
	}

	// 手动转换新增的备选调度和分区同步配置字段（内部 -> 外部）
	out.NumBackupNodes = &in.NumBackupNodes
	out.BackupUpdateStrategy = &in.BackupUpdateStrategy
	out.EnableSecondaryReserve = &in.EnableSecondaryReserve
	out.SyncMode = &in.SyncMode
	out.ScheduleStrategy = &in.ScheduleStrategy
	out.NumPartitions = &in.NumPartitions
	out.SchedulerIndex = &in.SchedulerIndex
	out.SyncGapSeconds = &in.SyncGapSeconds

	return convertToExternalPluginConfigArgs(out)
}

// convertToExternalPluginConfigArgs converts PluginConfig#Args into
// external (versioned) types using a scheme.
func convertToExternalPluginConfigArgs(out *v1beta2.KubeSchedulerConfiguration) error {
	scheme := GetPluginArgConversionScheme()
	for i := range out.Profiles {
		for j := range out.Profiles[i].PluginConfig {
			args := out.Profiles[i].PluginConfig[j].Args
			if args.Object == nil {
				continue
			}
			if _, isUnknown := args.Object.(*runtime.Unknown); isUnknown {
				continue
			}
			externalArgs, err := scheme.ConvertToVersion(args.Object, SchemeGroupVersion)
			if err != nil {
				return err
			}
			out.Profiles[i].PluginConfig[j].Args.Object = externalArgs
		}
	}
	return nil
}
