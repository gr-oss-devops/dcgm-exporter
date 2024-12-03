/*
 * Copyright (c) 2021, NVIDIA CORPORATION.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dcgmexporter

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"text/template"

	"github.com/NVIDIA/go-dcgm/pkg/dcgm"
	"github.com/prometheus/exporter-toolkit/web"
	"go.opentelemetry.io/otel/metric"
)

var (
	SkipDCGMValue   = "SKIPPING DCGM VALUE"
	FailedToConvert = "ERROR - FAILED TO CONVERT TO STRING"

	nvidiaResourceName      = "nvidia.com/gpu"
	nvidiaMigResourcePrefix = "nvidia.com/mig-"
	MIG_UUID_PREFIX         = "MIG-"

	// Note standard resource attributes
	podAttribute       = "pod"
	namespaceAttribute = "namespace"
	containerAttribute = "container"

	hpcJobAttribute = "hpc_job"

	oldPodAttribute       = "pod_name"
	oldNamespaceAttribute = "pod_namespace"
	oldContainerAttribute = "container_name"

	undefinedConfigMapData = "none"
)

type Transform interface {
	Process(metrics MetricsByCounter, sysInfo SystemInfo) error
	Name() string
}

type MetricsPipeline struct {
	config *Config

	transformations      []Transform
	migMetricsFormat     *template.Template
	switchMetricsFormat  *template.Template
	linkMetricsFormat    *template.Template
	cpuMetricsFormat     *template.Template
	cpuCoreMetricsFormat *template.Template

	counters        []Counter
	gpuCollector    *DCGMCollector
	switchCollector *DCGMCollector
	linkCollector   *DCGMCollector
	cpuCollector    *DCGMCollector
	coreCollector   *DCGMCollector

	otelMeters  *OtelMeters
	gpuCounters map[string]float64
}

type OtelMeters struct {
	Gauge     map[string]metric.Float64Gauge
	Counter   map[string]metric.Int64Counter
	Histogram map[string]metric.Float64Histogram
}

type DCGMCollector struct {
	Counters                 []Counter
	DeviceFields             []dcgm.Short
	Cleanups                 []func()
	UseOldNamespace          bool
	SysInfo                  SystemInfo
	Hostname                 string
	ReplaceBlanksInModelName bool
}

type Counter struct {
	FieldID   dcgm.Short
	FieldName string
	PromType  string
	Help      string
}

type Metric struct {
	Counter Counter
	Value   string

	GPU          string
	GPUUUID      string
	GPUDevice    string
	GPUModelName string
	GPUPCIBusID  string

	UUID string

	MigProfile    string
	GPUInstanceID string
	Hostname      string

	Labels     map[string]string
	Attributes map[string]string
}

// metricFingerprint produces a string that should uniquely identify a metric
// It is in no way the most performant way to do this, but it is simple and should be sufficien.
// The idea is to identify the metric by name and labels, so it could be used when selecting a right counter
// for a metric.
func (m *Metric) metricFingerprint() string {
	var sb strings.Builder
	sb.Grow(256)
	fmt.Fprintf(&sb, "name=%s,", m.Counter.FieldName)
	fmt.Fprintf(&sb, "gpu=%s,", m.GPU)
	fmt.Fprintf(&sb, "gpu_uuid=%s,", m.GPUUUID)
	fmt.Fprintf(&sb, "gpu_device=%s,", m.GPUDevice)
	fmt.Fprintf(&sb, "gpu_model_name=%s,", m.GPUModelName)
	fmt.Fprintf(&sb, "gpu_pci_bus_id=%s,", m.GPUPCIBusID)
	fmt.Fprintf(&sb, "uuid=%s,", m.UUID)
	fmt.Fprintf(&sb, "mig_profile=%s,", m.MigProfile)
	fmt.Fprintf(&sb, "gpu_instance_id=%s,", m.GPUInstanceID)
	fmt.Fprintf(&sb, "hostname=%s,", m.Hostname)
	keys := make([]string, 0, max(len(m.Labels), len(m.Attributes)))
	for k := range m.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&sb, "%s=%s,", k, m.Labels[k])
	}
	keys = keys[:0]
	for k := range m.Attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&sb, "%s=%s,", k, m.Attributes[k])
	}
	return sb.String()
}

func (m Metric) getIDOfType(idType KubernetesGPUIDType) (string, error) {
	// For MIG devices, return the MIG profile instead of
	if m.MigProfile != "" {
		return fmt.Sprintf("%s-%s", m.GPU, m.GPUInstanceID), nil
	}
	switch idType {
	case GPUUID:
		return m.GPUUUID, nil
	case DeviceName:
		return m.GPUDevice, nil
	}
	return "", fmt.Errorf("unsupported KubernetesGPUIDType for MetricID '%s'", idType)
}

var promMetricType = map[string]bool{
	"gauge":     true,
	"counter":   true,
	"histogram": true,
	"summary":   true,
	"label":     true,
}

type MetricsServer struct {
	sync.Mutex

	server      *http.Server
	webConfig   *web.FlagConfig
	metrics     string
	metricsChan chan string
	registry    *Registry
}

type PodMapper struct {
	Config *Config
}

type PodInfo struct {
	Name      string
	Namespace string
	Container string
}

// MetricsByCounter represents a map where each Counter is associated with a slice of Metric objects
type MetricsByCounter map[Counter][]Metric

// CounterSet return
type CounterSet struct {
	DCGMCounters     []Counter
	ExporterCounters []Counter
}
