// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otelcol

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/xconfmap"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/service"
	"go.opentelemetry.io/collector/service/pipelines"
	"go.opentelemetry.io/collector/service/telemetry"
)

// passthroughConfig wraps an arbitrary decoded YAML/JSON body so real-world
// component configs (extracted from an elastic-agent diagnostics bundle) can
// be loaded without depending on their real (out-of-repo) component
// factories. It preserves the real structural complexity (nesting, field
// counts, string sizes) of the original config for benchmarking purposes.
type passthroughConfig struct {
	Raw map[string]any
}

func (c *passthroughConfig) Unmarshal(conf *confmap.Conf) error {
	return conf.Unmarshal(&c.Raw)
}

type realConfigSamples struct {
	ReceiverTemplate map[string]any   `json:"receiver_template"`
	Processors       []map[string]any `json:"processors"`
	Exporters        []map[string]any `json:"exporters"`
	Connectors       []map[string]any `json:"connectors"`
	Extensions       []map[string]any `json:"extensions"`
}

func loadRealConfigSamples(tb testing.TB) realConfigSamples {
	tb.Helper()
	data, err := os.ReadFile("testdata/benchreal/real_config_samples.json")
	require.NoError(tb, err)
	var samples realConfigSamples
	require.NoError(tb, json.Unmarshal(data, &samples))
	return samples
}

const benchStubType = "stub"

func benchStubFactories() Factories {
	stubType := component.MustNewType(benchStubType)
	newDefault := func() component.Config { return &passthroughConfig{} }

	var factories Factories
	var err error
	factories.Receivers, err = MakeFactoryMap(receiver.NewFactory(
		stubType, newDefault,
		receiver.WithTraces(func(_ context.Context, _ receiver.Settings, _ component.Config, _ consumer.Traces) (receiver.Traces, error) {
			return &nopComponent{}, nil
		}, component.StabilityLevelStable),
		receiver.WithLogs(func(_ context.Context, _ receiver.Settings, _ component.Config, _ consumer.Logs) (receiver.Logs, error) {
			return &nopComponent{}, nil
		}, component.StabilityLevelStable),
	))
	if err != nil {
		panic(err)
	}
	factories.Processors, err = MakeFactoryMap(processor.NewFactory(
		stubType, newDefault,
		processor.WithTraces(func(_ context.Context, _ processor.Settings, _ component.Config, _ consumer.Traces) (processor.Traces, error) {
			return &nopComponent{}, nil
		}, component.StabilityLevelStable),
		processor.WithLogs(func(_ context.Context, _ processor.Settings, _ component.Config, _ consumer.Logs) (processor.Logs, error) {
			return &nopComponent{}, nil
		}, component.StabilityLevelStable),
	))
	if err != nil {
		panic(err)
	}
	factories.Exporters, err = MakeFactoryMap(exporter.NewFactory(
		stubType, newDefault,
		exporter.WithTraces(func(_ context.Context, _ exporter.Settings, _ component.Config) (exporter.Traces, error) {
			return &nopComponent{}, nil
		}, component.StabilityLevelStable),
		exporter.WithLogs(func(_ context.Context, _ exporter.Settings, _ component.Config) (exporter.Logs, error) {
			return &nopComponent{}, nil
		}, component.StabilityLevelStable),
	))
	if err != nil {
		panic(err)
	}
	factories.Connectors, err = MakeFactoryMap(connector.NewFactory(
		stubType, newDefault,
		connector.WithTracesToTraces(func(_ context.Context, _ connector.Settings, _ component.Config, _ consumer.Traces) (connector.Traces, error) {
			return &nopComponent{}, nil
		}, component.StabilityLevelStable),
		connector.WithLogsToLogs(func(_ context.Context, _ connector.Settings, _ component.Config, _ consumer.Logs) (connector.Logs, error) {
			return &nopComponent{}, nil
		}, component.StabilityLevelStable),
	))
	if err != nil {
		panic(err)
	}
	factories.Extensions, err = MakeFactoryMap(extension.NewFactory(
		stubType, newDefault,
		func(_ context.Context, _ extension.Settings, _ component.Config) (extension.Extension, error) {
			return &nopComponent{}, nil
		}, component.StabilityLevelStable,
	))
	if err != nil {
		panic(err)
	}
	factories.Telemetry = telemetry.NewFactory(func() component.Config { return fakeTelemetryConfig{} })
	return factories
}

// buildRealisticConfig constructs a *Config with numReceivers receivers, each
// cloned from a real filebeatreceiver body captured in an elastic-agent
// diagnostics bundle, plus the real processors/exporters/connectors/
// extensions from that same bundle. All components use benchStubType so no
// out-of-repo factories are required, while preserving real-world size and
// nesting.
func buildRealisticConfig(samples realConfigSamples, numReceivers int) *Config {
	pipeID := pipeline.NewID(pipeline.SignalLogs)
	pipelineCfg := &pipelines.PipelineConfig{}

	receivers := make(map[component.ID]component.Config, numReceivers)
	for i := 0; i < numReceivers; i++ {
		id := component.MustNewIDWithName(benchStubType, fmt.Sprintf("receiver-%d", i))
		receivers[id] = &passthroughConfig{Raw: cloneMap(samples.ReceiverTemplate)}
		pipelineCfg.Receivers = append(pipelineCfg.Receivers, id)
	}

	processors := make(map[component.ID]component.Config, len(samples.Processors))
	for i, body := range samples.Processors {
		id := component.MustNewIDWithName(benchStubType, fmt.Sprintf("processor-%d", i))
		processors[id] = &passthroughConfig{Raw: body}
		pipelineCfg.Processors = append(pipelineCfg.Processors, id)
	}

	exporters := make(map[component.ID]component.Config, len(samples.Exporters))
	for i, body := range samples.Exporters {
		id := component.MustNewIDWithName(benchStubType, fmt.Sprintf("exporter-%d", i))
		exporters[id] = &passthroughConfig{Raw: body}
		pipelineCfg.Exporters = append(pipelineCfg.Exporters, id)
	}

	connectors := make(map[component.ID]component.Config, len(samples.Connectors))
	for i, body := range samples.Connectors {
		id := component.MustNewIDWithName(benchStubType, fmt.Sprintf("connector-%d", i))
		connectors[id] = &passthroughConfig{Raw: body}
	}

	extensions := make(map[component.ID]component.Config, len(samples.Extensions))
	var extensionIDs []component.ID
	for i, body := range samples.Extensions {
		id := component.MustNewIDWithName(benchStubType, fmt.Sprintf("extension-%d", i))
		extensions[id] = &passthroughConfig{Raw: body}
		extensionIDs = append(extensionIDs, id)
	}

	return &Config{
		Receivers:  receivers,
		Processors: processors,
		Exporters:  exporters,
		Connectors: connectors,
		Extensions: extensions,
		Service: service.Config{
			Telemetry:  fakeTelemetryConfig{},
			Extensions: extensionIDs,
			Pipelines: pipelines.Config{
				pipeID: pipelineCfg,
			},
		},
	}
}

// cloneMap does a JSON round-trip deep copy, which is good enough for
// benchmark fixture setup (not part of the measured hot path).
func cloneMap(m map[string]any) map[string]any {
	data, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		panic(err)
	}
	return out
}

// oldCopyConfig is the pre-optimization implementation of copyConfig (see
// git history), kept here only to benchmark against for comparison.
func oldCopyConfig(cfg *Config, factories Factories) (*Config, error) {
	conf := confmap.New()
	if err := conf.Marshal(cfg, xconfmap.WithUnredacted()); err != nil {
		return nil, fmt.Errorf("could not marshal configuration for copy: %w", err)
	}

	settings, err := unmarshal(conf, factories)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal configuration copy: %w", err)
	}

	return &Config{
		Receivers:  settings.Receivers.Configs(),
		Processors: settings.Processors.Configs(),
		Exporters:  settings.Exporters.Configs(),
		Connectors: settings.Connectors.Configs(),
		Extensions: settings.Extensions.Configs(),
		Service:    settings.Service,
	}, nil
}

func benchmarkReceiverCounts() []int {
	return []int{25, 80, 200}
}

// BenchmarkOldCopyConfig measures the pre-optimization full confmap
// marshal/merge/unmarshal round trip that ran on every partial-reload
// comparison, against realistic receiver configs and counts.
func BenchmarkOldCopyConfig(b *testing.B) {
	samples := loadRealConfigSamples(b)
	factories := benchStubFactories()

	for _, n := range benchmarkReceiverCounts() {
		cfg := buildRealisticConfig(samples, n)
		b.Run(fmt.Sprintf("receivers=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := oldCopyConfig(cfg, factories); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkNewConfigSnapshot measures the hash-based replacement against the
// same realistic receiver configs and counts.
func BenchmarkNewConfigSnapshot(b *testing.B) {
	samples := loadRealConfigSamples(b)

	for _, n := range benchmarkReceiverCounts() {
		cfg := buildRealisticConfig(samples, n)
		b.Run(fmt.Sprintf("receivers=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				snapshot, err := newConfigSnapshot(cfg)
				if err != nil {
					b.Fatal(err)
				}
				snapshot.receiverHashes, err = service.HashComponentConfigs(cfg.Receivers)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
