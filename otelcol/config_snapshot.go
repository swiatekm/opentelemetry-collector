// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otelcol // import "go.opentelemetry.io/collector/otelcol"

import (
	"fmt"
	"slices"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/service"
	"go.opentelemetry.io/collector/service/pipelines"
)

// configSnapshot holds enough of a Config to compare it against a later
// configuration for partial receiver reload, without holding onto (or
// copying) the component configs themselves.
//
// Component configs (Extensions, Processors, Exporters, Connectors,
// Receivers, and Service.Telemetry) are tracked by hash rather than by
// value: they may be mutated by the running component that owns them, so a
// stored reference cannot be compared safely later, and producing an
// independent, comparison-safe copy of every one of them on every reload is
// expensive (see service.HashComponentConfigs). Receiver hashes are tracked
// separately from the rest, since UpdateReceivers already computes and
// returns them — see tryPartialReceiverReload.
//
// Everything else in Config is plain data (component.ID values and typed
// fields, never a component.Config), so it is cheap to hold an independent
// copy of directly.
type configSnapshot struct {
	telemetryHash     uint64
	serviceExtensions []component.ID
	pipelines         pipelines.Config

	extensionHashes map[component.ID]uint64
	processorHashes map[component.ID]uint64
	exporterHashes  map[component.ID]uint64
	connectorHashes map[component.ID]uint64
	receiverHashes  map[component.ID]uint64
}

// newConfigSnapshot builds a configSnapshot from cfg. It does not populate
// receiverHashes: callers set that separately, either from a direct hash of
// cfg.Receivers (on initial startup) or from UpdateReceivers's return value
// (after a partial reload), to avoid hashing the receivers twice.
func newConfigSnapshot(cfg *Config) (*configSnapshot, error) {
	telemetryHash, err := service.HashComponentConfig(cfg.Service.Telemetry)
	if err != nil {
		return nil, fmt.Errorf("could not hash telemetry config: %w", err)
	}
	extensionHashes, err := service.HashComponentConfigs(cfg.Extensions)
	if err != nil {
		return nil, fmt.Errorf("could not hash extension configs: %w", err)
	}
	processorHashes, err := service.HashComponentConfigs(cfg.Processors)
	if err != nil {
		return nil, fmt.Errorf("could not hash processor configs: %w", err)
	}
	exporterHashes, err := service.HashComponentConfigs(cfg.Exporters)
	if err != nil {
		return nil, fmt.Errorf("could not hash exporter configs: %w", err)
	}
	connectorHashes, err := service.HashComponentConfigs(cfg.Connectors)
	if err != nil {
		return nil, fmt.Errorf("could not hash connector configs: %w", err)
	}

	return &configSnapshot{
		telemetryHash:     telemetryHash,
		serviceExtensions: slices.Clone(cfg.Service.Extensions),
		pipelines:         clonePipelines(cfg.Service.Pipelines),
		extensionHashes:   extensionHashes,
		processorHashes:   processorHashes,
		exporterHashes:    exporterHashes,
		connectorHashes:   connectorHashes,
	}, nil
}

// clonePipelines returns an independent copy of cfg. PipelineConfig only
// ever holds component.ID values, never a component.Config, so a plain
// field-by-field copy (rather than a hash) is sufficient and cheap.
func clonePipelines(cfg pipelines.Config) pipelines.Config {
	cloned := make(pipelines.Config, len(cfg))
	for id, pipe := range cfg {
		cloned[id] = &pipelines.PipelineConfig{
			Receivers:  slices.Clone(pipe.Receivers),
			Processors: slices.Clone(pipe.Processors),
			Exporters:  slices.Clone(pipe.Exporters),
		}
	}
	return cloned
}
