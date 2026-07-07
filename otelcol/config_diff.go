// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otelcol // import "go.opentelemetry.io/collector/otelcol"

import (
	"maps"
	"slices"

	"go.opentelemetry.io/collector/component"
)

// receiversOnlyChange returns true when the only differences between old and
// new are in receiver configurations and/or the set of pure-receiver entries
// in pipeline receiver lists. Everything else (processors, exporters,
// connectors, extensions, telemetry, pipeline structure) must be identical.
//
// Component configs (telemetry, extensions, processors, exporters,
// connectors) are compared by hash rather than by value, since
// component.Config is an empty interface with no hash or fingerprint
// contract and the underlying values may be mutated by the components that
// own them; see service.HashComponentConfigs for why hashing is done by
// reflection rather than serialization. A false negative (reporting equal
// configs as different) is safe — it simply falls back to a full reload.
//
// isConnector, derived from old's connector set, reports whether a given
// component.ID refers to a connector (as opposed to a regular receiver).
// Changes to connector-as-receiver entries require a full reload.
func receiversOnlyChange(old, newCfg *configSnapshot) bool {
	// Service telemetry must be identical.
	if old.telemetryHash != newCfg.telemetryHash {
		return false
	}

	// Extensions list must be identical.
	if !slices.Equal(old.serviceExtensions, newCfg.serviceExtensions) {
		return false
	}

	// Extension configs must be identical.
	if !maps.Equal(old.extensionHashes, newCfg.extensionHashes) {
		return false
	}

	// Processor configs must be identical.
	if !maps.Equal(old.processorHashes, newCfg.processorHashes) {
		return false
	}

	// Exporter configs must be identical.
	if !maps.Equal(old.exporterHashes, newCfg.exporterHashes) {
		return false
	}

	// Connector configs must be identical.
	if !maps.Equal(old.connectorHashes, newCfg.connectorHashes) {
		return false
	}

	// Must have the same set of pipeline IDs.
	if len(old.pipelines) != len(newCfg.pipelines) {
		return false
	}
	for pid := range old.pipelines {
		if _, ok := newCfg.pipelines[pid]; !ok {
			return false
		}
	}

	isConnector := isConnectorID(old.connectorHashes)

	// Per-pipeline: processors, exporters, and connector-as-receiver entries
	// must be identical. Only pure-receiver entries may differ.
	for pid, oldPipe := range old.pipelines {
		newPipe := newCfg.pipelines[pid]

		// Processors must be identical.
		if !slices.Equal(oldPipe.Processors, newPipe.Processors) {
			return false
		}

		// Exporters must be identical.
		if !slices.Equal(oldPipe.Exporters, newPipe.Exporters) {
			return false
		}

		// For receivers, connector entries must match.
		// Extract connector-as-receiver entries from each pipeline.
		oldConnReceivers := filterIDs(oldPipe.Receivers, isConnector)
		newConnReceivers := filterIDs(newPipe.Receivers, isConnector)
		if !slices.Equal(oldConnReceivers, newConnReceivers) {
			return false
		}
	}

	// All non-receiver aspects are identical.
	return true
}

// filterIDs returns only the IDs for which the predicate returns true,
// preserving order.
func filterIDs(ids []component.ID, pred func(component.ID) bool) []component.ID {
	var out []component.ID
	for _, id := range ids {
		if pred(id) {
			out = append(out, id)
		}
	}
	return out
}

// isConnectorID returns a predicate function that checks whether a component.ID
// refers to a configured connector. This is used to distinguish connector-as-receiver
// entries from pure receivers in pipeline configs. The map's value type is
// irrelevant; only key membership is checked.
func isConnectorID[V any](connectors map[component.ID]V) func(component.ID) bool {
	return func(id component.ID) bool {
		_, ok := connectors[id]
		return ok
	}
}
