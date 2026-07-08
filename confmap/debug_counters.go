// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package confmap // import "go.opentelemetry.io/collector/confmap"

import "go.opentelemetry.io/collector/confmap/internal"

// DebugCopyCounts is temporary instrumentation for quantifying how many
// full-tree copy operations confmap's resolve pipeline performs per reload
// cycle, split by the exact call site that triggers each one. Not meant to
// be upstreamed.
type DebugCopyCounts struct {
	// NewFromStringMap call sites.
	ResolveNewFromExpanded int64 // Resolver.Resolve's retMap = NewFromStringMap(cfgMap)
	AsConfCalls            int64 // Retrieved.AsConf -> NewFromStringMap
	SubCalls               int64 // Conf.Sub -> NewFromStringMap
	SubItems               int64 // total leaf items across all Sub() targets

	// toStringMapWithExpand call sites (each walks+rebuilds the whole subtree
	// it's called on).
	UnmarshalCalls   int64 // Conf.Unmarshal -> toStringMapWithExpand
	ToStringMapCalls int64 // Conf.ToStringMap -> toStringMapWithExpand

	MergeCalls      int64
	AllKeysCalls    int64
	AllKeysTotalLen int64

	HookNewFromStringMap int64
}

// DebugCopyCounters returns a snapshot of the current counters.
func DebugCopyCounters() DebugCopyCounts {
	return DebugCopyCounts{
		ResolveNewFromExpanded: internal.Counters.ResolveNewFromExpanded.Load(),
		AsConfCalls:            internal.Counters.AsConfCalls.Load(),
		SubCalls:               internal.Counters.SubCalls.Load(),
		SubItems:               internal.Counters.SubItems.Load(),
		UnmarshalCalls:         internal.Counters.UnmarshalCalls.Load(),
		ToStringMapCalls:       internal.Counters.ToStringMapCalls.Load(),
		MergeCalls:             internal.Counters.MergeCalls.Load(),
		AllKeysCalls:           internal.Counters.AllKeysCalls.Load(),
		AllKeysTotalLen:        internal.Counters.AllKeysTotalLen.Load(),
		HookNewFromStringMap:   internal.Counters.HookNewFromStringMap.Load(),
	}
}

// ResetDebugCopyCounters zeroes all counters.
func ResetDebugCopyCounters() {
	internal.Counters.Reset()
}
