// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal // import "go.opentelemetry.io/collector/confmap/internal"

import "sync/atomic"

// CopyCounters is temporary instrumentation added to quantify how many
// full-tree copy operations confmap's resolve pipeline performs per reload
// cycle, split by the exact call site that triggers each one, and how large
// each one is. Not meant to be upstreamed.
type CopyCounters struct {
	// NewFromStringMap call sites.
	ResolveNewFromExpanded atomic.Int64 // Resolver.Resolve's retMap = NewFromStringMap(cfgMap)
	AsConfCalls            atomic.Int64 // Retrieved.AsConf -> NewFromStringMap
	SubCalls               atomic.Int64 // Conf.Sub -> NewFromStringMap
	SubItems               atomic.Int64 // total leaf items across all Sub() targets

	// toStringMapWithExpand call sites (each walks+rebuilds the whole subtree
	// it's called on).
	UnmarshalCalls   atomic.Int64 // Conf.Unmarshal -> toStringMapWithExpand
	ToStringMapCalls atomic.Int64 // Conf.ToStringMap -> toStringMapWithExpand

	MergeCalls      atomic.Int64
	AllKeysCalls    atomic.Int64
	AllKeysTotalLen atomic.Int64

	// unmarshalerHookFunc's own NewFromStringMap call (decoder.go), triggered
	// once per confmap.Unmarshaler-implementing value found during a decode.
	HookNewFromStringMap atomic.Int64
}

var Counters CopyCounters

func (c *CopyCounters) Reset() {
	c.ResolveNewFromExpanded.Store(0)
	c.AsConfCalls.Store(0)
	c.SubCalls.Store(0)
	c.SubItems.Store(0)
	c.UnmarshalCalls.Store(0)
	c.ToStringMapCalls.Store(0)
	c.MergeCalls.Store(0)
	c.AllKeysCalls.Store(0)
	c.AllKeysTotalLen.Store(0)
	c.HookNewFromStringMap.Store(0)
}
