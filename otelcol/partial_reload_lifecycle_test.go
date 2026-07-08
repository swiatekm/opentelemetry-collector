// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otelcol

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/featuregate"
)

// churnSequence mirrors docs/scripts/partial-reload/07-stress-pprof.sh: start
// at base receivers, move by one per step, reversing direction at base and
// base+swing. Each entry is the set of active pool indices [0, count) for
// that step, modeling one pod being added or removed at a time.
func churnSequence(base, swing, steps int) [][]int {
	seq := make([][]int, 0, steps)
	current := base
	dir := 1
	maxCount := base + swing
	for i := 0; i < steps; i++ {
		next := current + dir
		if next > maxCount {
			dir = -1
			next = current + dir
		} else if next < base {
			dir = 1
			next = current + dir
		}
		current = next
		indices := make([]int, current)
		for j := range indices {
			indices[j] = j
		}
		seq = append(seq, indices)
	}
	return seq
}

// realConfigFull is a full realistic configuration captured from a live
// elastic-agent diagnostics bundle (edot/otel-merged.yaml at ~104 receivers),
// split into the receivers that stay constant across reloads (one per
// non-log-emitter pod on the node: kube-apiserver, coredns, the agent's own
// monitoring, etc.) and the pool of receivers that churn as log-emitter pods
// scale up and down. This is what TestPartialReloadLifecycleMemory uses in
// place of the single-template reconstruction in realConfigSamples, since
// that undercounted the real receiver count roughly 3x (see conversation:
// "the real receiver count is 104, not 80").
type realConfigFull struct {
	FixedReceivers    []map[string]any `json:"fixed_receivers"`
	ChurnReceiverPool []map[string]any `json:"churn_receiver_pool"`
	Processors        []map[string]any `json:"processors"`
	Exporters         []map[string]any `json:"exporters"`
	Connectors        []map[string]any `json:"connectors"`
	Extensions        []map[string]any `json:"extensions"`
}

func loadRealConfigFull(tb testing.TB) realConfigFull {
	tb.Helper()
	data, err := os.ReadFile("testdata/benchreal/real_config_full.json")
	require.NoError(tb, err)
	var full realConfigFull
	require.NoError(tb, json.Unmarshal(data, &full))
	return full
}

// buildRawConfigFull returns the raw (pre-unmarshal) confmap representation
// of a realistic configuration: all of samples.FixedReceivers plus the
// churnPoolIndices subset of samples.ChurnReceiverPool, alongside the real
// (fixed, never-churning) processors/exporters/connectors/extensions.
func buildRawConfigFull(samples realConfigFull, churnPoolIndices []int) map[string]any {
	receivers := map[string]any{}
	receiverIDs := make([]any, 0, len(samples.FixedReceivers)+len(churnPoolIndices))
	for i, body := range samples.FixedReceivers {
		id := fmt.Sprintf("%s/fixed-receiver-%d", benchStubType, i)
		receivers[id] = cloneMap(body)
		receiverIDs = append(receiverIDs, id)
	}
	for _, i := range churnPoolIndices {
		id := fmt.Sprintf("%s/churn-receiver-%d", benchStubType, i)
		receivers[id] = cloneMap(samples.ChurnReceiverPool[i])
		receiverIDs = append(receiverIDs, id)
	}

	processors := map[string]any{}
	processorIDs := make([]any, 0, len(samples.Processors))
	for i, body := range samples.Processors {
		id := fmt.Sprintf("%s/processor-%d", benchStubType, i)
		processors[id] = body
		processorIDs = append(processorIDs, id)
	}

	exporters := map[string]any{}
	exporterIDs := make([]any, 0, len(samples.Exporters))
	for i, body := range samples.Exporters {
		id := fmt.Sprintf("%s/exporter-%d", benchStubType, i)
		exporters[id] = body
		exporterIDs = append(exporterIDs, id)
	}

	connectors := map[string]any{}
	for i, body := range samples.Connectors {
		id := fmt.Sprintf("%s/connector-%d", benchStubType, i)
		connectors[id] = body
	}

	extensions := map[string]any{}
	extensionIDs := make([]any, 0, len(samples.Extensions))
	for i, body := range samples.Extensions {
		id := fmt.Sprintf("%s/extension-%d", benchStubType, i)
		extensions[id] = body
		extensionIDs = append(extensionIDs, id)
	}

	return map[string]any{
		"receivers":  receivers,
		"processors": processors,
		"exporters":  exporters,
		"connectors": connectors,
		"extensions": extensions,
		"service": map[string]any{
			"extensions": extensionIDs,
			"pipelines": map[string]any{
				"logs": map[string]any{
					"receivers":  receiverIDs,
					"processors": processorIDs,
					"exporters":  exporterIDs,
				},
			},
		},
	}
}

type heapSample struct {
	label      string
	allocMB    float64
	inuseMB    float64
	sysMB      float64
	totalAlloc uint64
}

func sampleHeap(label string) heapSample {
	runtime.GC()
	debug.FreeOSMemory()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return heapSample{
		label:      label,
		allocMB:    float64(m.HeapAlloc) / 1e6,
		inuseMB:    float64(m.HeapInuse) / 1e6,
		sysMB:      float64(m.HeapSys) / 1e6,
		totalAlloc: m.TotalAlloc,
	}
}

// TestPartialReloadCopyCounts quantifies, for a single receiver-only reload
// cycle at realistic scale, how many times confmap's resolve pipeline
// performs a full-tree copy-triggering operation (NewFromStringMap, which
// loads through koanf's provider and copies; Merge, which copies via koanf;
// ToStringMap, which walks and rebuilds the whole tree), how many leaf keys
// get individually processed by the env-var expansion loop (AllKeys), and
// how that maps to actual bytes allocated relative to the config's own size.
// Uses confmap.DebugCopyCounters/ResetDebugCopyCounters, temporary
// instrumentation added directly to confmap/internal for this purpose (not
// meant to be upstreamed).
func TestPartialReloadCopyCounts(t *testing.T) {
	require.NoError(t, featuregate.GlobalRegistry().Set("service.partialReload", true))
	defer func() {
		require.NoError(t, featuregate.GlobalRegistry().Set("service.partialReload", false))
	}()

	samples := loadRealConfigFull(t)
	const base = 80
	active := make([]int, base)
	for i := range active {
		active[i] = i
	}

	provider := newFakeProvider("file", func(_ context.Context, _ string, _ confmap.WatcherFunc) (*confmap.Retrieved, error) {
		return confmap.NewRetrieved(buildRawConfigFull(samples, active))
	})

	rawCfg := buildRawConfigFull(samples, active)
	cfgJSON, err := json.Marshal(rawCfg)
	require.NoError(t, err)
	totalReceivers := len(samples.FixedReceivers) + base
	totalLeaves := countRawLeaves(rawCfg)
	t.Logf("config: %d receivers, %.3f MB serialized (JSON), %d primitive leaf values",
		totalReceivers, float64(len(cfgJSON))/1e6, totalLeaves)

	col, err := NewCollector(CollectorSettings{
		BuildInfo: component.NewDefaultBuildInfo(),
		Factories: func() (Factories, error) { return benchStubFactories(), nil },
		ConfigProviderSettings: ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs:              []string{"file:cfg"},
				ProviderFactories: []confmap.ProviderFactory{provider},
			},
		},
	})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, col.setupConfigurationComponents(ctx))
	t.Cleanup(func() {
		require.NoError(t, col.service.Shutdown(ctx))
		require.NoError(t, col.configProvider.Shutdown(ctx))
	})

	// One receiver-only change: swap the last active pool receiver for the
	// next one, matching a single real pod add/remove.
	active[len(active)-1] = len(samples.ChurnReceiverPool) - 1

	confmap.ResetDebugCopyCounters()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	reloaded, err := col.tryPartialReceiverReload(ctx)
	require.NoError(t, err)
	require.True(t, reloaded, "expected a receiver-only change to take the partial reload path")

	runtime.ReadMemStats(&after)
	counts := confmap.DebugCopyCounters()

	churned := after.TotalAlloc - before.TotalAlloc
	t.Logf("--- single reload cycle, by call site ---")
	t.Logf("Resolve's NewFromStringMap(cfgMap):     %d calls", counts.ResolveNewFromExpanded)
	t.Logf("Retrieved.AsConf -> NewFromStringMap:    %d calls", counts.AsConfCalls)
	t.Logf("Conf.Sub -> NewFromStringMap:            %d calls, %d total leaf items across all subs", counts.SubCalls, counts.SubItems)
	t.Logf("Conf.Unmarshal -> toStringMapWithExpand: %d calls", counts.UnmarshalCalls)
	t.Logf("Conf.ToStringMap -> toStringMapWithExpand: %d calls", counts.ToStringMapCalls)
	t.Logf("Conf.Merge:                              %d calls", counts.MergeCalls)
	t.Logf("Conf.AllKeys:                            %d calls, %d total keys (env-expansion loop iterations)", counts.AllKeysCalls, counts.AllKeysTotalLen)
	t.Logf("unmarshalerHookFunc's own NewFromStringMap: %d calls", counts.HookNewFromStringMap)
	t.Logf("config leaves: %d", totalLeaves)
	t.Logf("Sub() alone copies the equivalent of %.1fx the full config (%d items / %d items)",
		float64(counts.SubItems)/float64(totalLeaves), counts.SubItems, totalLeaves)
	t.Logf("bytes churned this reload: %.2f MB (config is %.3f MB serialized => %.0fx amplification)",
		float64(churned)/1e6, float64(len(cfgJSON))/1e6, float64(churned)/float64(len(cfgJSON)))
}

// countRawLeaves counts primitive leaf values in a raw (pre-unmarshal)
// confmap-shaped map/slice structure, mirroring confmap/internal's own
// countLeaves used by the DebugCopyCounters instrumentation.
func countRawLeaves(v any) int {
	switch m := v.(type) {
	case map[string]any:
		n := 0
		for _, val := range m {
			n += countRawLeaves(val)
		}
		return n
	case []any:
		n := 0
		for _, val := range m {
			n += countRawLeaves(val)
		}
		return n
	default:
		return 1
	}
}

// TestPartialReloadLifecycleMemory drives a real *Collector through a long
// sequence of receiver-only reloads (matching real pod churn: one receiver
// added or removed per step, everything else constant) using realistic
// receiver/processor/exporter/extension bodies captured from an elastic-agent
// diagnostics bundle. It reports heap growth over the sequence so the
// aggregate cost of many reloads can be checked against the per-reload cost
// measured by the BenchmarkNewConfigSnapshot benchmarks, instead of relying
// on live cluster measurements that mix in unrelated data-path and
// environment noise.
//
// Run with -v to see the memory trend, and with -memprofile/-cpuprofile to
// profile the whole sequence, e.g.:
//
//	go test -run TestPartialReloadLifecycleMemory -v -memprofile=/tmp/lifecycle_mem.prof .
func TestPartialReloadLifecycleMemory(t *testing.T) {
	require.NoError(t, featuregate.GlobalRegistry().Set("service.partialReload", true))
	defer func() {
		require.NoError(t, featuregate.GlobalRegistry().Set("service.partialReload", false))
	}()

	samples := loadRealConfigFull(t)
	t.Logf("fixed receivers: %d, churn pool: %d", len(samples.FixedReceivers), len(samples.ChurnReceiverPool))

	// Matches the real BASE=80/SWING=1 cluster run that produced the diagnostics
	// bundle this fixture was extracted from (edot/otel-merged.yaml, 104
	// receivers: 15 fixed + up to 81 of the 89-pod churn pool).
	const (
		base  = 80
		swing = 1
		steps = 80
	)
	sequence := churnSequence(base, swing, steps)

	active := sequence[0][:base] // start steady at base, like the real cluster before churn begins
	provider := newFakeProvider("file", func(_ context.Context, _ string, _ confmap.WatcherFunc) (*confmap.Retrieved, error) {
		return confmap.NewRetrieved(buildRawConfigFull(samples, active))
	})

	rawCfg := buildRawConfigFull(samples, active)
	cfgJSON, err := json.Marshal(rawCfg)
	require.NoError(t, err)
	t.Logf("initial config size (JSON, %d receivers): %.3f MB", len(samples.FixedReceivers)+base, float64(len(cfgJSON))/1e6)

	col, err := NewCollector(CollectorSettings{
		BuildInfo: component.NewDefaultBuildInfo(),
		Factories: func() (Factories, error) { return benchStubFactories(), nil },
		ConfigProviderSettings: ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs:              []string{"file:cfg"},
				ProviderFactories: []confmap.ProviderFactory{provider},
			},
		},
	})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, col.setupConfigurationComponents(ctx))
	t.Cleanup(func() {
		require.NoError(t, col.service.Shutdown(ctx))
		require.NoError(t, col.configProvider.Shutdown(ctx))
	})

	start := sampleHeap("start")
	t.Logf("%-12s alloc=%.2fMB inuse=%.2fMB sys=%.2fMB", start.label, start.allocMB, start.inuseMB, start.sysMB)

	fullReloads := 0
	for i, indices := range sequence {
		active = indices
		reloaded, err := col.tryPartialReceiverReload(ctx)
		require.NoError(t, err)
		if !reloaded {
			fullReloads++
			require.NoError(t, col.reloadConfiguration(ctx))
		}

		if (i+1)%10 == 0 || i == len(sequence)-1 {
			s := sampleHeap(fmt.Sprintf("step %d", i+1))
			t.Logf("%-12s alloc=%.2fMB inuse=%.2fMB sys=%.2fMB totalAlloc=%.2fMB",
				s.label, s.allocMB, s.inuseMB, s.sysMB, float64(s.totalAlloc)/1e6)
		}
	}
	require.Zero(t, fullReloads, "no step should have required a full reload")

	end := sampleHeap("end")
	t.Logf("%-12s alloc=%.2fMB inuse=%.2fMB sys=%.2fMB totalAlloc=%.2fMB",
		end.label, end.allocMB, end.inuseMB, end.sysMB, float64(end.totalAlloc)/1e6)
	t.Logf("retained growth over %d reloads: alloc %+.2fMB inuse %+.2fMB", steps, end.allocMB-start.allocMB, end.inuseMB-start.inuseMB)
	t.Logf("total churned (allocated+freed) over %d reloads: %.2fMB (%.3fMB/reload)",
		steps, float64(end.totalAlloc-start.totalAlloc)/1e6, float64(end.totalAlloc-start.totalAlloc)/1e6/steps)

	// A cold collector started directly at the final receiver count, with no
	// reload history, isolates the cost of "running this many receivers" from
	// any cost attributable to having churned through many reloads to get
	// there. If churn leaves something behind beyond what a cold start
	// retains, retained growth above should clearly exceed this delta.
	finalActive := sequence[len(sequence)-1]
	coldProvider := newFakeProvider("file", func(_ context.Context, _ string, _ confmap.WatcherFunc) (*confmap.Retrieved, error) {
		return confmap.NewRetrieved(buildRawConfigFull(samples, finalActive))
	})
	coldCol, err := NewCollector(CollectorSettings{
		BuildInfo: component.NewDefaultBuildInfo(),
		Factories: func() (Factories, error) { return benchStubFactories(), nil },
		ConfigProviderSettings: ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs:              []string{"file:cfg"},
				ProviderFactories: []confmap.ProviderFactory{coldProvider},
			},
		},
	})
	require.NoError(t, err)
	require.NoError(t, coldCol.setupConfigurationComponents(ctx))
	coldStats := sampleHeap("cold-start")
	t.Logf("%-12s alloc=%.2fMB inuse=%.2fMB sys=%.2fMB (collector started directly with %d receivers, no churn)",
		coldStats.label, coldStats.allocMB, coldStats.inuseMB, coldStats.sysMB, len(samples.FixedReceivers)+len(finalActive))
	require.NoError(t, coldCol.service.Shutdown(ctx))
	require.NoError(t, coldCol.configProvider.Shutdown(ctx))
}
