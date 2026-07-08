# Partial receiver reload: memory/CPU investigation

This documents an investigation into resource usage of the `service.partialReload` /
`service.partialReloadReceivers` feature (this fork's addition, see commit history on
`partial-receiver-reload`), prompted by higher-than-expected CPU/memory in an
elastic-agent deployment doing frequent partial reloads under pod churn. It covers
the original fix, a deeper investigation into confmap's own resolve pipeline, two
additional fixes found there, and what's left unfixed with an assessment of effort
and risk. Later readers of this branch's history should start here.

## Background: what `tryPartialReceiverReload` used to cost

`otelcol.Collector.tryPartialReceiverReload` originally called `copyConfig` on every
reload check (partial or full) to get an independently-copied snapshot of the whole
configuration for safe comparison against the previous one. `copyConfig` did a full
`confmap.Marshal` → `Merge` → `Unmarshal` round trip, and the `Merge` step went
through `mitchellh/copystructure`/`reflectwalk` — a full reflection-based deep copy
of every component's config, on every single reload, regardless of how small the
actual change was.

**Fix (commit `a34079d25`):** replaced the full copy with `newConfigSnapshot`, which
hashes each component's config via reflection (`service.HashComponentConfigs`,
`otelcol/config_snapshot.go`) instead of copying it. The plain-data parts of `Config`
(pipeline structure, extension ordering) are cheap to copy directly and still are.
Receiver-level change detection in `graph.UpdateReceivers` moved from
`reflect.DeepEqual` on retained configs to comparing hashes.

**Validated by:** `otelcol/partial_reload_bench_test.go`, using real receiver/
processor/exporter/extension bodies captured from an elastic-agent diagnostics
bundle (`otelcol/testdata/benchreal/real_config_samples.json`) rather than synthetic
data. At 80 receivers: 177.9ms / 57.8MB / 1,550,172 allocs per comparison (old) vs.
5.0ms / 1.55MB / 90,031 allocs (new) — roughly **35x** less time and memory.

## The numbers didn't add up

Live cluster measurements after the fix still showed a partial reload costing far
more than the fix alone predicted — pod memory in the hundreds of MB to >1GB range,
CPU spikes well above what "hash ~100 receivers" should cost. Two live k8s stress
tests (pre-fix vs. post-fix, same cluster, same session, same workload parameters)
showed a real but modest improvement (median CPU 218m→198m, memory 486Mi→368Mi
median) — real, but nowhere near a 35x improvement, and full of environment noise
(single-node kind cluster contention, run-to-run scheduling variance) that made
precise comparison unreliable.

This prompted a controlled, in-repo reproduction instead of more live-cluster
measurement.

## Full-lifecycle reproduction

`otelcol/partial_reload_lifecycle_test.go` (`TestPartialReloadLifecycleMemory`)
drives a real `*Collector` through 80 sequential receiver-only reloads at realistic
scale: 95 receivers (15 fixed system-pod receivers that never change + up to 81
churning ones, matching the receiver counts actually observed in a live diagnostics
capture — `otelcol/testdata/benchreal/real_config_full.json`). Stub factories
(`benchStubFactories`) avoid needing real out-of-repo Elastic component types while
preserving the real bodies' structural complexity.

**Result: no leak.** Forced-GC heap growth over 80 reloads: +2.5MB alloc / +8MB
inuse. The reload path, including the new hash-based comparison, retains nothing
across reloads.

**But real churn is high: ~86MB allocated and freed per reload**, against a ~313KB
serialized config — a **276x amplification**. `TestPartialReloadCopyCounts` isolates
a single reload cycle and reports the same number precisely (matching the 80-reload
average almost exactly, confirming it's a stable per-reload cost, not measurement
noise).

## Where the amplification comes from

### It's not one thing — it's ~13 effective full-config passes, each paying a real "reflection tax"

Measured baseline cost of touching a config's data *once*, via a completely
standard decoder (`TestPlainYAMLBaselineCost` equivalent measurements, no confmap
involved): parsing raw bytes into `map[string]any` costs ~7.7x the source size via
`encoding/json`, ~14.6x via YAML (YAML's decoder is intrinsically more expensive).
This is not confmap being wasteful — it's the inherent cost of `interface{}` boxing
and Go map overhead for a fully dynamic, schema-less representation. Confmap's own
operations (`NewFromStringMap`, `ToStringMap`) cost ~20-23x on data that's *already*
decoded, since they round-trip it through koanf's flatten/unflatten machinery again.

Dividing the observed 276x by that ~20x per-pass baseline gives **~13 effective
passes** over the same data during one reload. A differential heap profile
(snapshot immediately before/after the single reload call, diffed with
`pprof -base` to exclude setup noise) attributes them additively:

| Contributor | Share of total | Pass-equivalents |
|---|---|---|
| `confmap.Resolver.Resolve` (fetch/merge/expand) | ~43% | ~5.5 |
| `configunmarshaler.Configs[F].Unmarshal` for receivers | ~35% | ~4.4 |
| Other component kinds + `service.Config` decode | ~14% | ~1.8 |
| `graph.UpdateReceivers` (the actual receiver rebuild) | ~5% | ~0.65 |

None of this is specific to partial reload — `Resolve()` and `Configs[F].Unmarshal`
run on every `configProvider.Get()` call, partial reload or full.

### Enumerated sources of the ~13 passes

1. **`Resolve()`'s own load/merge (~3 passes):** `NewFromStringMap(cfgMap)`,
   `AsConf()`, `Merge()` each touch the whole config once, before any per-component
   work starts.
2. **Per-leaf-key env-var expansion (~2.5 passes):** `Resolve()` calls
   `expandValueRecursively` once per *flattened leaf key* (1,055 individual calls
   for a 95-receiver config), each paying its own small fixed lookup cost rather
   than walking the tree once.
3. **Per-component decode (~5.5 passes, the single largest piece):** each of the
   107 components (95 receivers + 12 other) goes through **three** separate
   full-tree operations, not one:
   - `Conf.Sub(id)` — its own `NewFromStringMap` call, extracting just that
     component's data (confirmed via `TestSubTruePenalty`: doing this as 95
     separate calls costs **2.28x** more per leaf than one call covering the same
     data — a real, measured, but modest small-batch penalty, not the "5.8x" I
     initially and incorrectly attributed to it from a mismatched comparison
     between differently-sized receivers, see git blame / conversation history for
     the correction).
   - `unmarshalerHookFunc`'s own `NewFromStringMap` (confirmed via a new counter,
     `HookNewFromStringMap`: fires ~112 times, ~once per component). Any
     component config implementing `confmap.Unmarshaler` gets its already-decoded
     `map[string]any` re-wrapped in a fresh, koanf-backed `Conf` just to satisfy
     that interface's signature — even though the data needs no further resolution
     at that point.
   - The inner `conf.Unmarshal((*plain)(c))` call that the standard
     `type plain T` idiom uses to avoid infinite recursion into a type's own
     custom `Unmarshal` method.

## Two fixes made (see individual commits for full detail)

1. **`confmap` — `sanitizeExpanded`'s wasted copy.** Called `maps.Copy(m)` (a full
   `copystructure`-based deep copy) and then unconditionally overwrote every one of
   that copy's values in the following loop. Replaced with a plain
   `make(map[string]any, len(m))`. This function alone was 37.5% of all allocations
   in a single reload before the fix. **Verified regression:** removing the copy
   silently changed nil-vs-empty-map behavior for a nil input; confmap's own
   `TestConfmapNilMerge` caught it, fixed by preserving the nil check already used
   by the sibling `[]any` case in the same function.
   **Measured effect:** 131.5MB → 86.7MB per reload (95 receivers), a 34% reduction.

2. **`otelcol` — `configunmarshaler.Configs[F]` decoding every component twice just
   to learn its ID.** `Unmarshal` called `conf.Unmarshal(&rawCfgs)` into
   `map[component.ID]map[string]any` purely to iterate over the keys — the decoded
   *values* were never read, since each component's actual config is re-fetched via
   `conf.Sub(id)` afterward (to preserve env-var-expansion metadata a generic map
   decode loses). Replaced with `componentIDs()`, which recovers the same ID set
   from `conf.AllKeys()` (already computed elsewhere) by splitting each flattened
   leaf path on its first segment — `component.ID.UnmarshalText` is pure string
   parsing, safe to call directly outside the decode pipeline.
   **Verified regression:** the naive version silently accepted two
   differently-whitespaced keys that normalize to the same ID after trimming (e.g.
   `"nop /x "` and `" nop/ x"`) instead of erroring as a duplicate; caught by the
   existing `TestUnmarshalError/duplicate` case, fixed by deduping on the parsed ID
   rather than the raw key.
   **Measured effect:** 86.16MB → 81.03MB per reload (259x amplification, down from
   276x) — modest, because the receivers section (95 of 107 components) dominates,
   but the same discovery step runs once per component *kind*, not just receivers.

Both fixes hold up under the existing `confmap` and `configunmarshaler` test suites,
plus the new lifecycle test (still zero leak, consistent per-reload churn after each
fix).

## What's left, and why it wasn't fixed here

Everything below is a real, measured cost, but each either needs a bigger
architectural change than a bug fix, or wasn't fully verified safe within the scope
of this investigation.

- **Per-component `Sub()`/`Unmarshal()` fragmentation (~2.3x penalty, the single
  largest remaining item).** Grouping components by factory type and decoding each
  group in one batched pass (most real configs have many receivers of the same
  type — e.g. `filebeatreceiver`) would eliminate most of the fragmentation
  penalty. Blocked on a real design problem: each component instance needs its own
  freshly-created default config (`factory.CreateDefaultConfig()`) before user
  config is overlaid, and defaults can't be shared across instances — that doesn't
  fit naturally into a single bulk mapstructure decode into
  `map[component.ID]*ConcreteType`. Worth a dedicated design effort, not a
  quick fix.

- **`unmarshalerHookFunc`'s extra `NewFromStringMap` per component.** Structurally
  required by the `confmap.Unmarshaler` interface signature
  (`Unmarshal(conf *confmap.Conf) error`) — the hook has an already-decoded
  `map[string]any` and must wrap it in a `*Conf` to call a custom `Unmarshal`
  method, and `confmap.Conf` has no lightweight variant that skips koanf's
  `Load()`/flatten step for data that's already fully resolved. Fixing this for
  real means adding such a variant to `confmap`'s core `Conf` type — a real API
  surface change, not a local fix.

- **Per-leaf-key env-var expansion loop (1,055 individual calls for 95
  receivers).** Restructuring `Resolver.Resolve()` to walk the tree once instead of
  doing one `UnsanitizedGet`/`expandValueRecursively` call per flattened leaf key
  would help, but touches the core resolve algorithm.

- **`Resolve()`'s `Merge()` into an always-empty `Conf`.** When there's exactly one
  config source (true for elastic-agent's single dynamic provider, and likely most
  deployments), `retMap := New(); retMap.Merge(retCfgMap)` produces the same result
  as just using `retCfgMap` directly — except `Merge`'s `isNil` semantics
  (`l.isNil = l.isNil && in.isNil`) mean the *current* behavior always forces
  `isNil = false` after merging into `New()`'s non-nil empty `Conf`, regardless of
  the source's own nil-ness. A correct fix needs to explicitly preserve that
  (`retMap = retCfgMap; retMap.isNil = false`), not just skip `Merge` naively. This
  is the one item on this list that looks genuinely small and low-risk, matching
  the pattern of the two fixes already made — it just wasn't implemented or
  benchmarked in this pass.

## Instrumentation

`confmap/internal/copycount.go` and `confmap/debug_counters.go` add call-site
counters (not byte-level, to keep overhead low) for `NewFromStringMap`, `Sub`,
`Unmarshal`, `Merge`, `AllKeys`, and the decode hook's own `NewFromStringMap`.
Explicitly marked as temporary/not for upstream. Useful for anyone continuing this
investigation; safe to delete if not needed (adds a handful of atomic increments to
hot confmap paths, no behavior change).

## Key artifacts

- `otelcol/partial_reload_bench_test.go` — old-copy vs. new-hash comparison,
  realistic data, multiple receiver counts.
- `otelcol/partial_reload_lifecycle_test.go` — full-lifecycle churn test
  (`TestPartialReloadLifecycleMemory`) and single-reload call-site breakdown
  (`TestPartialReloadCopyCounts`).
- `otelcol/testdata/benchreal/` — real receiver/processor/exporter/extension bodies
  and the fixed/churn-pool receiver split, both extracted from live elastic-agent
  diagnostics bundles.
- `confmap/internal/copycount.go`, `confmap/debug_counters.go` — the instrumentation
  counters referenced throughout.
