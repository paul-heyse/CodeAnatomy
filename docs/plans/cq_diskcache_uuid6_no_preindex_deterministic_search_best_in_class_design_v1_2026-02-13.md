# CQ DiskCache + UUID6 Best-in-Class Design v1 (No-Preindex, Max-Performance, Deterministic Search)

**Status:** Proposed  
**Date:** 2026-02-13  
**Scope:** `tools/cq` (search/query/run/neighborhood/caching/runtime metadata)  
**Primary objective:** Maximize search/query performance and determinism using `diskcache` + `uuid6` capabilities while preserving the hard requirement: **no pre-indexing of the codebase**.

---

## 1. Executive Conclusions

1. The current CQ cache architecture is directionally strong (workspace-scoped `FanoutCache`, deterministic key builder, fail-open behavior), but it is not yet maximizing either determinism or reuse potential.
2. The highest-value improvement is **not** global pre-indexing; it is **on-demand, content-addressed, scope-bounded caching** of scan artifacts and enriched outputs.
3. Determinism currently has avoidable weak points:
   - cache keys built from unordered inputs in some paths,
   - fixed TTL-only invalidation in some hot namespaces,
   - repeated root-wide scans in neighborhood flows.
4. `diskcache` should be used as a **hierarchical cache substrate** (snapshot keys + tag invalidation + bounded sharded store + concurrency primitives), not only as a simple key/value TTL store.
5. `uuid6`/UUIDv7 should remain for **run correlation, tracing, artifact ordering**; deterministic result identity should be content-derived (hash/structural IDs), not random UUID-derived.
6. Best-in-class outcome is a **two-tier no-preindex model**:
   - **L0 in-process ephemeral** caches for single-invocation speed,
   - **L1 diskcache content-addressed** caches for cross-invocation reuse,
   both driven by lazy snapshot fingerprints computed only when queries run.

---

## 2. Hard Constraints and Design Requirements

1. No pre-indexing: no background full-repo index build, no daemonized global index maintenance.
2. Deterministic behavior:
   - same workspace state + same query contract => same result ordering/content,
   - cache reuse/invalidation must not introduce nondeterministic drift.
3. Performance:
   - prioritize search/query/neighborhood hot paths,
   - minimize redundant file reads, redundant ast-grep scans, and redundant classification work.
4. Safety model:
   - preserve current fail-open UX under cache/LSP failures,
   - never block core search/query on cache corruption or transient IO errors.
5. Maintainability:
   - explicit, versioned cache contracts,
   - clear namespace ownership,
   - observability first-class (hit/miss/timeout/degraded paths).

---

## 3. Current-State Findings (Codebase Assessment)

### 3.1 Existing DiskCache integration points

1. **Backend and policy**
   - `tools/cq/core/cache/diskcache_backend.py`
   - `tools/cq/core/cache/policy.py`
   - `tools/cq/core/runtime/execution_policy.py`
   - Uses `FanoutCache(directory, shards, timeout, tag_index=True)` with fail-open fallback to `NoopCacheBackend`.
2. **Keying and tags**
   - `tools/cq/core/cache/key_builder.py`
   - Deterministic digest key: `cq:{namespace}:{version}:{sha256(msgspec-json(payload))}`.
   - Tag helpers exist for workspace/language and run scope.
3. **Active persistent cache namespaces**
   - `search_partition` in `tools/cq/search/smart_search.py`
   - `query_entity_scan` in `tools/cq/query/executor.py`
   - `calls_target_metadata` in `tools/cq/macros/calls_target.py`
   - `lsp_front_door` in `tools/cq/search/lsp_front_door_adapter.py`

### 3.2 Existing in-process cache layers

1. `tools/cq/search/classifier.py` has multiple per-file caches (`_sg_cache`, `_symtable_cache`, `_record_context_cache`, etc.).
2. `tools/cq/search/python_analysis_session.py` caches per-file analysis sessions keyed by content hash (64-entry process cache).
3. `tools/cq/search/tree_sitter_python.py` and `tools/cq/search/tree_sitter_rust.py` maintain parser/tree caches with hit/miss/eviction counters.
4. `tools/cq/search/smart_search.py` invokes `clear_caches()` at search context setup, limiting cross-run reuse by design.

### 3.3 High-value hot paths with residual inefficiency

1. Neighborhood paths (`run neighborhood`, `cli neighborhood`, search preview neighborhood) repeatedly do root scans via `sg_scan(paths=[root])` / `ScanSnapshot.build_from_repo(...)`.
2. Pattern query execution still re-runs ast-grep per query step even when run plans are related.
3. Repository file tabulation (`resolve_repo_context` + `build_repo_file_index` + `tabulate_files`) is rebuilt frequently across commands.
4. Current disk cache TTLs are hardcoded to `900` in several call sites even though runtime policy exists.

### 3.4 Determinism/correctness gaps

1. Some key payload extras can include order-unstable data (for example lists derived from unordered sets/frozensets).
2. Entity scan and search partition caches rely mostly on TTL rather than content/snapshot identity, so stale reads are possible within TTL windows after workspace edits.
3. `uuid7` is correctly thread-locked in `tools/cq/utils/uuid_factory.py`, but random IDs are still used in places where structural IDs would improve deterministic output comparability (`tools/cq/macros/calls.py` call IDs).

---

## 4. DiskCache Capability Review and CQ Applicability

### 4.1 Capabilities to exploit aggressively

1. **FanoutCache sharding**
   - Keep as primary L1 store for concurrent writers.
   - Tune shards by active writer concurrency profile, not CPU count.
2. **Tag-indexed group eviction (`tag_index=True`)**
   - Already enabled. Expand tag schema so targeted invalidation is cheap and reliable.
3. **Atomic primitives (`add`, `incr`, `decr`)**
   - Use for lock-free counters, cache write-attempt telemetry, run-level stats.
4. **Transactions for batch writes**
   - Use shard-level/cache-level transactions where multi-write batch persistence is needed (for large per-file fragment writes).
5. **`memoize` and `memoize_stampede`**
   - Apply selectively to deterministic, pure, expensive functions to reduce duplicate recomputation.
6. **Recipes (`Lock`, `RLock`, `BoundedSemaphore`, `barrier`)**
   - Use for strict single-flight around expensive cold computations (for specific keys only).
7. **`expire`, `evict(tag)`, `cull`, `size_limit/cull_limit/eviction_policy`**
   - Formalize operational control plane and periodic maintenance strategy.
8. **`stats()` / `volume()`**
   - Export runtime cache health and efficiency metrics.

### 4.2 Capabilities to use cautiously

1. **Global Fanout transactions**
   - Avoid as a default; they can negate sharding benefits.
2. **Best-effort silent-abort semantics under timeouts**
   - Fanout may absorb timeouts; always observe write acknowledgements on critical writes.
3. **Aggressive `check()` usage**
   - Keep out of hot paths; reserve for maintenance/debug workflows.

### 4.3 Capabilities to avoid for this objective

1. `DjangoCache` surface: out of scope for CQ CLI/runtime.
2. Disk serialization rewrites (`Disk`/`JSONDisk`) in phase 1 unless profiling proves serialization is a bottleneck.

---

## 5. UUID6 Capability Review and CQ Applicability

### 5.1 What to leverage

1. UUIDv7 time-ordering for run IDs/artifact IDs/log correlation.
2. Existing thread-safe wrapper pattern in `tools/cq/utils/uuid_factory.py` is correct and should remain.
3. Prefer stdlib `uuid.uuid7()` semantics when available; keep `uuid6` package fallback for compatibility.

### 5.2 What not to misuse

1. Do **not** use UUIDv7 for deterministic cache/result identity.
   - Deterministic identity must come from content-hash/structural signatures.
2. Do **not** use `uuid8()` in CQ runtime identity paths.
   - Semantics differ across implementations; unnecessary risk for determinism.
3. Do **not** treat UUID ordering as global total ordering across threads/processes/hosts.

### 5.3 UUID design split for CQ

1. **Trace IDs (non-deterministic, sortable):** UUIDv7 for `run_id`, artifact grouping, telemetry spans.
2. **Deterministic IDs (stable):** hash-derived IDs for entities/findings/callsites/cache fragments.

---

## 6. Target Architecture: No-Preindex, Deterministic, High-Reuse Cache Stack

### 6.1 Layered caching model

1. **L0 (in-process ephemeral):** fast local dict/LRU/session caches for one command execution.
2. **L1 (diskcache shared):** workspace-scoped `FanoutCache` for cross-command reuse.
3. **No background indexer:** all artifacts are created lazily only when a query path executes.

### 6.2 Snapshot-driven invalidation model

Introduce **Scope Snapshot Fingerprint (SSF)** as a first-class primitive:

1. SSF input = sorted tuple of `(rel_path, size, mtime_ns)` for the scope file set actually relevant to the operation.
2. SSF digest = stable hash of that tuple plus language/scope contract.
3. Cache keys include SSF for stale-proof correctness without preindexing.

Notes:
1. For paths that already enumerate files (entity query, pattern query, neighborhood), SSF cost is minimal.
2. For search candidate caches, use a two-step strategy:
   - fast workspace generation token for quick reject/accept,
   - short TTL + matched-file SSF fallback to avoid expensive full-tree fingerprinting on every request.

### 6.3 Namespace decomposition (recommended)

1. `file_inventory_v1`
   - output: filtered file list for `(scope, lang, globs, excludes)`.
2. `scope_snapshot_v1`
   - output: SSF metadata/digest.
3. `sg_scan_fragment_v1`
   - output: per-file ast-grep records keyed by `(file, content_hash, rule_set_hash, lang)`.
4. `entity_scan_aggregate_v2`
   - output: merged records for query plan from fragment keys.
5. `pattern_scan_fragment_v1`
   - output: per-file pattern match records.
6. `neighborhood_snapshot_v1`
   - output: `ScanSnapshot`-ready structural data for `(scope, lang, SSF)`.
7. `search_candidates_v2`
   - output: raw ripgrep candidates with strict key contract.
8. `search_enrichment_v2`
   - output: classified/enriched matches by anchor+content hash.
9. `lsp_front_door_v3`
   - output: positive and negative probes with short/medium TTL split.

### 6.4 Tagging scheme (for precise invalidation)

Use composable tags:

1. `ws:{workspace_hash}`
2. `lang:{python|rust}`
3. `ns:{namespace}`
4. `scope:{scope_hash}`
5. `snap:{ssf_digest}`
6. `run:{run_id}` (for ephemeral run-local cache products)

This enables targeted `evict(tag)` rather than broad cache wipes.

### 6.5 Deterministic ordering contract

1. All cached list payloads must be sorted in canonical order before persistence:
   - primarily by `(file, line, col, secondary fields)`.
2. All unordered collection inputs (`set`, `frozenset`, `dict` keys where ordering is semantically irrelevant) must be canonicalized prior to key hashing.
3. Persisted payload schemas remain msgspec-typed and versioned; decoding failures degrade gracefully but deterministically.

---

## 7. Subsystem-by-Subsystem Design Changes

### 7.1 Smart Search (`tools/cq/search/smart_search.py`)

1. Split current `search_partition` cache into two logical phases:
   - candidate cache (short TTL, mutation-aware token),
   - enrichment cache (longer TTL, anchor/file-content keyed).
2. Add canonical key extras:
   - normalize scope-relevant collections,
   - include query mode, lang scope, include/exclude filters with order-preserving semantics where order matters.
3. Persist per-anchor enrichment by `(file, line, col, match_text, file_content_hash, lang)`.
4. Keep deterministic final sort before output.

### 7.2 Entity Query (`tools/cq/query/executor.py`)

1. Replace TTL-only `query_entity_scan` keying with SSF-aware keying.
2. Fix order instability risk from `sg_record_types` by canonical sorting at key-build time.
3. Add optional fragment cache path:
   - compute per-file records once,
   - aggregate lazily for query-specific record types.
4. Use runtime policy TTL rather than hardcoded constants.

### 7.3 Pattern Query (`tools/cq/query/executor.py`, `tools/cq/run/runner.py`)

1. Add diskcache for pattern query scan results (currently missing).
2. Reuse file inventory + SSF across grouped run steps by language.
3. For multi-step run plans, share cached per-file pattern scan fragments.

### 7.4 Neighborhood (`tools/cq/run/runner.py`, `tools/cq/cli_app/commands/neighborhood.py`, `tools/cq/neighborhood/scan_snapshot.py`)

1. Introduce `neighborhood_snapshot_v1` keyed by `(workspace, lang, scope, SSF)`.
2. Eliminate repeated root-wide raw `sg_scan(paths=[root])` when snapshot available.
3. Reuse snapshot between:
   - `cq neighborhood`,
   - `run` neighborhood steps,
   - smart-search neighborhood preview.

### 7.5 LSP front-door (`tools/cq/search/lsp_front_door_adapter.py`)

1. Extend cache to include short-lived negative cache entries (`request_timeout`, `request_failed`, `no_signal`) with distinct TTLs.
2. Use stampede protection/single-flight per `(provider_root, file, line, col, symbol_hint)` to avoid concurrent duplicate probes.
3. Preserve fail-open output but attach deterministic degradation reason categories.

### 7.6 Calls metadata and IDs (`tools/cq/macros/calls_target.py`, `tools/cq/macros/calls.py`)

1. Keep cached `calls_target_metadata` namespace but include SSF signal for target file freshness.
2. Add deterministic `callsite_id` (content-derived) alongside existing UUID run-correlation ID.
3. Preserve UUIDv7 for traceability, not as semantic identity.

### 7.7 Repo index/tabulation (`tools/cq/index/files.py`)

1. Cache repo file inventory results under scope/lang/glob contract.
2. Reuse inventory artifacts across search/query/neighborhood within and across runs.
3. This remains no-preindex because artifacts are generated only on demand for observed scopes.

---

## 8. Concrete DiskCache Policy Upgrades

### 8.1 Runtime knobs to add

1. `CQ_RUNTIME_CACHE_SIZE_LIMIT_BYTES`
2. `CQ_RUNTIME_CACHE_CULL_LIMIT`
3. `CQ_RUNTIME_CACHE_EVICTION_POLICY`
4. `CQ_RUNTIME_CACHE_STATS_ENABLED`
5. Namespace-specific TTL overrides (for search/entity/pattern/lsp/neighborhood).

### 8.2 Suggested defaults

1. Keep `FanoutCache` with shard count tied to write concurrency profile.
2. Increase write timeout modestly for reliability-sensitive namespaces.
3. Use `retry=True` and inspect write acknowledgements for critical namespaces.
4. Keep `tag_index=True`.
5. Use bounded size + regular `expire`/`cull` maintenance in non-hot path.

### 8.3 Observability payload to add

1. Per-namespace hit/miss/write-fail/decode-fail counters.
2. Key build cardinality and key-size distribution.
3. Eviction counts by tag/namespace.
4. Cache timeout/abort counters.
5. Volume and cull stats at report footer / diagnostics summary.

---

## 9. Concrete UUID Policy Upgrades

### 9.1 Keep current strong behavior

1. Continue locked UUID generation wrapper (`tools/cq/utils/uuid_factory.py`) for thread safety with package fallback.
2. Continue UUIDv7 run IDs for sort-friendly artifacts.

### 9.2 Add explicit ID taxonomy in code contracts

1. `run_id` / `trace_id` => UUIDv7 (non-deterministic).
2. `stable_id` => deterministic hash/structural ID.
3. For APIs returning findings, document which IDs are stable across reruns and which are execution-scoped.

### 9.3 Stdlib parity posture

1. Prefer stdlib `uuid.uuid7()` when available.
2. Keep `uuid6` fallback for compatibility.
3. Avoid runtime ambiguity around `uuid8` semantics entirely.

---

## 10. Implementation Blueprint (Phased)

## Phase 0: Baseline + contracts

1. Add cache telemetry envelopes and namespace registries.
2. Add deterministic key canonicalization helper in `tools/cq/core/cache/key_builder.py`.
3. Add unit tests for canonicalization and stable digest outputs.

## Phase 1: Determinism hardening

1. Canonicalize unordered extras in all cache-key call sites.
2. Replace hardcoded TTL literals with policy-driven values.
3. Add deterministic sorting at cache-write boundaries for list payloads.

## Phase 2: SSF infrastructure

1. Add `tools/cq/core/cache/snapshot_fingerprint.py`.
2. Integrate SSF in entity/pattern/neighborhood cache keys.
3. Add invalidation/tagging rules using `snap:{digest}` and `scope:{hash}`.

## Phase 3: Fragmented scan caches

1. Add per-file ast-grep fragment cache namespaces.
2. Aggregate fragments for entity/pattern queries and neighborhood snapshots.
3. Wire into run-step grouped execution paths.

## Phase 4: Search + LSP specialization

1. Split smart-search candidate/enrichment caches.
2. Add negative LSP cache + single-flight guard.
3. Add run-level ephemeral tags and end-of-run optional tag eviction.

## Phase 5: UUID and stable IDs

1. Introduce stable content-derived IDs in calls/findings where needed.
2. Keep UUIDv7 as run/trace identity.
3. Add tests proving stable IDs across reruns with unchanged source.

## Phase 6: Tuning + rollout

1. Add policy env vars for size/cull/eviction/stats.
2. Stage rollout with feature flags per namespace.
3. Run benchmark and golden determinism suite before full enablement.

---

## 11. Test and Validation Plan

### 11.1 Determinism tests

1. Re-run identical query N times in unchanged repo state -> byte-identical normalized result payload.
2. Ensure stable ordering across process restarts.
3. Ensure cache-hit and cache-miss paths produce equivalent result content.

### 11.2 Freshness tests

1. Modify a file in-scope and verify SSF-triggered cache invalidation.
2. Add/remove files in scope and verify inventory/snapshot invalidation.
3. Validate no stale-while-valid violations across namespace boundaries.

### 11.3 Concurrency tests

1. Parallel identical queries across processes -> no corruption, bounded duplicate compute.
2. Timeout-injected diskcache backend -> fail-open behavior preserved.
3. LSP single-flight/negative-cache behavior under load.

### 11.4 Performance benchmarks

1. Cold run baseline vs warm run by command family:
   - `search`, `q entity`, `q pattern`, `neighborhood`, `run --steps` mixed.
2. Metrics:
   - p50/p95 latency,
   - total files read,
   - ast-grep invocations,
   - cache hit ratio per namespace,
   - CPU and IO profile deltas.

---

## 12. Risk Register and Mitigations

1. **Risk:** stale results due to incomplete invalidation.
   - Mitigation: SSF for scope-aware namespaces, short TTL where full SSF is too expensive.
2. **Risk:** excessive cache size growth.
   - Mitigation: size limits, cull policy, namespace TTL tiering, periodic maintenance.
3. **Risk:** key-cardinality explosion.
   - Mitigation: canonical key normalization, namespace-specific key budgeting, telemetry alerts.
4. **Risk:** contention from overusing locks/transactions.
   - Mitigation: shard-local batching, narrow critical sections, avoid global fanout barriers.
5. **Risk:** deterministic/stable ID confusion.
   - Mitigation: explicit run ID vs stable ID contract and schema docs.

---

## 13. Recommendations Ranked by ROI

## P0 (immediate)

1. Canonicalize cache key extras (especially unordered collections).
2. Route all TTLs through runtime policy.
3. Add SSF-based keying for entity/pattern/neighborhood namespaces.

## P1 (high impact)

1. Add fragment caches for per-file ast-grep results.
2. Split smart-search candidate vs enrichment caches.
3. Introduce LSP negative-cache + single-flight guard.

## P2 (medium)

1. Deterministic stable IDs for calls/findings where currently random UUID-only.
2. Full diskcache operational tuning knobs and stats surfacing.
3. Optional `memoize_stampede` for selected expensive pure functions.

---

## 14. Final Design Position

The best-in-class solution for CQ is **not** a global pre-indexer. It is a **lazy, deterministic, content-addressed cache graph** backed by DiskCache, with UUIDv7 reserved for traceability and stable hashes used for semantic identity.

This design keeps the architectural intent intact (on-demand analysis), materially reduces repeated compute/IO, and provides deterministic outputs with explicit freshness semantics.
