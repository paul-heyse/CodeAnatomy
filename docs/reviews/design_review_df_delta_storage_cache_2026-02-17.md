# Design Review: DF Delta Control Plane + Storage + Cache

**Date:** 2026-02-17
**Scope:** `src/datafusion_engine/delta/`, `src/storage/`, `src/datafusion_engine/cache/`
**Focus:** Boundaries (1-6), Correctness (16-18), Quality (23-24)
**Depth:** deep
**Files reviewed:** 55 (30 delta, 19 storage, 6 cache)

---

## Executive Summary

The three sub-scopes form a coherent, layered system: a Rust-backed Delta control plane exposed through typed Python adapters, a storage layer for DML and maintenance, and a Delta-backed cache ledger. Boundary discipline is largely sound — the Ports & Adapters pattern is present throughout, protocol types are explicit, and the command/query separation is respected. Three issues warrant attention. First, `DeltaDeleteRequest.predicate` is typed `str | None` with `None` meaning "delete all rows", but no Python-layer guard enforces that callers do not accidentally pass `None`; under DF52 Issue #19840 this becomes worse because filters may silently fail to reach `delete_from`. Second, the cache layer implements a bespoke Delta-backed file metadata cache (inventory + ledger + metadata snapshots) that substantially overlaps with DF52's new `FileStatisticsCache` and `DefaultListFilesCache` builtins; significant LOC reduction is possible. Third, a minor DRY violation exists: `_WriterPort` Protocol is defined identically in both `cache/inventory.py` and `cache/ledger.py`.

---

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 3 | — | low | Internal helpers prefixed `_`; `__all__` declared; Rust types opaque via Protocol |
| 2 | Separation of concerns | 2 | medium | medium | `control_plane_core.py` mixes adapter plumbing with module-level re-export wiring |
| 3 | SRP | 2 | medium | low | `delta_runtime_ops.py` handles retry, span attrs, CDF read, observability dispatch, and commit options in one module |
| 4 | High cohesion, low coupling | 2 | medium | medium | `cache/inventory.py` duplicates `_WriterPort` Protocol from `cache/ledger.py`; `delta_write.py` imports from `delta_read.py` for internal types |
| 5 | Dependency direction | 3 | — | low | Core delta types (`control_plane_types.py`) have no storage or session deps; storage depends on delta |
| 6 | Ports & Adapters | 3 | — | low | `protocols.py`, `storage/deltalake/ports.py`, `_WriterPort` all explicit; Rust bridge behind Protocol |
| 16 | Functional core, imperative shell | 2 | medium | medium | `delta_write.py:delta_delete_where` mixes retry loop, span tracking, artifact recording, and mutation dispatch in one function (80 LOC) |
| 17 | Idempotency | 2 | small | high | `DeltaDeleteRequest.predicate: str | None` — `None` means delete-all; no Python guard against accidental empty-predicate delete; DF52 Issue #19840 worsens this |
| 18 | Determinism / reproducibility | 3 | — | low | Plan fingerprints, schema identity hashes, and version pinning all present |
| 23 | Design for testability | 2 | medium | medium | `delta_delete_where` and `vacuum_delta` have dual Rust/Python-fallback code paths that are flagged `# pragma: no cover`; retry logic is untestable without mocks |
| 24 | Observability | 2 | small | low | `snapshot_datafusion_caches` in `cache/metadata_snapshots.py` silently continues on error, swallowing the error into a row without re-raising; cache snapshot SQL query errors are caught and suppressed |

---

## Detailed Findings

### Category: Boundaries

#### P2. Separation of Concerns — Alignment: 2/3

**Current state:**
`src/datafusion_engine/delta/control_plane_core.py` serves two distinct purposes: (a) it defines shared adapter helpers (`_resolve_extension_module`, `_internal_ctx`, `_cdf_options_to_ext`, `_scan_effective_payload`, `_decode_schema_ipc`) and (b) at lines 285-333 it performs module-level re-export wiring by importing from three sub-modules and binding their public names into its own namespace. This is unusual: the file is both a utility library and an aggregating facade, and these two roles can evolve independently.

**Findings:**
- `control_plane_core.py:285-333` — after defining adapter helpers, the module executes three deferred imports and re-binds ~40 names into its own namespace. A reader must understand both the helper contracts and the re-export topology to use or modify the file.
- `delta_runtime_ops.py` conflates: retry classification (`delta_retry_classification`, `delta_retry_delay`), span attribute builders (`storage_span_attributes`, `feature_control_span`), CDF provider construction (`delta_cdf_table_provider`), observability dispatch (`record_mutation_artifact`, `record_delta_feature_mutation`, `record_delta_maintenance`), and commit options normalization (`delta_commit_options`, `commit_metadata_from_properties`). These all change for different reasons.

**Suggested improvement:**
Move the re-export block in `control_plane_core.py` into the package `__init__.py` where aggregating facades belong. Split `delta_runtime_ops.py` into at least two modules: `delta_retry.py` (retry classification and delay logic) and `delta_telemetry.py` (span builders, artifact recording). The commit-options normalization belongs in `delta_write.py` since it is only consumed there.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P3. SRP — Alignment: 2/3

**Current state:**
`src/storage/deltalake/delta_runtime_ops.py` (980 LOC) changes for at least five distinct reasons: retry policy changes, observability/telemetry changes, commit metadata format changes, CDF provider API changes, and locking policy changes. The module is also a compilation boundary problem: every caller of any one of these capabilities must import the entire file.

**Findings:**
- `delta_runtime_ops.py:159-195` — retry classification and delay. Changes when error keywords or back-off policy changes.
- `delta_runtime_ops.py:292-348` — span attribute builders. Changes when OTel schema changes.
- `delta_runtime_ops.py:916-944` — `delta_cdf_table_provider`. Changes when CDF provider API changes.
- `delta_runtime_ops.py:771-808` — `record_mutation_artifact`. Changes when observability artifact schema changes.
- `delta_runtime_ops.py:811-861` — `delta_commit_options`. Changes when commit metadata format changes.

**Suggested improvement:**
Extract `delta_retry.py` (retry classification, delay, execute_delta_merge), `delta_observability_dispatch.py` (record_mutation_artifact, record_delta_feature_mutation, record_delta_maintenance), and move `delta_cdf_table_provider` into `delta_runtime_ops.py` or `delta_read.py` since it is a pure read path. This brings each module to a single reason to change.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P4. High Cohesion, Low Coupling — Alignment: 2/3

**Current state:**
Two concrete coupling problems were identified:

1. `_WriterPort` Protocol is defined identically in two sibling modules (`cache/inventory.py:91-94` and `cache/ledger.py:46-49`). Both define `def write(self, request: WriteRequest) -> WriteResult`. A change to the write protocol requires updating both.

2. `storage/deltalake/delta_write.py` imports private types from `delta_read.py` at the top level: `_normalize_commit_metadata`, `_DeltaMergeExecutionResult`, `_DeltaMergeExecutionState`. The `_DeltaMergeExecutionState` and `_DeltaMergeExecutionResult` are `TYPE_CHECKING`-only imports, but `_normalize_commit_metadata` is imported and called directly.

**Findings:**
- `cache/inventory.py:91-94` and `cache/ledger.py:46-49` — identical `_WriterPort` Protocol definitions.
- `storage/deltalake/delta_write.py:30` — `from storage.deltalake.delta_read import _normalize_commit_metadata`. A private helper from `delta_read` is used by `delta_write`, indicating the module boundary between read and write helpers is not fully resolved.

**Suggested improvement:**
Consolidate `_WriterPort` into a shared `cache/_ports.py` (or promote it to the `cache/__init__.py`) and import it in both `inventory.py` and `ledger.py`. Move `_normalize_commit_metadata` into a shared `delta_commit.py` that both `delta_read.py` and `delta_write.py` can import from without crossing module-private boundaries.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness

#### P16. Functional Core, Imperative Shell — Alignment: 2/3

**Current state:**
`storage/deltalake/delta_write.py:delta_delete_where` (lines 105-217) is an 80-LOC imperative function that mixes: locking-provider validation, append-only policy enforcement, retry loop with `time.sleep`, span attribute emission, metric extraction from response, and mutation artifact recording. There is no functional core that describes "what a delete produces"; the entire function is side-effectful orchestration.

Similarly, `storage/deltalake/delta_maintenance.py:vacuum_delta` contains a primary code path (Rust control plane) and a fallback code path (Python `DeltaTable.vacuum`) within the same function, with the fallback selected by catching broad exceptions from the primary and silently substituting.

**Findings:**
- `delta_write.py:165-191` — retry while-loop with `time.sleep` inline in the mutation function. Retry state and mutation logic are entangled.
- `delta_maintenance.py:59-108` — Rust vacuum path attempted, exception caught as `fallback_error`, Python `DeltaTable.vacuum` invoked as fallback. Both paths coexist in one function; the fallback exception is only recorded as a span attribute, not raised.
- `delta_write.py:182` — `except Exception as exc:` — overly broad catch, commented `# pragma: no cover`.

**Suggested improvement:**
Extract a pure `_delete_once(ctx, request)` function returning `Mapping[str, object]` (raises on failure, no side effects). The retry wrapper becomes a separate, generic `_retry_delta_mutation(fn, *, policy, span)` higher-order function. Telemetry and artifact recording move to a thin orchestrator. For `vacuum_delta`, make the fallback explicit: accept a `prefer_rust: bool` parameter and split the two paths into `_vacuum_via_control_plane` and `_vacuum_via_deltalake`, called from an orchestrator that logs the chosen path.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P17. Idempotency — Alignment: 2/3

**Current state:**
`DeltaDeleteRequest` (`control_plane_types.py:143-153`) declares `predicate: str | None`. Per the DF52 spec and delta-rs semantics, a `None` predicate means "delete all rows". Neither `delta_delete` in `control_plane_mutation.py` nor `delta_delete_where` in `delta_write.py` raises when `predicate` is `None`. `delta_delete_where` has `request.predicate: str | None` and passes it through to the Rust entrypoint without a guard. Additionally, DF52 Issue #19840 documents that filters may not be passed to `delete_from` under pushdown, meaning a predicate that appears to be set may silently not be applied.

Under the DF52 `TableProvider::delete_from(state, filters)` hook, empty `filters` means delete all — this is documented as a known safety footgun in the DF52 change catalog. The Python delete-where path does carry a span attribute `codeanatomy.has_filters: bool(request.predicate)` but does not refuse the call when predicate is falsy.

**Findings:**
- `control_plane_types.py:150` — `predicate: str | None` with no minimum-content constraint.
- `control_plane_mutation.py:41-56` — `delta_delete` passes `predicate=request.predicate` to Rust with no None guard.
- `delta_write.py:105-217` — `delta_delete_where` passes `request.predicate` to `DeltaDeleteRequest` without asserting it is non-None.
- `delta_write.py:135-137` — `enforce_append_only_policy` checks `operation == "delete" or updates_present` but does not check `predicate is not None`.

**Suggested improvement:**
Add an explicit precondition in `delta_delete` (in `control_plane_mutation.py`) and in `delta_delete_where` (in `delta_write.py`): if `predicate is None`, raise `ValueError("Delta delete requires an explicit predicate to prevent accidental full-table deletion. Pass predicate='1=1' to delete all rows intentionally.")`. If intentional full-table deletion is needed, callers must pass an explicit tautological predicate. This makes the footgun visible at Python-layer before hitting the Rust entrypoint. Document the DF52 Issue #19840 risk in the module docstring for `control_plane_mutation.py`.

**Effort:** small
**Risk if unaddressed:** high

---

#### P18. Determinism / Reproducibility — Alignment: 3/3

The codebase enforces determinism through plan fingerprints (`plan_fingerprint`, `plan_identity_hash`), schema identity hashes (`schema_identity_hash`), Delta version pinning (`delta_version`, `snapshot_version`), and the `SnapshotKey` type (`delta_read.py:75-81`). The `FilePruningPolicy.fingerprint()` method produces stable hashes. Cache hit resolution (`cache/registry.py:resolve_cache_hit`) validates both plan hash and snapshot version before returning a hit. No nondeterministic behavior was identified.

---

### Category: Quality

#### P23. Design for Testability — Alignment: 2/3

**Current state:**
Several key mutation paths are structurally difficult to test without live Rust extension or live Delta tables:

1. `delta_delete_where` in `delta_write.py` and `execute_delta_merge` in `delta_runtime_ops.py` have retry branches marked `# pragma: no cover - retry paths depend on delta-rs`. The retry logic (backoff, classification, max attempts) is embedded in functions that also do I/O, making unit testing impossible without mocking at the `delta_delete`/`delta_merge` boundary.

2. `vacuum_delta` in `delta_maintenance.py` has a Rust-path/Python-fallback structure where the fallback is reached via exception swallowing. Testing the fallback path requires causing the Rust path to fail, which requires either the Rust extension to be absent or patching at the `delta_vacuum` entrypoint.

3. `snapshot_datafusion_caches` in `cache/metadata_snapshots.py` calls `ctx.sql(sql)` with SQL strings that query DF52 built-in cache tables (`metadata_cache()`, `statistics_cache()`, `list_files_cache()`). The fallback to `_fallback_cache_snapshot_source` is triggered by a broad `AttributeError, RuntimeError, TypeError, ValueError` catch with `source = None`. The pure fallback path is the only testable path.

**Findings:**
- `delta_write.py:182` and `delta_runtime_ops.py:278` — `except Exception` retry branches are untestable.
- `delta_maintenance.py:83-86` — fallback path selected by catching `(ImportError, RuntimeError, TypeError, ValueError)` from Rust call.
- `metadata_snapshots.py:108-113` — SQL cache query failure silently substitutes fallback source.

**Suggested improvement:**
Extract the retry loop into a generic `retry_with_policy(fn, *, policy)` function that accepts a callable and returns `(result, attempts)`. This function can be tested with a mock callable that raises on the first N calls. For `vacuum_delta`, separate the path selection from the execution by making the Rust path an injectable dependency (or accepting a `_vacuum_fn` parameter that defaults to the Rust call). For `snapshot_datafusion_caches`, make `_fallback_cache_snapshot_source` the explicit primary path for tests by exposing it as a public function or making the SQL path injectable.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P24. Observability — Alignment: 2/3

**Current state:**
Observability is generally well-structured: `storage_span_attributes`, `stage_span`, structured OTel attributes with `codeanatomy.*` namespace, `record_mutation_artifact`, and the `delta_observability.py` Delta-specific artifact recording are all consistent patterns. However two gaps were found:

1. `cache/metadata_snapshots.py:108-113` — a `ctx.sql(sql)` call that queries built-in DF52 cache tables is wrapped in a broad `AttributeError, RuntimeError, TypeError, ValueError` catch that silently assigns `source = None` and falls back to an empty/diagnostic table. The failure is not recorded to the OTel span within that inner try block; only the outer try/except at lines 95-155 catches errors from the whole snapshot operation. SQL failures here are invisible to the observability system until the outer handler fires.

2. `delta_maintenance.py:83-86` — when the Rust vacuum path fails, the `fallback_error` is stored and later reported as a span attribute (`codeanatomy.vacuum_fallback_error`), but only as a string attribute, not as a span event with a full exception. This means the root cause is visible but not linkable to stack traces in OTel backends.

**Findings:**
- `cache/metadata_snapshots.py:108-113` — silent SQL failure suppression before span event recording.
- `delta_maintenance.py:103-105` — `span.set_attribute("codeanatomy.vacuum_fallback_error", str(fallback_error))` — exception recorded as string not as span event.

**Suggested improvement:**
In `metadata_snapshots.py`, add `span.record_exception(exc)` or `span.set_attribute("codeanatomy.cache_sql_error", str(exc))` in the inner exception handler before falling back. For `delta_maintenance.py`, use `span.record_exception(fallback_error)` (OTel API) to attach the full exception stack to the span.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Dual Rust/Python fallback paths reduce testability and blur correctness contracts

`vacuum_delta` and `ensure_cache_inventory_table` both have Rust-primary/Python-fallback structures where the fallback is selected by catching broad exception groups from the primary. This pattern appears three times across the scope. Root cause: the Rust extension is optional/degradable, so callers need a working path when `datafusion_ext` is unavailable. Affected principles: P16, P23.

Suggested approach: make the fallback selection explicit at the call site rather than inside the function body. Introduce a `ControlPlaneMode` enum (`rust | python_fallback | auto`) and route at construction time. This makes the two paths independently testable.

### Theme 2: DF52 custom cache infrastructure vs built-in caches

`cache/inventory.py`, `cache/ledger.py`, and `cache/metadata_snapshots.py` constitute a bespoke cache observation and persistence system (~900 LOC) that records metadata about cached views into Delta tables. Meanwhile `cache/metadata_snapshots.py:26-35` shows that the system already queries `metadata_cache()`, `statistics_cache()`, and `list_files_cache()` — the DF52 built-in session-scoped caches — and snapshots them to Delta. The custom inventory is a superset of what DF52's `FileStatisticsCache` and `DefaultListFilesCache` provide. Migration to DF52 built-ins would retire the inventory/ledger Delta tables for the file-statistics use case, though the plan-fingerprint and schema-hash cache hit validation logic has no DF52 equivalent. Affected principles: P2, P3, P23.

### Theme 3: `_WriterPort` and commit-metadata helpers scattered without shared home

`_WriterPort` appears in two files; `_normalize_commit_metadata` crosses a module-private boundary; `CacheCommitMetadataRequest` + `cache_commit_metadata` live in `cache/commit_metadata.py` (correctly isolated) but the pattern is not applied consistently. Affected principles: P4, P7.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P17 | Add `None`-predicate guard in `delta_delete` and `delta_delete_where` | small | Prevents accidental full-table deletion |
| 2 | P4 | Consolidate `_WriterPort` into `cache/_ports.py` | small | Eliminates knowledge duplication |
| 3 | P24 | Add `span.record_exception` in vacuum fallback and cache SQL failure paths | small | Improves OTel debuggability |
| 4 | P16 | Extract `retry_with_policy(fn, *, policy)` from `delta_delete_where` and `execute_delta_merge` | medium | Makes retry logic unit-testable |
| 5 | P2 | Move `control_plane_core.py` re-export block into `delta/__init__.py` | medium | Clarifies module purpose |

---

## Required Output Sections

### Rust Migration Candidates

| Python Module | Candidate Logic | Rust Counterpart | Migration Value |
|---|---|---|---|
| `storage/deltalake/file_pruning.py` — `select_candidate_files` (pure Python loop over partition/stats maps, lines 230-284) | Partition filter + stats filter evaluation | `delta_control_plane.rs` already calls `PruningPredicate` from DataFusion for scan-level pruning | Moderate: the Python loop is fast for small file counts; migrating to Rust via a new PyO3 entrypoint eliminates the Python serialization roundtrip for the file index table, estimated 100-200 LOC reduction in Python |
| `cache/metadata_snapshots.py` — SQL-based DF cache snapshots | Querying `metadata_cache()`, `statistics_cache()`, `list_files_cache()` | No Rust counterpart needed; DF52 `FileStatisticsCache`/`DefaultListFilesCache` are already Rust | High: the DF52 built-ins replace the need to materialize cache snapshots to Delta; ~200 LOC reduction |
| `delta_runtime_ops.py` — `delta_retry_classification` + `delta_retry_delay` | Error string classification + exponential backoff | `delta_mutations.rs` already implements retry at the Rust level for some operations | Low: keeping retry policy configurable from Python is preferable; Rust-side retry is not exposed as a configurable port |
| `control_plane_mutation.py` — msgspec encode + Rust FFI calls | Thin Python-to-Rust serialization layer | `datafusion_python/src/codeanatomy_ext/delta_mutations.rs` — PyO3 bridge | These are already the minimal Rust bridge. No further migration needed. |

### DF52 Migration Impact

**High-impact DF52 changes for this scope:**

1. **`TableProvider::delete_from(state, filters)` DML hook (DF52 Section E)**
   - Directly affects `delta_delete` (`control_plane_mutation.py:41-56`) and `delta_delete_where` (`delta_write.py:105-217`).
   - Under DF52, SQL `DELETE FROM t WHERE <expr>` will route through `TableProvider::delete_from` if the provider implements it. The Delta-rs `DeltaTableProvider` in `delta_control_plane.rs` uses `DeltaTableProvider` from `deltalake::delta_datafusion`, which may or may not implement this hook depending on the deltalake version.
   - **Known DF52 footgun (Issue #19840):** filters may not be passed to `delete_from` under certain pushdown configurations. The Python guard recommended in P17 above is the safe mitigation.
   - **Action:** Add the None-predicate guard (P17 recommendation) before DF52 adoption of the DML hook. Audit whether `DeltaTableProvider` in deltalake implements `delete_from`; if it does, the Rust-side `delta_delete_request_payload` entrypoint may become redundant.

2. **`FileStatisticsCache` and `DefaultListFilesCache` (DF52 Section C)**
   - These new session-scoped caches directly replace the purpose of `cache/metadata_snapshots.py`'s SQL queries to `metadata_cache()`, `statistics_cache()`, and `list_files_cache()`.
   - `metadata_snapshots.py:26-35` already references these table names as SQL function calls; DF52 formalizes them as introspectable objects accessible via `ctx.statistics_cache()` and `ctx.list_files_cache()`.
   - **Migration opportunity:** Replace the SQL-query-based snapshot with direct Python calls to `ctx.statistics_cache()` / `ctx.list_files_cache()`. This removes the try/except suppression of SQL failures and makes the snapshot path deterministic. Estimated LOC reduction: ~80 lines.
   - **DF52 staleness footgun:** `ListingTableProvider` now caches object-store `LIST` results for provider lifetime. If `ensure_cache_inventory_table` registers a Delta table and the table is written to during the same provider lifetime, the cache will return stale file listings unless the provider is recreated. The current `DataFusionExecutionFacade.register_dataset(..., overwrite=True)` pattern recreates the provider on each inventory write, which is the correct mitigation.

3. **`CacheAccessor` API change (DF52 Section J)**
   - `CacheAccessor.remove()` no longer requires a mutable instance. This affects any code that holds a `CacheAccessor` for manual cache invalidation. No such code was found in scope — this is a non-issue for the current codebase.

4. **Projection pushdown moved from `FileScanConfig` to `FileSource` (DF52 Section D)**
   - `delta_control_plane.rs` uses `DeltaScanConfigBuilder` which is part of the `deltalake::delta_datafusion` surface. If deltalake upgrades to DF52 internals, `DeltaScanConfigBuilder` may need a matching update. No Python-layer impact directly, but the Rust `delta_control_plane.rs` should be audited when upgrading deltalake.

### Planning-Object Consolidation

| Bespoke Code | Location | What It Does | DF Built-in Alternative | Verdict |
|---|---|---|---|---|
| `_scan_effective_payload` | `control_plane_core.py:259-282` | Extracts scan config fields from Rust response dict, decodes schema IPC | No DF built-in | Keep — this is unpacking a Rust FFI response, not reimplementing DF planning |
| `_decode_schema_ipc` | `control_plane_core.py:201-214` | Decodes Arrow schema IPC bytes using `pa.ipc.read_schema` | DF's `DFSchema` / `SchemaRef` handling is Rust-only | Keep — necessary for Arrow IPC boundary |
| Cache metadata snapshots via SQL `SELECT * FROM metadata_cache()` | `cache/metadata_snapshots.py:84` | Materializes DF session-internal cache state to Delta | DF52: `ctx.statistics_cache()`, `ctx.list_files_cache()` as Python objects | Replace SQL queries with direct Python object access; reduces ~80 LOC and removes SQL failure suppression |
| `select_candidate_files` file-index loop | `storage/deltalake/file_pruning.py:230-284` | Python loop over partition/stats index for file pruning | DF52's sort pushdown + projection pushdown to scans effectively does this via `PruningPredicate` at scan time | The Python pruning is pre-scan; it feeds file paths to `delta_provider_with_files`. No DF built-in replaces this pre-scan Python path. Keep, but consider delegating to `delta_control_plane.rs:PruningPredicate` via a new Rust entrypoint |
| Cache inventory registry + ledger tables | `cache/inventory.py`, `cache/ledger.py` | Custom Delta-backed audit log of cache hits/misses | DF52 `FileStatisticsCache` caches file metadata, not plan-level cache hit semantics | Partial: file-level statistics caching can be delegated to DF52; plan fingerprint + schema hash cache hit validation has no DF equivalent and must be retained |

---

## Recommended Action Sequence

1. **Add None-predicate guard (P17, small, high risk):** In `control_plane_mutation.py:delta_delete` and `delta_write.py:delta_delete_where`, raise `ValueError` when `predicate is None`. Add a docstring note about DF52 Issue #19840.

2. **Consolidate `_WriterPort` (P4, small):** Create `src/datafusion_engine/cache/_ports.py` exporting one `WriterPort` Protocol. Update `inventory.py` and `ledger.py` to import from it.

3. **Add `span.record_exception` in vacuum fallback and cache SQL failure (P24, small):** `delta_maintenance.py:103`, `metadata_snapshots.py:109-113`.

4. **Extract `retry_with_policy` (P16, P23, medium):** Create a standalone `retry_with_policy(fn, *, policy, span) -> tuple[T, int]` in a new `delta_retry.py`. Remove inline while loops from `delta_delete_where` and `execute_delta_merge`.

5. **Move re-export block from `control_plane_core.py` to `delta/__init__.py` (P2, medium):** `control_plane_core.py` should define adapter helpers only.

6. **Migrate cache SQL queries to DF52 direct API (DF52 opportunity, medium):** Replace `ctx.sql("SELECT * FROM metadata_cache()")` etc. in `metadata_snapshots.py` with `ctx.statistics_cache()` / `ctx.list_files_cache()` direct access once DF52 Python bindings expose these objects.

7. **Audit `DeltaTableProvider::delete_from` implementation (DF52 DML hook, medium):** When upgrading to DF52+, verify whether deltalake's `DeltaTableProvider` implements `delete_from`. If it does, the `delta_delete_request_payload` Rust entrypoint may be redundant; if it does not, document the gap.

8. **Split `delta_runtime_ops.py` (P3, large):** Extract `delta_retry.py` (retry classification, delay, `execute_delta_merge`), `delta_observability_dispatch.py` (record_mutation_artifact, record_delta_feature_mutation, record_delta_maintenance). This is a longer-horizon refactor with no correctness impact.
