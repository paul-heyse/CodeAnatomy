# Design Review: src/datafusion_engine/plan/ and src/datafusion_engine/lineage/

**Date:** 2026-02-16
**Scope:** `src/datafusion_engine/plan/`, `src/datafusion_engine/lineage/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 17

Key files examined (with approximate LOC):
- `plan/bundle_artifact.py` (~2,563 LOC)
- `plan/artifact_store.py` (~1,654 LOC)
- `lineage/diagnostics.py` (~918 LOC)
- `lineage/scheduling.py` (~824 LOC)
- `lineage/reporting.py` (~570 LOC)
- `plan/signals.py` (~449 LOC)
- `plan/cache.py` (~347 LOC)
- `plan/diagnostics.py` (~293 LOC)
- `plan/result_types.py` (~475 LOC)
- `plan/perf_policy.py` (~137 LOC)
- `plan/normalization.py` (~204 LOC)
- `plan/profiler.py` (~172 LOC)
- `plan/walk.py` (~109 LOC)
- `plan/udf_analysis.py` (~155 LOC)
- `plan/contracts.py` (~19 LOC)
- `plan/__init__.py` (~5 LOC)
- `lineage/__init__.py` (~5 LOC)

## Executive Summary

The plan/lineage subsystem demonstrates strong architectural discipline in its core domain: deterministic plan fingerprinting, lineage extraction, and scheduling are well-decomposed and cleanly layered. The main structural concern is the `artifact_store.py` module (1,654 LOC), which conflates Delta persistence mechanics, table bootstrapping, determinism validation, and multiple artifact type management -- it carries at least four reasons to change. A secondary concern is duplicated coercion utilities across `signals.py`, `diagnostics.py`, and `scheduling.py`. The diagnostics layer (`lineage/diagnostics.py`) is well-designed with a clean Protocol/Recorder pattern but has accumulated view-level helper functions that belong closer to the view graph, and `DiagnosticsRecorderAdapter.__getattr__` introduces a hidden delegation pattern that bypasses the typed surface. Overall, this scope is solidly designed with a handful of targeted improvements that would meaningfully reduce coupling and improve maintainability.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `artifact_store.py` exposes table names and bootstrap mechanics as module-level constants |
| 2 | Separation of concerns | 1 | medium | medium | `artifact_store.py` mixes persistence, validation, SQL generation, and event processing |
| 3 | SRP | 1 | medium | medium | `artifact_store.py` has at least 4 reasons to change; `lineage/diagnostics.py` mixes recorder infra with view-level payload builders |
| 4 | High cohesion, low coupling | 2 | small | low | `bundle_artifact.py` has high cohesion around plan construction; `artifact_store.py` lower cohesion |
| 5 | Dependency direction | 2 | small | low | Core lineage types depend on nothing; `artifact_store.py` depends inward correctly |
| 6 | Ports & Adapters | 2 | small | low | `DiagnosticsSink` Protocol is a well-designed port; OTel adapter is cleanly separated |
| 7 | DRY (knowledge) | 1 | small | medium | `_coerce_int`/`_int_or_none`/`_float_or_none` duplicated across 4 modules |
| 8 | Design by contract | 2 | small | low | Frozen dataclasses enforce invariants; `PlanBundleOptions` validates preconditions |
| 9 | Parse, don't validate | 2 | small | low | `ScanLineage`/`LineageReport` as msgspec Structs enforce structure at boundary |
| 10 | Make illegal states unrepresentable | 2 | small | low | `ExecutionResultKind` enum is good; `ExecutionResult` allows kind/payload mismatch |
| 11 | CQS | 2 | small | low | `persist_plan_artifacts_for_views` both persists and returns rows (acceptable for persistence) |
| 12 | DI + explicit composition | 2 | small | low | `DiagnosticsRecorder` uses DI; `WritePipeline` composed with injected recorder |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; all composition-based |
| 14 | Law of Demeter | 1 | medium | medium | Deep attribute chains through `profile.policies.*`, `profile.diagnostics.*`, `profile.delta_ops.*` |
| 15 | Tell, don't ask | 2 | small | low | `_comparison_policy_for_profile` asks profile for policy then acts on it |
| 16 | Functional core | 2 | small | low | `extract_lineage` and `extract_plan_signals` are pure; persistence at edges |
| 17 | Idempotency | 2 | small | low | Append-mode persistence is safe; bootstrap has rmtree-then-create pattern |
| 18 | Determinism | 3 | - | - | Core design goal achieved: fingerprinting, identity hashes, and validation are rigorous |
| 19 | KISS | 2 | small | low | `_BundleAssemblyState` has many fields but justified by determinism requirements |
| 20 | YAGNI | 2 | small | low | `PlanWithDeltaPinsRequestV1` in `contracts.py` uses bare `Any` for all fields |
| 21 | Least astonishment | 2 | small | low | `DiagnosticsRecorderAdapter.__getattr__` delegates to sink silently |
| 22 | Public contracts | 2 | small | low | Versioned table names (v10, v2); `__all__` consistently declared |
| 23 | Testability | 2 | small | low | `InMemoryDiagnosticsSink` is excellent; `artifact_store.py` hard to test without Delta |
| 24 | Observability | 3 | - | - | Comprehensive: OTel spans, artifact specs, structured diagnostics throughout |

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Module internals are generally well-hidden behind public API functions. `plan/walk.py`, `plan/normalization.py`, and `plan/profiler.py` expose minimal, stable surfaces.

**Findings:**
- `artifact_store.py:50-58` exports table name constants (`PLAN_ARTIFACTS_TABLE_NAME`, `WRITE_ARTIFACTS_TABLE_NAME`, `PIPELINE_EVENTS_TABLE_NAME`) and directory name constants as module-level public values. These are implementation details of the storage layout that callers should not need to know.
- `artifact_store.py:60-63` computes `_DEFAULT_ARTIFACTS_ROOT` using `__file__` relative path traversal, which is fragile across packaging modes.

**Suggested improvement:**
Make the table name constants private (prefix with underscore). Where callers need to reference table names, expose them through a function or registry that can evolve without breaking callers. Move `_DEFAULT_ARTIFACTS_ROOT` resolution into a dedicated factory function that can be overridden via profile configuration (which is already partially done via `plan_artifacts_root`).

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`artifact_store.py` is the primary concern. It handles: (1) Delta table bootstrapping and schema management, (2) plan artifact row construction and serialization, (3) determinism validation via SQL queries, (4) pipeline event processing and persistence, (5) write artifact persistence, and (6) diff-gate violation detection.

**Findings:**
- `artifact_store.py:66-96` defines `DeterminismValidationResult` -- a validation concern mixed with persistence.
- `artifact_store.py:300-436` (`persist_plan_artifacts_for_views`) orchestrates table creation, policy lookup, row building, retention filtering, diff-gate checking, and persistence in a single function spanning 137 lines.
- `artifact_store.py:439-510` (`validate_plan_determinism`) implements SQL query construction and result parsing -- a query concern embedded in a persistence module.
- `artifact_store.py:857-907` (`persist_pipeline_events`) handles NDJSON decoding, event normalization, and persistence in a single flow.
- In contrast, `lineage/reporting.py` is an excellent example of separation: pure lineage extraction with zero IO concerns.

**Suggested improvement:**
Split `artifact_store.py` into at minimum three modules:
1. `plan/artifact_persistence.py` -- Delta table bootstrap, registration, and low-level write operations.
2. `plan/artifact_builder.py` -- `build_plan_artifact_row`, `_apply_plan_artifact_retention`, payload construction helpers.
3. `plan/determinism_validation.py` -- `validate_plan_determinism`, `_determinism_validation_query`, `DeterminismValidationResult`.

The pipeline events and write artifact persistence could further separate into `plan/event_persistence.py`. This would give each module one clear reason to change.

**Effort:** medium
**Risk if unaddressed:** medium -- The current 1,654 LOC module is the most likely location for merge conflicts and accidental coupling when any of its four concerns evolves independently.

---

#### P3. SRP -- Alignment: 1/3

**Current state:**
The SRP concern concentrates in two modules.

**Findings:**
- `artifact_store.py` changes for: (a) Delta table schema evolution, (b) plan artifact serialization format changes, (c) determinism validation logic changes, (d) pipeline event format changes, (e) write artifact format changes. This is at least five reasons to change.
- `lineage/diagnostics.py:746-905` contains `view_udf_parity_payload`, `view_fingerprint_payload`, and `rust_udf_snapshot_payload` -- view-level diagnostic payload builders that are not part of the core diagnostics recording infrastructure. These functions are called from `datafusion_engine/views/graph.py` and conceptually belong to the view graph layer.

**Suggested improvement:**
For `artifact_store.py`, apply the split described in P2. For `lineage/diagnostics.py`, extract the three view-level payload builder functions (`view_udf_parity_payload`, `view_fingerprint_payload`, `rust_udf_snapshot_payload`) into `views/diagnostics_payloads.py` or inline them where they are consumed. This keeps `lineage/diagnostics.py` focused on the Recorder/Sink infrastructure.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Most modules have high cohesion. `bundle_artifact.py` is tightly cohesive around plan construction. `lineage/reporting.py` is tightly cohesive around lineage extraction. `plan/signals.py` is tightly cohesive around signal derivation.

**Findings:**
- `artifact_store.py` has low cohesion: `DeterminismValidationResult`, `PipelineEventRow`, `WriteArtifactRequest`, and `PlanArtifactBuildRequest` are four unrelated data types sharing a module.
- Coupling is well-managed via `TYPE_CHECKING` imports. `plan/signals.py:12-14`, `plan/diagnostics.py:17-19`, and `plan/result_types.py:27-37` correctly use TYPE_CHECKING for type-only imports, keeping runtime coupling minimal.

**Suggested improvement:**
The split described in P2 would simultaneously improve cohesion. No additional coupling changes needed -- the TYPE_CHECKING pattern is used consistently and correctly.

**Effort:** small (addressed by P2 split)
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Dependencies flow correctly inward. Core domain types (`LineageReport`, `ScanLineage`, `PlanSignals`) have minimal dependencies. Persistence (`artifact_store.py`) depends on domain types, not the reverse.

**Findings:**
- `lineage/reporting.py:9` imports `plan/walk.py` and `udf/extension_runtime.py` -- both are infrastructure/utility concerns, not core domain, so this is acceptable.
- `plan/diagnostics.py:11-12` imports from `lineage/diagnostics.py` and `lineage/reporting.py`, creating a dependency from `plan/` to `lineage/`. This is directionally correct (plan diagnostics depends on lineage infrastructure).
- `plan/bundle_artifact.py:27` imports `plan/diagnostics.py` at the module level, which creates a coupling from the core bundle type to the diagnostics subsystem. This could be deferred to a lazy import.

**Suggested improvement:**
Move the `from datafusion_engine.plan.diagnostics import PlanPhaseDiagnostics, record_plan_phase_diagnostics` import in `bundle_artifact.py:27` into the function that uses it (`_record_plan_phase_telemetry`), matching the lazy-import pattern already used elsewhere in the module (e.g., line 770).

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The `DiagnosticsSink` Protocol in `lineage/diagnostics.py:48` is an exemplary port definition. It defines a clear contract with five methods. `InMemoryDiagnosticsSink` serves as the test adapter, and `OtelDiagnosticsSink` (lazy-imported at line 917) serves as the production adapter.

**Findings:**
- `lineage/diagnostics.py:48-97`: `DiagnosticsSink` Protocol is well-designed with clear method contracts.
- `lineage/diagnostics.py:908-918`: `otel_diagnostics_sink()` factory correctly isolates the OTel dependency behind a function boundary.
- The `DiagnosticsRecorder` class (line 267) properly wraps the sink with context enrichment, maintaining the adapter boundary.

**Suggested improvement:**
No significant changes needed. The port/adapter pattern is well-applied here.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Knowledge

#### P7. DRY (knowledge) -- Alignment: 1/3

**Current state:**
There is meaningful duplication of type coercion utilities across the scope.

**Findings:**
- `plan/diagnostics.py:266-281` defines `_coerce_int` and `_coerce_float`.
- `plan/signals.py:148-197` defines `_int_or_none` and `_float_or_none` with near-identical logic.
- `lineage/scheduling.py:448-451` defines `_metadata_int` which delegates to `coerce_int` from `utils/value_coercion.py`.
- `datafusion_engine/cache/inventory.py:309` has yet another `_coerce_int` definition.
- The `utils/value_coercion.py` module already provides `coerce_int`, which `scheduling.py` correctly uses. The other modules reinvent it.

- `artifact_store.py:1620-1631` defines `_msgpack_payload`, `_msgpack_payload_raw`, and `_msgpack_or_none` -- three serialization helpers that could be unified.

**Suggested improvement:**
Replace `_coerce_int`, `_int_or_none`, and `_coerce_float`/`_float_or_none` in `plan/diagnostics.py` and `plan/signals.py` with imports from `utils/value_coercion.py`. If the existing `coerce_int` does not return `None` for non-coercible values, extend it with a `default=None` parameter rather than duplicating.

**Effort:** small
**Risk if unaddressed:** medium -- Coercion semantics may silently diverge between modules over time.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Frozen dataclasses and msgspec Structs enforce structural invariants at construction time. `build_plan_artifact` at `bundle_artifact.py:278-335` validates preconditions explicitly (lines 304-309).

**Findings:**
- `bundle_artifact.py:304-309`: Explicit validation that `compute_substrait=True` and `session_runtime` is not None. Good precondition enforcement.
- `artifact_store.py:962-984` (`build_plan_artifact_row`): Raises `ValueError` when lineage is unavailable -- explicit postcondition.
- `plan/cache.py:28-46` (`PlanProtoCacheEntry`): Frozen struct with `cache_key()` method that derives from `plan_identity_hash` -- the invariant that key derives from identity is enforced by construction.

**Suggested improvement:**
`PlanWithDeltaPinsRequestV1` in `plan/contracts.py:10-16` uses `Any` for all fields (`view_nodes: tuple[Any, ...]`, `runtime_profile: Any | None`, `semantic_context: Any | None`). This defeats the contract purpose entirely. Replace with the actual types or at minimum use `object` instead of `Any`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Lineage extraction exemplifies this principle: `lineage/reporting.py` walks the raw plan tree once and produces structured `ScanLineage`, `JoinLineage`, and `ExprInfo` types. Downstream code operates on these typed values without re-validating.

**Findings:**
- `lineage/reporting.py:53-106`: `ScanLineage`, `JoinLineage`, `ExprInfo`, and `LineageReport` are all `StructBaseStrict` frozen types -- once constructed, they are guaranteed valid.
- `plan/signals.py:109-145` (`_extract_stats`): Parses raw `plan_details` mapping into a typed `NormalizedPlanStats` -- good parse-once pattern.
- `artifact_store.py:99-104` (`_DeterminismRow`): Validates SQL result rows into typed structs via `msgspec.convert` -- good boundary parsing.

**Suggested improvement:**
The `_event_time_unix_ms` function at `artifact_store.py:1387-1394` performs scattered validation (checking `isinstance`, `isdigit()`, fallback to `time.time()`). This could be replaced with a dedicated `EventTimestamp` type that parses once at the boundary.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
`ExecutionResultKind` enum with `require_*` accessors is a good pattern. Frozen dataclasses prevent mutation after construction.

**Findings:**
- `plan/result_types.py:54-64`: `ExecutionResult` allows `kind=DATAFRAME` with `dataframe=None`, making the kind/payload pairing unenforced at construction time. The `require_*` methods compensate at runtime.
- `plan/perf_policy.py:78-99`: `PerformancePolicy` composes `CachePolicyTier`, `StatisticsPolicy`, and `PlanBundleComparisonPolicy` as frozen defaults -- illegal combinations (e.g., negative TTL) are not prevented by the type.

**Suggested improvement:**
For `ExecutionResult`, consider a tagged union approach using separate classes per kind (e.g., `DataFrameResult`, `TableResult`) with a common base or Protocol. This would make it impossible to construct a `DATAFRAME` result without an actual DataFrame. However, this is a larger change and the current `require_*` pattern is a pragmatic alternative.

**Effort:** medium
**Risk if unaddressed:** low -- The `require_*` methods catch misuse at runtime.

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most query functions are pure. Side-effecting persistence functions both persist and return results, which is standard for persistence operations.

**Findings:**
- `lineage/reporting.py`: All functions are pure queries -- `extract_lineage`, `referenced_tables_from_plan`, `required_columns_by_table` return data without side effects.
- `plan/signals.py:337-406` (`extract_plan_signals`): Pure derivation function that returns `PlanSignals` without side effects.
- `artifact_store.py:360-436` (`persist_plan_artifacts_for_views`): Both persists rows AND may raise `RuntimeError` for diff-gate violations. The side-effect (raising) is conditional on persistence, mixing query-like validation with command behavior.

**Suggested improvement:**
Extract the diff-gate validation from `persist_plan_artifacts_for_views` into a separate `validate_diff_gates(rows, previous_by_view)` function that returns violations without raising. The caller can then decide whether to raise. This separates the validation query from the persistence command.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
`DiagnosticsRecorder` is composed with an injected `DiagnosticsSink` -- good DI pattern. `WritePipeline` in `artifact_store.py:785-799` is composed with an injected `recorder` and `runtime_profile`.

**Findings:**
- `lineage/diagnostics.py:267-303`: `DiagnosticsRecorder.__init__` accepts `DiagnosticsSink | None` via constructor injection. Clean DI.
- `artifact_store.py:784-799`: `WritePipeline` is composed in `_write_artifact_table` with injected dependencies. Dependencies are assembled at the call site, not hidden.
- `plan/cache.py:49-64`: `PlanProtoCache` uses `DiskCacheProfile` via constructor, with lazy cache initialization. The `default_diskcache_profile` factory default is a minor coupling to global state.

**Suggested improvement:**
`PlanProtoCache` and `PlanCache` both use `default_diskcache_profile()` as the default factory for `cache_profile`. This creates a hidden dependency on global state. Consider making the profile a required parameter (no default) and having the composition root supply it.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies exist in the scope. All behavior is composed via frozen dataclasses, Protocols, and function composition.

**Findings:**
No issues found. The scope consistently uses composition: `_BundleAssemblyState` composes `_PlanCoreComponents`, `_ExplainArtifacts`, `_UdfArtifacts`, etc. `DiagnosticsRecorder` composes a `DiagnosticsSink`. `PerformancePolicy` composes `CachePolicyTier`, `StatisticsPolicy`, and `PlanBundleComparisonPolicy`.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
There are pervasive deep attribute access chains through the `profile` object, particularly in `bundle_artifact.py` and `artifact_store.py`.

**Findings:**
- `bundle_artifact.py:691` accesses `session_runtime.profile.policies.sql_policy.allow_ddl` -- four levels deep.
- `bundle_artifact.py:697-698` accesses `profile.policies.schema_hardening.explain_format` -- three levels plus optional None check.
- `bundle_artifact.py:706-729` builds the `_planning_env_snapshot` with 8 separate chains like `profile.diagnostics.capture_explain`, `profile.execution.target_partitions`, `profile.features.enable_cache_manager`, `profile.policies.async_udf_timeout_ms`, etc.
- `artifact_store.py:258-264` (`_comparison_policy_for_profile`): Reaches through `profile.policies.performance_policy.comparison` -- three levels.
- `artifact_store.py:1154-1156` (`_plan_artifacts_root`): Reaches `profile.policies.plan_artifacts_root` and `profile.policies.local_filesystem_root`.
- `lineage/scheduling.py:622` accesses `runtime_profile.policies.delta_protocol_support` and `runtime_profile.policies.delta_protocol_mode`.

**Suggested improvement:**
The `DataFusionRuntimeProfile` is a "god object" intermediary that everyone reaches through. For this scope, the most impactful mitigation is to pass narrow, purpose-specific configuration to the functions that need it. For example:
- `_planning_env_snapshot` could accept a `PlanningEnvironmentConfig` dataclass instead of `SessionRuntime`.
- `_comparison_policy_for_profile` could accept `PlanBundleComparisonPolicy` directly.
- `_plan_artifacts_root` could accept an `ArtifactStorageConfig` instead of the full profile.

This is a larger cross-cutting refactor but can be done incrementally, function by function.

**Effort:** medium
**Risk if unaddressed:** medium -- Deep chains create brittle coupling to the profile's internal structure. Any restructuring of `DataFusionRuntimeProfile` ripples through all these call sites.

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Most domain types encapsulate behavior. `DataFusionPlanArtifact` has display methods. `DeterminismValidationResult` has `payload()`.

**Findings:**
- `artifact_store.py:258-264`: `_comparison_policy_for_profile` asks the profile for its policy, then uses `getattr` to check if the attribute exists. This is a "ask, then decide" pattern.
- `lineage/reporting.py:93-106`: `LineageReport` provides computed properties (`referenced_tables`, `all_required_columns`) -- good tell pattern.
- `plan/signals.py:337-406`: `extract_plan_signals` extracts all needed data from the bundle in one call, rather than having callers pick attributes individually -- a good encapsulation.

**Suggested improvement:**
Have `DataFusionRuntimeProfile` expose a `comparison_policy()` method that returns `PlanBundleComparisonPolicy` with proper defaults, rather than requiring `artifact_store.py` to navigate the policy hierarchy and handle missing attributes.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Core lineage extraction and signal derivation are pure functional. Persistence, table bootstrapping, and SQL execution are properly at the edges.

**Findings:**
- `lineage/reporting.py`: Entirely pure. `extract_lineage` takes a plan object and returns a `LineageReport`. No IO, no side effects.
- `plan/signals.py:337-406`: `extract_plan_signals` is nearly pure -- the only impurity is the lazy import at line 360, which is a structural convenience, not a semantic side effect.
- `plan/walk.py`: Pure tree traversal utility.
- `artifact_store.py`: IO-heavy (Delta writes, SQL queries), correctly positioned at the shell.
- `bundle_artifact.py:278-335`: `build_plan_artifact` is the boundary between functional plan construction and side-effectful cache storage (line 331-334). The cache write is correctly deferred to the end.

**Suggested improvement:**
No significant changes needed. The functional core / imperative shell boundary is well-drawn. Minor: `_store_plan_cache_entry` (bundle_artifact.py:338-357) could be extracted to the caller of `build_plan_artifact` to make the bundle construction purely functional, with caching as an explicit shell concern.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 2/3

**Current state:**
Plan artifact persistence uses `mode="append"`, which is idempotent for new data but can duplicate rows on retry. Table bootstrap uses `mode="overwrite"`.

**Findings:**
- `artifact_store.py:1129-1150` (`_reset_artifacts_table_path`): Uses `shutil.rmtree` followed by `mkdir`. This is destructive but intentional for schema recovery. The `reason` parameter and artifact recording make it traceable.
- `artifact_store.py:846` (`persist_plan_artifact_rows`): Uses `mode="append"` with `schema_mode="merge"`. Re-running with the same rows produces duplicates. This is acceptable for append-only audit tables.
- `plan/cache.py:88-93` (`PlanProtoCache.put`): Cache `set` is inherently idempotent -- same key/value produces the same result.

**Suggested improvement:**
For `persist_plan_artifact_rows`, consider adding a deduplication check using `plan_identity_hash` before appending, or document that duplicates are expected and handled at read time. The diff-gate mechanism partially addresses this.

**Effort:** small
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Determinism is a first-class design goal throughout this scope, and the implementation is rigorous.

**Findings:**
- `bundle_artifact.py:109-147`: `DataFusionPlanArtifact` captures `plan_fingerprint`, `plan_identity_hash`, and comprehensive `PlanArtifacts` for full reproducibility.
- `artifact_store.py:439-510`: `validate_plan_determinism` queries the artifact store to verify that the same logical plan consistently produces the same fingerprint.
- `plan/cache.py:147-191`: `PlanCacheKey` uses a `CompositeFingerprint` incorporating 10 separate hash dimensions (profile, substrait, UDF snapshot, settings, delta inputs, etc.).
- `lineage/scheduling.py:85-103`: `_scan_unit_key` produces deterministic keys from dataset name, version, protocol, and projected columns.
- `plan/signals.py:63-106`: `PlanSignals` captures deterministic plan metadata including sort keys and selectivity estimates.

No issues found. This is the strongest area of the scope.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Simplicity

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are straightforward. The complexity in `bundle_artifact.py` is justified by the determinism requirements. `artifact_store.py`'s complexity is partly structural (addressable by splitting) rather than inherent.

**Findings:**
- `bundle_artifact.py:380-414`: The `_BundleAssemblyState` dataclass has 14 fields. This is complex but justified -- it captures the complete intermediate state needed for deterministic plan assembly.
- `artifact_store.py:300-436`: `persist_plan_artifacts_for_views` is 137 lines with multiple nested concerns. Simplification via extraction would help.
- `lineage/reporting.py:363-394`: `_column_refs_from_expr_inner` uses recursive traversal with cycle detection via `seen: set[int]`. This is the simplest correct approach for potentially cyclic plan graphs.

**Suggested improvement:**
The `_BundleAssemblyState` is acceptable given determinism needs. Focus simplification efforts on `persist_plan_artifacts_for_views` by extracting the diff-gate logic and row-building loop into separate functions.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most abstractions are justified by current use cases. A few speculative elements exist.

**Findings:**
- `plan/contracts.py:10-16`: `PlanWithDeltaPinsRequestV1` uses `tuple[Any, ...]` for `view_nodes` and `Any | None` for `runtime_profile` and `semantic_context`. The `V1` suffix suggests versioning intent, but with `Any` types it provides no structural guarantees. This file is 19 lines total and may be a placeholder.
- `artifact_store.py:139-143`: `PipelineEventLine` with `payload: object` is maximally generic -- it cannot be wrong, but it also cannot validate anything.

**Suggested improvement:**
Either give `PlanWithDeltaPinsRequestV1` real types or remove it if unused. Check if this contract is imported anywhere; if not, it is dead code.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
API naming and behavior are generally predictable. One surprising pattern exists.

**Findings:**
- `lineage/diagnostics.py:581-589`: `DiagnosticsRecorderAdapter.__getattr__` delegates unknown attribute access to the underlying `sink`. This is surprising because the class appears to have a typed surface (it explicitly implements `record_artifact`, `record_event`, etc.), but any unknown attribute silently falls through to the sink. Callers cannot know which attributes are available without inspecting the sink type at runtime.
- `lineage/diagnostics.py:563-566` and `568-579`: `events_snapshot` and `artifacts_snapshot` on `DiagnosticsRecorderAdapter` use `getattr(self.sink, ...)` with a `callable` check, which is defensive but also surprising -- the `DiagnosticsSink` Protocol already requires these methods.

**Suggested improvement:**
Remove `__getattr__` from `DiagnosticsRecorderAdapter`. If specific sink methods are needed, expose them explicitly. The `getattr(self.sink, "events_snapshot", None)` pattern in `events_snapshot()` / `artifacts_snapshot()` should be replaced with a direct `self.sink.events_snapshot()` call, since `DiagnosticsSink` requires this method.

**Effort:** small
**Risk if unaddressed:** low -- But the `__getattr__` delegation can mask bugs where code accidentally accesses non-existent attributes without raising `AttributeError`.

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
Public surfaces are declared via `__all__` in every module. Table names include version suffixes (v2, v10).

**Findings:**
- All 17 files declare `__all__` explicitly.
- `artifact_store.py:50-55`: Table names include version suffixes: `datafusion_plan_artifacts_v10`, `datafusion_write_artifacts_v2`, `datafusion_pipeline_events_v2`. This is good schema versioning practice.
- `plan/contracts.py:10`: `PlanWithDeltaPinsRequestV1` has a V1 suffix, indicating versioning awareness.
- `lineage/reporting.py:78-106`: `LineageReport` uses `StructBaseStrict` which enforces exact field matching -- this is a strong public contract.

**Suggested improvement:**
No significant changes needed. The versioning discipline is already in place.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality

#### P23. Testability -- Alignment: 2/3

**Current state:**
`InMemoryDiagnosticsSink` is an excellent test double. Pure functional modules (`reporting.py`, `signals.py`, `walk.py`) are trivially testable.

**Findings:**
- `lineage/diagnostics.py:145-239`: `InMemoryDiagnosticsSink` provides `get_artifacts(name)`, `events_snapshot()`, and `artifacts_snapshot()` -- comprehensive test inspection surface.
- `lineage/reporting.py`: Entirely testable with mock plan objects.
- `plan/signals.py`: Testable with mock `DataFusionPlanArtifact` instances.
- `artifact_store.py`: Hard to test without a real Delta Lake instance. The `_write_artifact_table` function creates a `WritePipeline` internally (line 785), making it impossible to inject a test double for the write operation. The table bootstrap functions (`_bootstrap_plan_artifacts_table`) also require a real `SessionContext`.

**Suggested improvement:**
Extract the Delta write operation in `_write_artifact_table` behind a `PlanArtifactWriter` Protocol (or pass it as a callable), allowing tests to inject a no-op writer. This would make `persist_plan_artifact_rows` testable without Delta infrastructure.

**Effort:** small
**Risk if unaddressed:** low -- The current state means artifact store tests require heavyweight integration setup.

---

#### P24. Observability -- Alignment: 3/3

**Current state:**
Observability is comprehensive and well-structured throughout the scope.

**Findings:**
- `plan/diagnostics.py:89-131`: `record_plan_bundle_diagnostics` emits both artifact records and OTel span attributes.
- `plan/diagnostics.py:158-217`: `record_plan_execution_diagnostics` captures trace IDs, span IDs, execution stats, and structured diagnostics.
- `bundle_artifact.py:297-301`: `stage_span("planning.plan_bundle")` wraps the entire bundle construction with OTel tracing.
- `lineage/scheduling.py:172-179`: `stage_span("scheduling.scan_unit")` wraps scan unit planning.
- `artifact_store.py:1244-1265`: `_record_plan_artifact_summary` emits structured artifact summaries after persistence.
- `lineage/diagnostics.py:358-383`: `record_compilation` captures SQL compilation diagnostics with session context and timing.

No issues found. The observability layer is thorough, structured, and consistently applied.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

## Cross-Cutting Themes

### Theme 1: The artifact_store.py monolith

**Description:** `artifact_store.py` at 1,654 LOC is the largest module in scope and carries responsibilities spanning persistence, validation, serialization, and event processing. It is imported by only 3 external callers (`session/facade.py`, `io/write.py`, `views/graph.py`), all of which use different subsets of its functionality.

**Root cause:** Organic growth around the plan artifact persistence feature, with related concerns accreting rather than being extracted as they grew.

**Affected principles:** P2 (Separation of concerns), P3 (SRP), P4 (Cohesion), P19 (KISS), P23 (Testability).

**Suggested approach:** Split into 3-4 focused modules as described in P2. Start with the determinism validation extraction, which is the most self-contained piece.

### Theme 2: Profile object as God Object intermediary

**Description:** Deep attribute chains through `DataFusionRuntimeProfile` occur throughout `bundle_artifact.py`, `artifact_store.py`, and `lineage/scheduling.py`. Functions routinely reach 3-4 levels deep into the profile structure.

**Root cause:** `DataFusionRuntimeProfile` is a universal context carrier that aggregates policies, features, execution config, diagnostics config, and delta operations. Every function that needs any runtime configuration reaches through it.

**Affected principles:** P14 (Law of Demeter), P15 (Tell, don't ask), P23 (Testability).

**Suggested approach:** Incrementally introduce narrow configuration types. For example, functions in this scope could accept `PlanBundleComparisonPolicy`, `ArtifactStorageConfig`, or `PlanningEnvironmentConfig` directly rather than reaching through the profile. This can be done function-by-function without requiring a full profile refactor.

### Theme 3: Duplicated type coercion utilities

**Description:** `_coerce_int`/`_int_or_none`/`_float_or_none` appear with slightly varying signatures in `plan/diagnostics.py`, `plan/signals.py`, `cache/inventory.py`, and `session/runtime.py`. Only `lineage/scheduling.py` uses the canonical `utils/value_coercion.py`.

**Root cause:** Each module independently solved the "convert unknown type to int" problem without checking for existing utilities.

**Affected principles:** P7 (DRY).

**Suggested approach:** Consolidate into `utils/value_coercion.py` with `coerce_int(value, default=None)` and `coerce_float(value, default=None)` signatures. Replace local definitions in all four modules.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 DRY | Consolidate duplicated `_coerce_int`/`_int_or_none`/`_float_or_none` into `utils/value_coercion.py` | small | Eliminates 4 redundant implementations, prevents semantic drift |
| 2 | P21 Least astonishment | Remove `__getattr__` from `DiagnosticsRecorderAdapter` (lineage/diagnostics.py:581-589) | small | Eliminates hidden delegation that can mask bugs |
| 3 | P3 SRP | Extract `view_udf_parity_payload`, `view_fingerprint_payload`, `rust_udf_snapshot_payload` from `lineage/diagnostics.py:746-905` to view-layer module | small | Keeps diagnostics module focused on recorder infrastructure |
| 4 | P5 Dependency direction | Move `plan/diagnostics.py` import in `bundle_artifact.py:27` to lazy import inside `_record_plan_phase_telemetry` | small | Reduces module-load coupling for the core bundle type |
| 5 | P8 Design by contract | Replace `Any` types in `PlanWithDeltaPinsRequestV1` (`plan/contracts.py:10-16`) with concrete types | small | Gives the contract actual validation power |

## Recommended Action Sequence

1. **Consolidate coercion utilities (P7).** Replace `_coerce_int`/`_int_or_none`/`_float_or_none` in `plan/diagnostics.py` and `plan/signals.py` with `utils.value_coercion.coerce_int` and a new `coerce_float`. This is a safe, isolated change with no risk.

2. **Remove `__getattr__` from `DiagnosticsRecorderAdapter` (P21).** Replace with explicit delegation for any methods actually needed. Test that no callers rely on dynamic attribute access.

3. **Extract view-level payload builders from `lineage/diagnostics.py` (P3).** Move `view_udf_parity_payload`, `view_fingerprint_payload`, and `rust_udf_snapshot_payload` to a view-layer module. Update the 2-3 callers.

4. **Make `plan/diagnostics.py` import lazy in `bundle_artifact.py` (P5).** Move the import from line 27 into the function body at `_record_plan_phase_telemetry`.

5. **Fix `PlanWithDeltaPinsRequestV1` types (P8/P20).** Replace `Any` with concrete types or remove the file if unused.

6. **Split `artifact_store.py` into focused modules (P2/P3/P4).** This is the highest-impact structural improvement but has the most effort. Start by extracting `DeterminismValidationResult` and `validate_plan_determinism` into `plan/determinism_validation.py`. Then extract payload builder functions into `plan/artifact_builder.py`. Finally, separate pipeline events and write artifacts into `plan/event_persistence.py`.

7. **Incrementally narrow profile access (P14).** Start with `_comparison_policy_for_profile` -- change it to accept `PlanBundleComparisonPolicy` directly and have the caller extract it from the profile. Apply the same pattern to other functions that reach deep into the profile.
