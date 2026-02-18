# Design Review: DF IO + Extensions + Catalog + Remaining

**Date:** 2026-02-17
**Scope:** `src/datafusion_engine/io/`, `src/datafusion_engine/extract/`, `src/datafusion_engine/extensions/`, `src/datafusion_engine/bootstrap/`, `src/datafusion_engine/catalog/`, `src/datafusion_engine/obs/`, `src/datafusion_engine/sql/`, `src/datafusion_engine/compile/`, `src/datafusion_engine/kernels.py`, `src/datafusion_engine/hashing.py`, `src/datafusion_engine/registry_facade.py`
**Focus:** Boundaries (1-6), Simplicity (19-22), Composition (12-15)
**Depth:** moderate
**Files reviewed:** 20 (representative selection from 281 total)

---

## Executive Summary

The scoped modules exhibit strong overall design discipline: the extensions FFI boundary (`datafusion_ext.py`) uses a clean normalization layer; the IO write pipeline is correctly decomposed across seven files; and `catalog/provider.py` implements a proper Ports and Adapters pattern for catalog registration. The three systemic issues worth addressing are: (1) `kernels.py` at 1,010 LOC is the most significant SRP violation, mixing kernel specifications, DataFrame computation helpers, and domain-specific join algorithms; (2) the extensions module has not yet adopted the DF52 `session` parameter in `__datafusion_*_provider__` method signatures, creating a forward compatibility gap; and (3) `WritePipeline` carries raw `DataFusionRuntimeProfile` state that leaks profile internals into method bodies, violating Tell-Don't-Ask. Quick wins are a `kernels.py` split and a DF52 entrypoint upgrade audit on `required_entrypoints.py`.

---

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `_normalize_ctx` hides ctx internals correctly; `WritePipeline._dataset_location_for_destination` reaches into `runtime_profile.data_sources.dataset_templates` two levels deep |
| 2 | Separation of concerns | 2 | medium | medium | `kernels.py` mixes spec types, computation helpers, and join algorithms; IO pipeline is otherwise well-separated |
| 3 | SRP | 1 | medium | medium | `kernels.py` (1,010 LOC) has at least three distinct change vectors: spec schema, kernel operations, interval-align join logic |
| 4 | High cohesion, low coupling | 2 | small | low | `catalog/introspection.py` correctly groups snapshot and cache state together; `obs/metrics_bridge.py` is appropriately thin |
| 5 | Dependency direction | 2 | small | low | `extensions/datafusion_ext.py` is the single inward-facing adapter; `obs/` correctly depends only on `lineage/` and `serde_artifact_specs` |
| 6 | Ports and Adapters | 2 | medium | medium | DF52 PyCapsule signature (`session` param) not reflected in `required_entrypoints.py`; adapter is otherwise well-structured |
| 12 | Dependency inversion + explicit composition | 2 | small | low | `RegistryFacade` uses `ProviderRegistryLike` protocol correctly; `_default_registry_adapters()` creates concrete instances inside the constructor |
| 13 | Prefer composition over inheritance | 3 | n/a | low | All providers use dataclass composition over inheritance; well aligned |
| 14 | Law of Demeter | 1 | medium | medium | `WritePipeline._dataset_location_for_destination` walks `self.runtime_profile.data_sources.dataset_templates` and `self.runtime_profile.policies.delta_store_policy`; `kernels.py` similarly accesses `runtime_profile.execution.target_partitions`, `runtime_profile.policies.join_policy`, etc. |
| 15 | Tell, don't ask | 1 | medium | medium | `WritePipeline` repeatedly interrogates `self.runtime_profile is None` and then reaches into `.policies`, `.data_sources`, `.execution` — the profile should answer questions, not expose raw fields |
| 19 | KISS | 2 | medium | low | `datafusion_ext.py` fallback+primary dual-module resolution logic is necessarily complex but could benefit from a flatter `_resolve_attr` path |
| 20 | YAGNI | 2 | small | low | `compile/options.py:DataFusionCompileOptions` has 32 fields — many flags appear plausible but some (e.g., `substrait_plan_override`, `prefer_substrait`, `record_substrait_gaps`) may be speculative if Substrait is not a live integration path |
| 21 | Least astonishment | 2 | small | low | `safe_sql` raises `PermissionError` for DML but `ValueError` for DDL — callers expecting consistent exception types will be surprised |
| 22 | Declare and version public contracts | 2 | small | low | `REQUIRED_RUNTIME_ENTRYPOINTS` is a well-declared contract but `install_planner_rules` / `install_physical_rules` are absent from the tuple despite being invoked via `datafusion_ext.py:149-156` |

---

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding — Alignment: 2/3

**Current state:**
`datafusion_ext.py` correctly hides the `datafusion._internal` / stub dual-module logic behind named public functions. All internal helpers are prefixed with `_`. The `_normalize_ctx` function (`datafusion_ext.py:76-78`) encapsulates the wrapper-to-native context unwrapping, so callers never need to know about `.ctx` attribute semantics.

A minor gap exists in `WritePipeline._dataset_location_for_destination` (`io/write_pipeline.py:161-197`) where the method accesses `profile.data_sources.dataset_templates` and `profile.data_sources.extract_output.dataset_locations` directly. If `DataFusionRuntimeProfile.data_sources` reorganizes its shape, every call site in `WritePipeline` must be updated.

**Findings:**
- `src/datafusion_engine/io/write_pipeline.py:189-196`: `candidates = dict(profile.data_sources.dataset_templates)` then `candidates.update(profile.data_sources.extract_output.dataset_locations)` — two levels of attribute traversal into `data_sources`.
- `src/datafusion_engine/io/write_pipeline.py:210`: `policy=self.runtime_profile.policies.delta_store_policy` — depends on the internal `policies` struct layout.

**Suggested improvement:**
Add a `DataFusionRuntimeProfile.dataset_candidates(destination: str) -> DatasetLocation | None` method that encapsulates the lookup across templates and extract output locations. `WritePipeline` should call that single method instead of reconstructing the candidate map inline.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns — Alignment: 2/3

**Current state:**
The IO write pipeline is well-separated: `write_pipeline.py` orchestrates, `write_delta.py` handles Delta-specific logic, `write_formats.py` handles CSV/JSON/Arrow format dispatch, `write_planning.py` owns adaptive sizing, and `write_core.py` holds shared types. This is clean.

`kernels.py` conflates three concerns in 1,010 lines: (a) kernel specification type definitions (`SortKey`, `DedupeSpec`, `IntervalAlignOptions`, `AsofJoinSpec`), (b) DataFusion computation helpers (`_df_from_table`, `_repartition_for_join`, `_existing_table_names`), and (c) domain kernel implementations (`dedupe_kernel`, `winner_select_kernel`, `interval_align_kernel`, `explode_list_kernel`). Changing a spec struct requires opening the same file as changing a join algorithm.

**Findings:**
- `src/datafusion_engine/kernels.py:40-99`: Spec types (`SortKey`, `DedupeSpec`, `IntervalAlignOptions`, `AsofJoinSpec`) intermixed with implementation code.
- `src/datafusion_engine/kernels.py:114-228`: Internal helpers (`_session_context`, `_df_from_table`, `_repartition_for_join`, `_existing_table_names`) co-located with public kernel functions.
- `src/datafusion_engine/kernels.py:506-750+`: Interval-align join logic (`_prepare_interval_tables`, `_rename_right_columns`, `_interval_join_frames`, etc.) is a distinct algorithmic concern from deduplication.

**Suggested improvement:**
Split `kernels.py` into three modules: `kernel_specs.py` (spec types only), `kernel_helpers.py` (private DataFusion context helpers), and keep `kernels.py` as the public API that imports and re-exports. Alternatively, a `kernels/` sub-package with `_specs.py`, `_helpers.py`, `dedupe.py`, `interval_align.py`. The file comment at line 1 already notes "(formerly kernel_specs.py)" suggesting a prior split was begun but not completed.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P3. SRP — Alignment: 1/3

**Current state:**
`kernels.py` at 1,010 LOC has at least three independent reasons to change:
1. The spec schema changes (e.g., adding a new `DedupeStrategy` variant).
2. A DataFusion computation helper changes (e.g., batch size logic, repartition policy).
3. The interval-align join algorithm changes (e.g., new match modes).

Any of these would require touching the same file. The comment on line 98 (`# Alias for backward compatibility — PlanSortKey = SortKey`) confirms that at least one prior merge happened, bringing in content from a separate file.

**Findings:**
- `src/datafusion_engine/kernels.py:1-1010`: Single 1,010-line module combining spec types, helpers, and three distinct kernel algorithms (dedupe, sort, explode, interval-align).
- The interval-align section alone spans approximately lines 582-870 and contains 15+ private helper functions (`_prepare_interval_tables`, `_rename_right_columns`, `_interval_output_schema`, `_interval_join_frames`, `_interval_order_exprs`, etc.).

**Suggested improvement:**
Extract `kernels/interval_align.py` as the highest-priority split (it is the largest cohesive sub-section and changes independently from deduplication logic). Keep `kernels.py` as an aggregation re-export to maintain backward compatibility.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling — Alignment: 2/3

**Current state:**
`catalog/introspection.py` correctly groups `IntrospectionSnapshot`, `IntrospectionCache`, `CacheConfigSnapshot`, `CacheStateSnapshot`, and all cache snapshot helpers in a single module. `obs/datafusion_runs.py` is tightly focused on run lifecycle: `start_run`, `finish_run`, `tracked_run`, `create_run_context`. `obs/metrics_bridge.py` delegates entirely to `obs/` and `serde_artifact_specs`, staying thin.

`catalog/introspection.py:468-498` depends on `datafusion_engine.session.runtime_config_policies.DEFAULT_DF_POLICY` for cache config defaults. This creates a coupling between the catalog introspection module and the session runtime config layer — a surprising inward dependency.

**Findings:**
- `src/datafusion_engine/catalog/introspection.py:470`: `from datafusion_engine.session.runtime_config_policies import DEFAULT_DF_POLICY` — catalog introspection importing from session runtime config is a layering surprise.

**Suggested improvement:**
Pass the default settings as a parameter to `_extract_cache_config(settings, defaults)` and have the caller (in the session layer) supply `DEFAULT_DF_POLICY.settings`. This breaks the inward dependency without adding complexity.

**Effort:** small
**Risk if unaddressed:** low

---

#### P5. Dependency direction — Alignment: 2/3

**Current state:**
`extensions/datafusion_ext.py` is correctly positioned as the outermost adapter — it imports nothing from `session/` and depends only on `required_entrypoints.py`. `obs/` depends on `lineage/` and `serde_artifact_specs` only. The kernel module appropriately depends on `datafusion_engine.io.ingest` and `datafusion_engine.arrow.*` but not on session runtime.

The exception noted under P4 (`catalog/introspection.py` depending on `session.runtime_config_policies`) is the only notable layering violation.

**Findings:**
- `src/datafusion_engine/catalog/introspection.py:470`: Catalog layer imports from session layer.

**Suggested improvement:** As above in P4 — pass defaults as a parameter.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports and Adapters — Alignment: 2/3

**Current state:**
`RegistrySchemaProvider` and `RegistryCatalogProvider` implement the DataFusion `SchemaProvider` and `CatalogProvider` port contracts cleanly. `extensions/schema_runtime.py` defines `SchemaRuntime` as a `Protocol` and wraps the Rust extension behind `load_schema_runtime()`, a textbook adapter. `extensions/datafusion_ext.py` is a dedicated adapter wrapping `datafusion._internal`.

**DF52 gap:** The DF52 upgrade guide (Section K in the reference doc) states `__datafusion_*_provider__` methods now take an additional `session` parameter. The `required_entrypoints.py` list (`src/datafusion_engine/extensions/required_entrypoints.py`) does not include `install_planner_rules` or `install_physical_rules` in the verified set (they are called directly via `_call_required` in `datafusion_ext.py:149-156` but are not in `REQUIRED_RUNTIME_ENTRYPOINTS`). When upgrading to DF52, `FFI_TableProvider::new` will require `task_ctx_provider` and PyCapsule methods will require `(py, session)`. The `_normalize_ctx` unwrapper (`datafusion_ext.py:76-78`) handles the Python-side wrapper context stripping but does not add the `session` argument expected by DF52's extended FFI signatures.

**Findings:**
- `src/datafusion_engine/extensions/required_entrypoints.py:5-20`: `install_planner_rules` and `install_physical_rules` are absent from `REQUIRED_RUNTIME_ENTRYPOINTS` despite being called as required entrypoints via `_call_required`.
- `src/datafusion_engine/extensions/datafusion_ext.py:76-78`: `_normalize_ctx` strips the Python wrapper but does not inject the `session` argument that DF52 FFI providers will require.
- `src/datafusion_engine/catalog/provider.py:76`: `getattr(dataset, "__datafusion_table_provider__", None)` — this PyCapsule attribute pattern changes in DF52 to require `(py, session)`.

**Suggested improvement:**
(a) Add `install_planner_rules` and `install_physical_rules` to `REQUIRED_RUNTIME_ENTRYPOINTS` so the startup validation catches missing entrypoints. (b) In a DF52 migration branch, update `_normalize_args` in `datafusion_ext.py` to also pass `session` as a keyword argument when DF52 is detected (via version probe). (c) Audit `catalog/provider.py:_table_from_dataset` for all `__datafusion_table_provider__` usage to ensure it is compatible with the DF52 `(py, session)` signature.

**Effort:** medium
**Risk if unaddressed:** high (blocks DF52 upgrade)

---

### Category: Composition

#### P12. Dependency inversion + explicit composition — Alignment: 2/3

**Current state:**
`RegistryFacade` (`registry_facade.py:136`) is constructed with injected dependencies (`dataset_catalog`, `provider_registry`, `udf_registry`, `view_registry`). The `ProviderRegistryLike` protocol is correctly defined for the provider dependency. `registry_facade_for_context` is the explicit composition root.

The gap: `RegistryFacade.__init__` calls `_default_registry_adapters()` at construction time (line 160), which instantiates seven concrete registry objects from `datafusion_engine.schema` and `semantics.registry` directly. These are not injectable and cannot be substituted in tests without patching module-level state.

**Findings:**
- `src/datafusion_engine/registry_facade.py:160`: `self._extra_registries = _default_registry_adapters()` — hidden construction of seven concrete registries inside `__init__`.
- `src/datafusion_engine/registry_facade.py:108-133`: `_default_registry_adapters()` imports from `datafusion_engine.schema` and `semantics.registry` — the facade's construction silently depends on the full semantic registry system.

**Suggested improvement:**
Add an `extra_registries: Mapping[str, Registry[object, object]] | None = None` parameter to `RegistryFacade.__init__`. Default to `_default_registry_adapters()` when `None`, but allow callers and tests to substitute an empty mapping or lightweight fakes without loading the full semantic registry.

**Effort:** small
**Risk if unaddressed:** low

---

#### P14. Law of Demeter — Alignment: 1/3

**Current state:**
`WritePipeline` in `io/write_pipeline.py` accesses its `runtime_profile` attribute through two and three levels of attribute traversal in multiple methods:

- Line 103: `self.runtime_profile.sql_options()` — acceptable (one level).
- Line 189: `profile.data_sources.dataset_templates` — two levels.
- Line 190: `profile.data_sources.extract_output.dataset_locations` — three levels.
- Line 209: `self.runtime_profile.policies.delta_store_policy` — two levels.

`kernels.py` similarly accesses `runtime_profile.execution.target_partitions` (line 189), `runtime_profile.policies.join_policy` (line 193), and `runtime_profile.diagnostics_sink()` (line 175) — always accessed via `self.runtime_profile` but reaching through multiple intermediate objects.

**Findings:**
- `src/datafusion_engine/io/write_pipeline.py:189-190`: Three-level chain `profile.data_sources.extract_output.dataset_locations`.
- `src/datafusion_engine/io/write_pipeline.py:209`: Two-level `self.runtime_profile.policies.delta_store_policy`.
- `src/datafusion_engine/kernels.py:189`: `runtime_profile.execution.target_partitions`.
- `src/datafusion_engine/kernels.py:193`: `runtime_profile.policies.join_policy`.

**Suggested improvement:**
On `DataFusionRuntimeProfile`, add targeted query methods: `dataset_candidates() -> Mapping[str, DatasetLocation]`, `join_repartition_policy() -> JoinPolicy | None`, `target_partitions() -> int | None`. These methods can be simple property wrappers initially but insulate callers from internal restructuring. For `WritePipeline` specifically, the `ManifestDatasetResolver` injection already exists — prefer it exclusively over `profile.data_sources.*`.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P15. Tell, don't ask — Alignment: 1/3

**Current state:**
`WritePipeline` contains a repeated pattern: check if `self.runtime_profile is None`, then interrogate internal fields, then call a function using those fields. This "ask before acting" pattern appears in:

- `_resolved_sql_options` (line 99-104): checks `self.runtime_profile is not None` then calls `self.runtime_profile.sql_options()`.
- `_dataset_location_for_destination` (lines 172-197): checks `self.runtime_profile is None` then reaches into `.data_sources.*`.
- `_record_adaptive_write_policy` (lines 411-429): checks `self.runtime_profile is None` then uses `self.runtime_profile` for an artifact spec.
- `_maybe_count_rows` (lines 431-437): checks `self.recorder is None`.

A null `runtime_profile` is a frequent guard. This suggests the profile should be a required dependency (always present) or the pipeline should have a null-object implementation.

**Findings:**
- `src/datafusion_engine/io/write_pipeline.py:99-104`: Guard-then-access pattern for SQL options.
- `src/datafusion_engine/io/write_pipeline.py:172-197`: Guard-then-reach pattern for dataset resolution (most complex).
- `src/datafusion_engine/io/write_pipeline.py:411-428`: Guard-then-use for adaptive write policy recording.

**Suggested improvement:**
Introduce a `NullDataFusionRuntimeProfile` that implements the same interface with safe no-op defaults. `WritePipeline.__init__` takes `runtime_profile: DataFusionRuntimeProfile` (required) instead of `runtime_profile: DataFusionRuntimeProfile | None`. Tests construct with the null object. This eliminates all `if self.runtime_profile is None` guards and lets the profile object "tell" the pipeline what to do.

**Effort:** medium
**Risk if unaddressed:** medium

---

### Category: Simplicity

#### P19. KISS — Alignment: 2/3

**Current state:**
`datafusion_ext.py` has necessarily complex dual-module resolution (primary `datafusion._internal` + fallback stub + attribute-level fallback). This complexity is justified by the hard test-environment constraint (no real Rust extension in unit tests). The implementation is well-structured with clear `_INTERNAL` / `_FALLBACK_INTERNAL` module handles.

`WritePipeline` defines a `_DeltaCommitFinalizeContext` dataclass nested inside the class body (`io/write_pipeline.py:510-516`). This is unusual and provides no encapsulation benefit over a module-level private dataclass, but adds cognitive indirection when reading the class.

**Findings:**
- `src/datafusion_engine/io/write_pipeline.py:510-516`: Nested `_DeltaCommitFinalizeContext` dataclass inside `WritePipeline` body — no benefit over module-level private dataclass.
- `src/datafusion_engine/extensions/datafusion_ext.py:83-93`: `_normalize_args` iterates positional args and replaces index 0 — positional-arg mutation by index is fragile if extension function signatures change.

**Suggested improvement:**
Move `_DeltaCommitFinalizeContext` to module level in `write_pipeline.py`. In `_normalize_args`, use keyword-argument normalization exclusively (the `ctx` kwarg path), and document that positional `ctx` is only used as a legacy accommodation.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI — Alignment: 2/3

**Current state:**
`compile/options.py:DataFusionCompileOptions` has 32 fields. Fields related to Substrait (`substrait_plan_override`, `prefer_substrait`, `substrait_validation`, `substrait_replay_error_hook`, `record_substrait_gaps`) constitute a parallel execution path. If Substrait is not an active integration path (or is exploratory), these fields represent speculative generality that must be maintained and tested.

`DataFusionCompileOptionsSpec` mirrors `DataFusionCompileOptions` with 17 fields and is the serializable twin. Keeping two parallel structs in sync is a maintenance overhead.

**Findings:**
- `src/datafusion_engine/compile/options.py:75-98`: `DataFusionCompileOptionsSpec` with 17 fields.
- `src/datafusion_engine/compile/options.py:194-237`: `DataFusionCompileOptions` with 32 fields including 5 Substrait-related fields.

**Suggested improvement:**
If Substrait is not a validated production path, consolidate Substrait fields into a single optional `SubstraitOptions` struct that defaults to `None`. This avoids proliferating the 32-field struct while still permitting the path when needed.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P21. Least astonishment — Alignment: 2/3

**Current state:**
`sql/guard.py:_preflight_sql` raises `PermissionError` for DML violations (`line 230`) but `ValueError` for DDL (`line 227`) and statement (`line 232`) violations. Callers catching `ValueError` will miss DML rejections; callers catching `PermissionError` will be surprised by DDL rejections as `ValueError`.

**Findings:**
- `src/datafusion_engine/sql/guard.py:227`: `raise ValueError("DDL statements are disabled")`.
- `src/datafusion_engine/sql/guard.py:230`: `raise PermissionError("DML is blocked by SQL policy")`.
- `src/datafusion_engine/sql/guard.py:233`: `raise ValueError("statements are disabled")`.

**Suggested improvement:**
Raise a single domain exception type for all SQL policy violations, e.g., `SqlPolicyViolation(DataFusionEngineError)` from `datafusion_engine.errors`. The exception can carry a `violation_kind: Literal["ddl", "dml", "statement", "named_args"]` attribute so callers can distinguish without relying on exception class.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts — Alignment: 2/3

**Current state:**
`REQUIRED_RUNTIME_ENTRYPOINTS` is a well-declared contract tuple in `required_entrypoints.py`. The startup call to `ensure_required_runtime_entrypoints()` at module load time (line 296 of `datafusion_ext.py`) is a strong guard.

Gap: `install_planner_rules` and `install_physical_rules` are invoked as "required" via `_call_required` (lines 149-156) but are absent from `REQUIRED_RUNTIME_ENTRYPOINTS`. The startup validation therefore does not check for them, meaning a stub missing these methods would pass startup validation and fail at first use.

**Findings:**
- `src/datafusion_engine/extensions/required_entrypoints.py:5-20`: Missing `install_planner_rules` and `install_physical_rules`.
- `src/datafusion_engine/extensions/datafusion_ext.py:149-156`: Both functions call `_call_required` but are not in the validation tuple.

**Suggested improvement:**
Add `"install_planner_rules"` and `"install_physical_rules"` to `REQUIRED_RUNTIME_ENTRYPOINTS`. The startup validation at module load will then catch missing stubs immediately.

**Effort:** small
**Risk if unaddressed:** medium (silent failure at first planner-rule installation)

---

## Cross-Cutting Themes

### Theme 1: Profile interrogation spread across callers

`DataFusionRuntimeProfile` is passed to `WritePipeline`, `kernels.py`, `catalog/provider.py`, and numerous session sub-modules. In each consumer, callers directly reach into `.policies.*`, `.execution.*`, `.data_sources.*` rather than asking the profile to answer domain questions. This is a systemic violation of Tell-Don't-Ask (P15) and Law of Demeter (P14) rooted in the profile's data-bag design. Adding query methods to `DataFusionRuntimeProfile` in a single focused change would ripple positively across all consumers.

**Root cause:** `DataFusionRuntimeProfile` was likely designed as a configuration container before consumers needed rich behavior.
**Affected principles:** P1, P14, P15.
**Suggested approach:** Add facade methods `DataFusionRuntimeProfile.dataset_candidates(destination)`, `DataFusionRuntimeProfile.join_repartition_enabled(keys)`, and `DataFusionRuntimeProfile.effective_sql_options()` as a focused improvement pass. Existing attribute access can remain as implementation detail behind these methods.

---

### Theme 2: kernels.py as accumulated module

`kernels.py` accumulated content (confirmed by the `PlanSortKey = SortKey` alias at line 98, noting the "(formerly kernel_specs.py)" comment). The file is now a catch-all for kernel-adjacent code. Three distinct algorithmic concerns (deduplication, interval-align join, explode-list) are co-located with shared spec types and DataFusion context helpers. Each section has different test surface, change frequency, and ownership.

**Root cause:** Incremental merging without re-decomposition.
**Affected principles:** P2, P3.
**Suggested approach:** Extract `kernels/interval_align.py` first (largest, most independent), then `kernels/_specs.py` (frozen types only). Keep `kernels.py` as re-export for backward compatibility.

---

### Theme 3: DF52 FFI boundary compatibility

The extensions module's `_normalize_ctx` and `_normalize_args` functions were designed for DF51's PyCapsule convention. DF52 introduces `(py, session)` as the required signature for `__datafusion_*_provider__` methods. The current normalization layer does not inject `session`, and `install_planner_rules`/`install_physical_rules` are not in the startup validation set. When DF52 is adopted, the FFI layer will require targeted updates in `datafusion_ext.py`, `catalog/provider.py`, and `required_entrypoints.py`.

**Root cause:** Version-pinned design assumption not yet forward-proofed.
**Affected principles:** P6, P22.
**Suggested approach:** Add a DF version probe at startup in `datafusion_ext.py` and branch `_normalize_args` to include `session` injection when DF52+ is detected. Add the missing entrypoints to the validation set immediately.

---

## Rust Migration Candidates

| Module | LOC | Current State | Rust Case | DF52 Relevance |
|--------|-----|---------------|-----------|----------------|
| `kernels.py` — interval-align join | ~290 | Python+DF/Arrow DataFrame ops | High: the interval-overlap join with score-based winner selection is an O(N*M) operation on byte-span tables; a Rust `TableProvider` using DataFusion's sort-merge join and sort pushdown (DF52 section D) could eliminate the Python materialization round-trip | DF52 sort pushdown to scans (section D.2) would eliminate Python sort overhead if migrated |
| `kernels.py` — dedupe kernel | ~90 | `row_number()` window UDF | Medium: `dedupe_best_by_score` is already a Rust UDF (`rust/datafusion_ext/src/udf/`); migrating the window-frame setup to Rust would reduce Python overhead | Low direct impact |
| `io/write_delta.py` | 734 | Python Delta write orchestration | Medium: `rust/codeanatomy_engine/src/executor/delta_writer.rs` already exists (461 LOC). The Python write_delta.py duplicates Arrow-to-Delta type mapping and commit orchestration logic that the Rust executor already handles. The gap is the IPC/payload handoff. | DF52 `TableProvider` DELETE/UPDATE hooks (section E) would allow Rust delta_writer to handle mutations natively |
| `obs/metrics_bridge.py` + `obs/datafusion_runs.py` | ~550 combined | Python telemetry aggregation | Low: telemetry is a secondary concern and Python overhead is acceptable here | No DF52 impact |

**Priority recommendation:** `io/write_delta.py` → Rust `delta_writer.rs` integration is the highest-value migration because the Rust infrastructure exists and the Python layer is 734 LOC of Delta-specific orchestration that could be replaced by IPC round-trips to the existing Rust executor.

---

## DF52 Migration Impact

| Change Area | DF52 Section | Affected Files | Impact Level |
|-------------|-------------|----------------|--------------|
| PyCapsule `__datafusion_*_provider__` gains `session` param | K.2 | `extensions/datafusion_ext.py`, `catalog/provider.py`, `required_entrypoints.py` | High — breaks FFI boundary at upgrade |
| `FFI_TableProvider::new` requires `TaskContextProvider` | K.1 | `extensions/datafusion_ext.py:_normalize_args` | High — `_normalize_ctx` does not inject session |
| `ListingTableProvider` caches LIST results by default | C.3 | `catalog/introspection.py:register_cache_introspection_functions` | Medium — DF52 cache TTL footgun (staleness) |
| Statistics cache now accessible via `statistics_cache()` | C.1 | `catalog/introspection.py:statistics_cache_snapshot` (line 589) | Low — fallback logic already handles missing table |
| List files cache now CLI-visible via `list_files_cache()` | C.2 | `catalog/introspection.py:list_files_cache_snapshot` (line 563) | Low — introspection table name may differ |
| `CoalesceBatchesExec` removed | H.1 | No direct Python usage detected | None |
| Parquet row-filter builder signature simplified | G.3 | Not found in scoped files | None |

**Highest-risk DF52 change:** The `(py, session)` PyCapsule signature change (K.2) will silently break all custom catalog/schema/table providers registered via `datafusion_ext.py`. The current `_normalize_ctx` function (`datafusion_ext.py:76-78`) only strips the wrapper but does not add the `session` argument. This is a blocking change that requires a targeted update to `_normalize_args` before DF52 adoption.

---

## Planning-Object Consolidation

| Item | Location | DF Built-in Replacement | LOC |
|------|----------|------------------------|-----|
| `SqlBindings` + `_ResolvedSqlBindings` in `sql/guard.py` | `src/datafusion_engine/sql/guard.py:22-44` | DataFusion `SessionContext.sql_with_options` already accepts `param_values` and `**named_params` directly — the bindings resolution layer is justified for type safety but `_ResolvedSqlBindings` is an internal-only artifact | ~22 LOC overhead |
| `IntrospectionSnapshot.capture` manually queries `information_schema.*` | `src/datafusion_engine/catalog/introspection.py:61-190` | DF52 adds `statistics_cache()` and `list_files_cache()` table functions for cache state — the snapshot capture can use these instead of the fallback column-mapping approach for DF52+ | ~30 LOC reducible |
| `WritePipeline._df_has_rows` collects all batches to check row count | `src/datafusion_engine/io/write_pipeline.py:107-109` | `DataFrame.count()` is a lighter alternative; already used in `_maybe_count_rows` (line 431-437) | ~3 LOC duplication |

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P22 (contract declarations) | Add `install_planner_rules` and `install_physical_rules` to `REQUIRED_RUNTIME_ENTRYPOINTS` in `required_entrypoints.py` | small | Closes silent failure mode at planner-rule install time |
| 2 | P21 (least astonishment) | Unify SQL policy violation exception type: replace mixed `ValueError`/`PermissionError` in `sql/guard.py:227-233` with a single `SqlPolicyViolation` domain exception | small | Predictable error handling for all callers |
| 3 | P6 (Ports & Adapters / DF52) | Audit `_normalize_args` in `datafusion_ext.py` for `session` injection and add it to the DF52 migration checklist as a blocking change | small | Prevents silent DF52 upgrade breakage |
| 4 | P3/P2 (SRP / separation) | Extract `kernels/interval_align.py` from `kernels.py` — move the 15+ private helpers and public `interval_align_kernel` function | medium | Reduces kernels.py from 1,010 to ~720 LOC; isolates the most complex algorithm |
| 5 | P4 (cohesion/coupling) | Remove `DEFAULT_DF_POLICY` import from `catalog/introspection.py:470`; pass defaults as a parameter from the session layer | small | Breaks catalog→session layering inversion |

---

## Recommended Action Sequence

1. **Add missing entrypoints to `REQUIRED_RUNTIME_ENTRYPOINTS`** (`required_entrypoints.py`) — no dependencies, zero risk, closes a silent failure gap. (P22)

2. **Unify SQL policy exceptions** in `sql/guard.py:_preflight_sql` — replace inconsistent `ValueError`/`PermissionError` with a domain exception. Requires adding `SqlPolicyViolation` to `datafusion_engine/errors.py`. (P21)

3. **Remove `DEFAULT_DF_POLICY` import from `catalog/introspection.py`** — pass defaults as a parameter from the call site in the session layer. Breaks the catalog→session layering inversion. (P4, P5)

4. **Add `DataFusionRuntimeProfile` query methods** — add `dataset_candidates(destination)`, `effective_sql_options()`, and `join_repartition_enabled(keys)` as facade methods. Wire existing `WritePipeline` and `kernels.py` call sites to use them. (P14, P15) — depends on nothing above.

5. **Extract `kernels/interval_align.py`** — move `_prepare_interval_tables`, `_rename_right_columns`, `_interval_join_frames`, `_interval_output_schema`, `_interval_order_exprs`, and `interval_align_kernel` to a dedicated module. Keep `kernels.py` as re-export. (P2, P3)

6. **DF52 migration branch: update `_normalize_args`** in `datafusion_ext.py` to inject `session` for `(py, session)` PyCapsule signatures. Audit `catalog/provider.py:_table_from_dataset` for `__datafusion_table_provider__` compatibility. (P6) — should be done in a dedicated DF52 upgrade branch.

7. **`io/write_delta.py` → Rust `delta_writer.rs` integration** — evaluate connecting the Python write orchestration to the existing Rust executor via IPC payload. Estimated LOC reduction: 400+ Python lines. (Rust migration candidate, not a design principle fix — contingent on executor IPC contract being established.)
