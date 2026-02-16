# Design Review: datafusion_engine IO, Extract, Tables, Cache, SQL, Symtable, Pruning, and Utilities

**Date:** 2026-02-16
**Scope:** `src/datafusion_engine/io/`, `src/datafusion_engine/extract/`, `src/datafusion_engine/tables/`, `src/datafusion_engine/cache/`, `src/datafusion_engine/sql/`, `src/datafusion_engine/symtable/`, `src/datafusion_engine/pruning/`, `src/datafusion_engine/kernels.py`, `src/datafusion_engine/hashing.py`, `src/datafusion_engine/registry_facade.py`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 28

## Executive Summary

The scoped modules form the IO, data processing, and caching substrate of the DataFusion engine. The write pipeline (`io/write.py` at 2,703 lines) is the primary concern -- it accumulates multiple responsibilities (Delta policy resolution, commit metadata assembly, feature mutation orchestration, streaming execution, maintenance scheduling, and diagnostics) into a single monolithic module. Extraction templates (`extract/templates.py`, 1,435 lines) use a convention-based discovery pattern via `globals()` that duplicates structural knowledge across 9+ record-builder functions. The kernel module is well-structured with clear operation boundaries. Cache infrastructure is coherent but exhibits duplicated patterns between `ledger.py` and `cache/registry.py`. There are 3 copies of `_sql_identifier` across the scope, violating DRY at the knowledge level. The registry facade provides clean abstraction but eagerly loads all default registries at construction, increasing coupling. Quick wins include extracting shared SQL helpers, decomposing write.py into sub-concerns, and consolidating duplicated schema record builders in templates.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | WritePipeline exposes `ctx` and `runtime_profile` as public attrs |
| 2 | Separation of concerns | 1 | large | high | `io/write.py` mixes Delta policy, commit metadata, feature mutation, maintenance, diagnostics, and write execution |
| 3 | SRP | 1 | large | high | `io/write.py` changes for Delta policy, commit protocol, feature gating, write execution, maintenance, and observability reasons |
| 4 | High cohesion, low coupling | 2 | medium | medium | `extract/templates.py` couples 9 schema record builders with convention-based discovery via `globals()` |
| 5 | Dependency direction | 2 | medium | medium | `registry_facade.py` eagerly imports 7 registries from `semantics/` and `datafusion_engine/schema` at construction |
| 6 | Ports & Adapters | 2 | small | low | `io/adapter.py` provides a clean adapter pattern; `WritePipeline` bypasses it for Delta writes |
| 7 | DRY | 1 | small | medium | `_sql_identifier` duplicated in `write.py:142`, `kernels.py:326`, and `expr/spec.py:193` |
| 8 | Design by contract | 2 | small | low | `WriteRequest` uses frozen dataclass; `DeltaWriteSpec` documents invariants |
| 9 | Parse, don't validate | 2 | medium | low | Policy override parsing (`_delta_*_override`) repeatedly validates at multiple call sites |
| 10 | Make illegal states unrepresentable | 2 | medium | medium | `WriteRequest.format_options` is `dict[str, object]` -- any key/value accepted |
| 11 | CQS | 2 | small | low | `_apply_explicit_delta_features` has side effects and returns None; mostly clean |
| 12 | DI + explicit composition | 2 | medium | low | `WritePipeline` constructs internal collaborators (IO adapter, delta service) ad-hoc |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; composition throughout |
| 14 | Law of Demeter | 1 | medium | medium | Deep chaining: `runtime_profile.policies.delta_store_policy`, `dataset_location.resolved.delta_write_policy` |
| 15 | Tell, don't ask | 2 | medium | low | `_delta_policy_context` reaches into multiple objects to assemble policy |
| 16 | Functional core, imperative shell | 2 | medium | medium | `kernels.py` is largely functional; `write.py` entangles IO with policy logic |
| 17 | Idempotency | 3 | - | - | Explicit idempotent write options with `app_id`/`version` tracking |
| 18 | Determinism | 3 | - | - | Deterministic commit properties, fingerprinting, and canonical table URIs |
| 19 | KISS | 1 | medium | medium | `io/write.py` at 2,703 LOC; `_delta_write_spec` is 120 lines of assembly |
| 20 | YAGNI | 2 | small | low | `_LEGACY_DATASET_ALIASES` is an empty dict; `_DATASET_TEMPLATE_REGISTRY` assigned twice |
| 21 | Least astonishment | 2 | small | low | `WritePipeline.write` ignores `prefer_streaming` parameter |
| 22 | Public contracts | 2 | small | low | `__all__` declarations present in all modules |
| 23 | Testability | 1 | medium | high | `WritePipeline` tightly couples to `SessionContext`, Delta service, and filesystem |
| 24 | Observability | 2 | small | low | Structured diagnostics via `record_artifact`; consistent pattern |

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
`WritePipeline` at `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:854-882` exposes `self.ctx`, `self.sql_options`, `self.recorder`, `self.runtime_profile`, and `self.dataset_resolver` as public mutable attributes despite being internal implementation details.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:878-882`: Five constructor parameters stored as public attributes (`self.ctx`, `self.sql_options`, `self.recorder`, `self.runtime_profile`, `self.dataset_resolver`) that should be prefixed with underscore.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/adapter.py:100-127`: `DataFusionIOAdapter` is a frozen dataclass that properly exposes `ctx` and `profile` -- but callers like `WritePipeline` create fresh adapter instances inline at `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1326,1368`, bypassing any centralized registration point.

**Suggested improvement:**
Prefix `WritePipeline` internal attributes with underscore (`_ctx`, `_runtime_profile`, etc.). Extract a private `_io_adapter()` factory method to centralize adapter creation rather than constructing `DataFusionIOAdapter` at each write callsite.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`io/write.py` at 2,703 lines is the third-largest file in the codebase. It contains at least six distinct concerns in a single module.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:376-442`: Delta policy resolution logic (`_delta_policy_context`) -- 66 lines of policy assembly that reads from `DeltaWritePolicy`, `DeltaSchemaPolicy`, `StatsColumnsInputs`, and `DatasetLocation`.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:2509-2605`: Commit metadata assembly (`_base_commit_metadata`, `_dataset_location_commit_metadata`, `_optional_commit_metadata`, `_delta_commit_metadata`, `_apply_policy_commit_metadata`) -- 96 lines of metadata construction logic.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:690-825`: Delta feature mutation orchestration (`_apply_explicit_delta_features`, `_apply_delta_check_constraints`, `_existing_delta_constraints`) -- 135 lines of feature enablement logic.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1741-1804`: Post-write maintenance scheduling (`_run_post_write_maintenance`) -- 63 lines of maintenance decision logic.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1233-1284`: Diagnostics recording (`_record_write_artifact`, `_record_adaptive_write_policy`) -- diagnostics concern interwoven with write logic.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:2234-2298`: Six `_delta_*_override` functions (stats_columns, zorder_by, enable_features, write_policy, schema_policy, schema_mode) that parse `format_options` dict entries into typed objects.

**Suggested improvement:**
Decompose `io/write.py` into focused sub-modules: (1) `io/write_core.py` -- `WritePipeline`, `WriteRequest`, `WriteResult`, and the write execution methods; (2) `io/delta_policy.py` -- `_DeltaPolicyContext`, `_delta_policy_context`, all `_delta_*_override` parsers, and `DeltaWriteSpec` construction; (3) `io/delta_commit.py` -- commit metadata assembly and idempotent commit reservation; (4) `io/delta_features.py` -- feature mutation and constraint application. The `WritePipeline` class would delegate to these sub-modules via composition.

**Effort:** large
**Risk if unaddressed:** high -- changes to any single concern force understanding the entire 2,703-line file, increasing defect risk and review burden.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`WritePipeline` changes for at least six different reasons, each of which could evolve independently.

**Findings:**
- Delta commit protocol changes (idempotent writes, `CommitProperties` format) would modify `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1417-1463`.
- Delta feature gating changes would modify `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:690-755`.
- Write format changes (CSV, JSON, Arrow writers) would modify `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:2067-2119`.
- Maintenance scheduling changes would modify `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1741-1804`.
- Stats decision policy changes would modify `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:445-468`.
- Observability/diagnostics changes would modify `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1233-1284,1650-1701,1703-1739`.

**Suggested improvement:**
Same as P2 decomposition. Each extracted module would change for exactly one reason: policy resolution, commit protocol, feature mutation, or write execution.

**Effort:** large
**Risk if unaddressed:** high

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
`extract/templates.py` couples template declarations, config declarations, and dataset record builders into a single 1,435-line module. The nine `_*_records()` functions are structurally similar but not parameterized.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/extract/templates.py:482-596`: `_repo_scan_records` produces 5 dataset records with ~115 lines of dict literal construction.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/extract/templates.py:689-901`: `_cst_records` builds a single record with 212 lines of nested shape definitions.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/extract/templates.py:1359-1378`: Convention-based discovery via `globals()` scans module-level symbols four separate times (lines 34, 243, 453, 1363), coupling the discovery mechanism to naming conventions.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:29-103`: `write.py` imports from 15+ modules across `storage/`, `schema_spec/`, `relspec/`, `core_types`, `serde_*`, and `datafusion_engine/` sub-packages.

**Suggested improvement:**
For templates, extract the `nested_shapes` definitions into a declarative data structure (e.g., a dict mapping extractor name to nested shape specs) and generate the repetitive dict records from it, eliminating the per-extractor `_*_records()` functions. For write.py, the decomposition in P2 would naturally reduce import fan-out per module.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The registry facade eagerly loads semantic registries, coupling an infrastructure module to domain-level modules.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/registry_facade.py:107-132`: `_default_registry_adapters()` imports from `datafusion_engine.schema` (4 registries) and `semantics.registry` (3 registries). This is called in `RegistryFacade.__init__` at line 159, creating coupling from infrastructure to domain at construction time.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:48-53`: `write.py` imports `IDENTIFIER_PATTERN` from `core_types` and `DeltaFeatureMutationRequest` from `datafusion_engine.delta.service`, keeping core-to-detail direction, but the sheer volume of imports (26 import statements) suggests the module has too many collaborators.

**Suggested improvement:**
Make `_default_registry_adapters()` lazy or accept registries as an explicit parameter on `RegistryFacade` construction, allowing callers to compose the registry set rather than having the facade eagerly import domain modules.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`io/adapter.py` implements a clean adapter pattern wrapping `SessionContext` operations with diagnostics. However, `WritePipeline` partially bypasses the adapter for Delta writes.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/adapter.py:100-127`: `DataFusionIOAdapter` is a proper adapter that wraps registration operations with diagnostics recording and cache invalidation.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1946-2015`: `_write_delta_bootstrap` directly calls `write_deltalake()` from `deltalake.writer`, bypassing the IO adapter. This is necessary for the Delta-specific API, but it means Delta write instrumentation is inconsistent with other IO paths.

**Suggested improvement:**
No change required for the core Delta write path (the `deltalake` API does not fit the adapter pattern). Consider extracting a `DeltaWriteAdapter` that encapsulates `write_deltalake` calls and retry logic to standardize the instrumentation surface.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Knowledge

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
`_sql_identifier` is defined three times with subtly different implementations across the codebase scope.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:142-147`: `_sql_identifier` validates parts and conditionally quotes.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/kernels.py:326-328`: `_sql_identifier` always quotes with double-quote escaping (simpler variant).
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/expr/spec.py:193` (outside scope but same package): Third copy.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/extract/templates.py:482-596,598-686,689-901,904-968,971-1020,1023-1151,1154-1196,1199-1229,1232-1351`: Nine `_*_records()` functions repeat the same dict structure with `"enabled_when": None, "feature_flag": None, "postprocess": None, "metadata_extra": None, "evidence_required_columns": None` boilerplate in every record.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/cache/ledger.py:104-133` vs `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/cache/ledger.py:198-227`: `ensure_cache_run_summary_table` and `ensure_cache_snapshot_registry_table` are structurally identical (bootstrap + register pattern).

**Suggested improvement:**
(1) Extract `_sql_identifier` and `_sql_string_literal` into a shared `datafusion_engine/sql/helpers.py` module. (2) For templates, define a `_default_record_fields()` function that produces the common boilerplate keys, and have each `_*_records()` function only specify the unique fields via `{**defaults, **specific}`. (3) For ledger, extract a generic `_ensure_ledger_table(ctx, profile, table_name, schema, operation)` helper.

**Effort:** small
**Risk if unaddressed:** medium -- the three `_sql_identifier` variants could drift, causing inconsistent SQL quoting behavior.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Contracts are generally well-defined via frozen dataclasses and typed specifications.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:472-527`: `WriteRequest` is a frozen dataclass with typed fields and docstrings describing each parameter.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:606-633`: `DeltaWriteSpec` is a frozen dataclass with 28 fields, fully documenting the write contract.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/tables/param.py:166-172`: `register_values` validates preconditions (spec existence, key column presence) before proceeding.

**Suggested improvement:**
Minor: add `__post_init__` validation to `WriteRequest` to enforce that `format_options` keys conform to expected types, rather than deferring validation to the point of use in `_delta_*_override` functions.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Policy override parsing happens at multiple call sites via `_delta_*_override` functions that repeatedly validate the same `format_options` dict.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:2268-2298`: `_delta_write_policy_override` and `_delta_schema_policy_override` both check `isinstance(raw, Mapping)`, then convert -- but these are called multiple times within `_delta_policy_context` and `_delta_write_spec`.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/extract/registry.py:332-391`: `normalize_options` correctly implements parse-don't-validate by converting messy inputs into typed options at the boundary.

**Suggested improvement:**
Parse `format_options` into a typed `DeltaFormatOptions` dataclass once at `WritePipeline.write` entry, then pass the parsed object through. This eliminates repeated `options.get()` + type-checking patterns throughout `_delta_policy_context`, `_delta_write_spec`, and `_delta_commit_metadata`.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
`WriteRequest.format_options` is `dict[str, object] | None`, accepting arbitrary keys and values. Invalid combinations are caught at runtime deep in the write pipeline.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:519`: `format_options: dict[str, object] | None = None` -- an untyped bag allowing any key/value combination.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:2268-2282`: `_delta_write_policy_override` silently returns `None` if `raw` is not a recognized type, masking misconfigurations.

**Suggested improvement:**
Define a `DeltaFormatOptions` msgspec struct (or frozen dataclass) with typed fields for `write_policy`, `schema_policy`, `maintenance_policy`, `feature_gate`, `writer_properties`, `storage_options`, `stats_columns`, `zorder_by`, `enable_features`, `target_file_size`, `commit_metadata`, and `idempotent`. Accept `DeltaFormatOptions | dict[str, object] | None` and parse early.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. A few command functions also return values, but this is acceptable for builder patterns.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1169-1198`: `WritePipeline.write` both modifies state (writes data) and returns `WriteResult` -- acceptable command-with-receipt pattern.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/tables/param.py:152-192`: `register_values` both mutates `self.artifacts` and returns `ParamTableArtifact` -- acceptable since the return confirms the registration outcome.

**Suggested improvement:**
No changes needed. The command-with-receipt pattern is appropriate for write operations.

**Effort:** N/A
**Risk if unaddressed:** low

---

### Category: Composition

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
`WritePipeline` constructs collaborators ad-hoc rather than accepting them through injection.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1828`: `delta_service_for_profile(self.runtime_profile)` called inside `_write_delta` -- the Delta service is resolved at each write call rather than injected at construction.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1326,1368`: `DataFusionIOAdapter(ctx=self.ctx, profile=self.runtime_profile)` constructed inline at each write method.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/cache/ledger.py:170,265`: `WritePipeline(ctx=active_ctx, runtime_profile=profile)` constructed at each ledger operation.

**Suggested improvement:**
Accept an optional `delta_service` parameter on `WritePipeline.__init__` and lazily resolve it once. Create a private `_adapter` property that caches the IO adapter instance.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies exist in the reviewed scope. All behavior is composed via delegation, frozen dataclasses, and protocol types.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/registry_facade.py:27-39`: `ProviderRegistryLike` uses Protocol for structural typing rather than inheritance.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/tables/registration.py:38-54`: `ListingRegistrationContext` is a Protocol with property accessors.

**Suggested improvement:**
None needed. This principle is well-satisfied.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
Deep attribute chains are common throughout `io/write.py` and `registry_facade.py`, requiring callers to understand internal structure of collaborators.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:387-390`: `dataset_location.resolved.delta_write_policy` and `dataset_location.resolved.delta_schema_policy` -- reaching through `.resolved` accessor to get policy objects.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1002-1003`: `self.runtime_profile.policies.delta_store_policy` -- three-level chain.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1603`: `self.runtime_profile.delta_ops.reserve_delta_commit(...)` -- three-level chain.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/registry_facade.py:399`: `runtime_profile.function_factory_policy_hash(ctx)` is acceptable, but `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/cache/registry.py:156`: `profile.delta_ops.delta_service().table_version(...)` is a four-level chain.

**Suggested improvement:**
Add convenience methods on `DataFusionRuntimeProfile` for the most common chain patterns: `profile.delta_write_policy()`, `profile.delta_schema_policy()`, `profile.reserve_commit(...)`. For `DatasetLocation`, add `location.write_policy` as a property that delegates through `.resolved`.

**Effort:** medium
**Risk if unaddressed:** medium -- deep chains propagate breakage when intermediate structures are refactored.

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
`_delta_policy_context` at `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:376-442` is the clearest "ask" violation -- it reaches into `dataset_location`, `write_policy`, `schema_policy`, and `options` to assemble a policy context externally.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:386-390`: Asks `dataset_location.resolved.delta_write_policy` and falls back to None -- rather than telling the location to provide its resolved policy.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1033-1034`: `dataset_spec = dataset_location.dataset_spec if dataset_location is not None else None` -- conditional None-check pattern repeated for three fields.

**Suggested improvement:**
Add a `DatasetLocation.resolved_policy_context()` method that returns a complete policy bundle, centralizing the None-fallback logic. The caller would tell the location to resolve its policies rather than asking for individual fields.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Correctness

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
`kernels.py` successfully separates pure computation (spec types, sort key manipulation, ordering logic) from IO (DataFusion context creation, table ingestion). `write.py` does not maintain this separation.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/kernels.py:39-98`: Spec types (`SortKey`, `DedupeSpec`, `IntervalAlignOptions`, `AsofJoinSpec`) are pure, frozen value objects with no IO.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/kernels.py:404-476`: `dedupe_kernel` and `winner_select_kernel` perform IO (session context creation, table ingestion) but the core deduplication logic in `_dedupe_dataframe` at line 358 is a pure DataFrame transformation.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1465-1584`: `_delta_write_spec` is a 120-line method that is mostly pure policy assembly but is entangled with `self._reserve_runtime_commit` (an IO operation at line 1450) and `self._record_adaptive_write_policy` (diagnostics IO at line 1510).

**Suggested improvement:**
Extract `_delta_write_spec` policy assembly into a pure function that accepts pre-resolved inputs (commit reservation result, adaptive decision) and returns a `DeltaWriteSpec`. Move the IO calls (`_reserve_runtime_commit`, `_record_adaptive_write_policy`) to the caller.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
The write pipeline has explicit idempotency support through `IdempotentWriteOptions` with `app_id` and `version` tracking.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:2645-2661`: `_delta_idempotent_options` parses idempotent write options from format options.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1586-1613`: `_reserve_runtime_commit` reserves idempotent commits through the runtime profile's Delta ops.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1546-1554`: `idempotent_commit_properties` produces deterministic commit properties with operation and mode labels.

**Suggested improvement:**
None needed. Idempotency is well-designed with explicit reservation and finalization lifecycle.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Determinism is a first-class concern across the scope.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/hashing.py:127-138`: `stable_id` produces deterministic identifiers using `hash128_from_text`.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/tables/param.py:336-359`: `param_signature_from_array` produces deterministic signatures by sorting values and hashing with SHA-256.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:192-255`: `_DeltaPolicyContext` is frozen and has a `fingerprint()` method for deterministic policy tracking.

**Suggested improvement:**
None needed.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Simplicity

#### P19. KISS -- Alignment: 1/3

**Current state:**
The 2,703-line `io/write.py` is the primary simplicity concern. The `_delta_write_spec` method at 120 lines and `_write_delta` at 120+ lines demonstrate excessive complexity in single functions.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1806-1944`: `_write_delta` is 138 lines handling: version check, protocol validation, bootstrap write, feature enablement, constraint application, mutation recording, version finalization, commit finalization, and maintenance scheduling. Each of these is a distinct step that could be a separate method.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/extract/templates.py:28-39,234-248,444-458,1359-1378`: Four `globals()`-scanning discovery functions follow the same pattern. While DRY in structure, the convention-based approach via `globals()` requires understanding implicit naming contracts to add new templates.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:2459-2506`: `_delta_storage_options` defines a nested function `_require_str_mapping` that could be a module-level helper.

**Suggested improvement:**
(1) Decompose `_write_delta` into a pipeline of named steps: `_validate_pre_write`, `_bootstrap_write`, `_apply_features`, `_apply_constraints`, `_record_mutation`, `_finalize_version`, `_schedule_maintenance`. (2) For templates, consider replacing the `globals()` scan with an explicit registration list at the end of the module.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
A few unused or redundant constructs exist but overall the code avoids speculative generality.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/extract/registry.py:63`: `_LEGACY_DATASET_ALIASES: dict[str, str] = {}` -- an empty dict that is checked on every `extract_metadata` and `dataset_schema` call but never populated.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/extract/templates.py:1354-1356`: `_DATASET_TEMPLATE_REGISTRY` is assigned an empty `ImmutableRegistry` at line 1356 and immediately overwritten at line 1378. The initial assignment is dead code.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1197`: `WritePipeline.write` accepts `prefer_streaming` but immediately discards it (`_ = prefer_streaming`), always delegating to `write_via_streaming`.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:2684-2698`: `_delta_configuration` function is defined but never called within the write module.

**Suggested improvement:**
(1) Remove the empty `_LEGACY_DATASET_ALIASES` or replace with a TODO comment noting planned deprecation. (2) Remove the initial `_DATASET_TEMPLATE_REGISTRY` assignment. (3) Remove or deprecate `prefer_streaming` parameter if streaming is the only supported path. (4) Verify `_delta_configuration` usage outside this module; if unused, remove it.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
The `prefer_streaming` parameter that is silently ignored violates user expectations.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1169-1198`: `write(request, prefer_streaming=True)` documents "if True, prefer streaming write for DELTA format" but the implementation at line 1197 discards the value with `_ = prefer_streaming`.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/sql/options.py:57-66`: `safe_sql_options_for_profile` accepts a `profile` parameter but ignores it (`_ = profile`), always returning default read-only options. This is documented behavior but the parameter signature is misleading.

**Suggested improvement:**
Either remove the `prefer_streaming` parameter from `write()` or implement the non-streaming fallback path. For `safe_sql_options_for_profile`, rename to `default_safe_sql_options()` or document that the parameter is reserved for future use.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
All modules declare `__all__` exports. Key types use frozen dataclasses or `StructBaseStrict` for versioned contracts.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:106-129`: `WriteFormat`, `WriteMode`, `WriteMethod` enums define the contract vocabulary.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/kernels.py:1004-1019`: `__all__` explicitly declares the public surface including spec types and kernel functions.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/tables/param.py:27`: `SCALAR_PARAM_SIGNATURE_VERSION = 1` -- versioned contract for parameter signatures.

**Suggested improvement:**
Minor: Add versioning to `WriteRequest` and `DeltaWriteSpec` (e.g., a `_VERSION` class attribute) to track contract evolution, especially given the 28-field `DeltaWriteSpec`.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
`WritePipeline` is difficult to unit test because it tightly couples to `SessionContext`, the Delta service, the filesystem, and diagnostics recording without injection points.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:854-882`: Constructor takes `SessionContext` directly (not an abstraction), plus optional `runtime_profile` that gates most behavior. Testing any write path requires a real or heavily mocked `SessionContext`.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1828`: `delta_service_for_profile(self.runtime_profile)` -- service resolution inside the method body, not injectable.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1294-1313`: `_prepare_destination` does filesystem I/O (`path.exists()`, `shutil.rmtree`, `path.parent.mkdir`) directly, making unit testing require filesystem setup.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/cache/ledger.py:104-195`: `ensure_cache_run_summary_table` and `record_cache_run_summary` combine table bootstrapping, IO registration, pipeline construction, and write execution in a single call chain, requiring full integration setup to test.

**Suggested improvement:**
(1) Accept an optional `delta_service` parameter in `WritePipeline.__init__` for test injection. (2) Extract filesystem operations into an injectable `WriteDestinationResolver` that can be replaced with an in-memory implementation. (3) For the pure policy assembly functions (`_delta_policy_context`, `_stats_decision_from_policy`), ensure they remain free of IO so they can be tested in isolation without mocking.

**Effort:** medium
**Risk if unaddressed:** high -- critical write pipeline code is difficult to test without heavy integration setup, reducing coverage confidence.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Observability follows a consistent pattern via `record_artifact` and `DiagnosticsRecorder`. However, the pattern is scattered across many callsites within `WritePipeline`.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1233-1264`: `_record_write_artifact` produces structured write diagnostics with format, method, rows, duration, and features.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1650-1701`: `_persist_write_artifact` records write metadata to the plan artifact store with full commit context.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/write.py:1703-1739`: `_record_delta_mutation` records mutation artifacts with constraint status.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/io/adapter.py:484-520`: `_record_registration` and `_record_artifact` provide consistent diagnostics for all registration operations.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/pruning/metrics.py:12-39`: `PruningMetrics` is a clean, frozen contract with all fields defaulted and documented.

**Suggested improvement:**
Consolidate the three `_record_*` methods in `WritePipeline` into a single `_emit_write_diagnostics(outcome)` method that dispatches to the appropriate artifact specs. This would centralize observability for writes rather than spreading it across `_record_write_artifact`, `_persist_write_artifact`, `_record_delta_mutation`, and `_record_adaptive_write_policy`.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Write Pipeline Monolith

**Description:** `io/write.py` at 2,703 lines accumulates six distinct concerns that evolve independently. This is the root cause of violations in P2 (separation of concerns), P3 (SRP), P4 (coupling), P14 (Law of Demeter), P16 (functional core/imperative shell), P19 (KISS), and P23 (testability).

**Root cause:** The write pipeline grew organically as Delta capabilities were added (features, constraints, maintenance, idempotency, adaptive file sizing, protocol validation). Each capability was added to the existing `WritePipeline` class rather than composed as a separate collaborator.

**Affected principles:** 2, 3, 4, 14, 16, 19, 23

**Suggested approach:** Decompose into `io/write_core.py` (pipeline orchestration), `io/delta_policy.py` (policy resolution), `io/delta_commit.py` (commit assembly), `io/delta_features.py` (feature mutation). `WritePipeline` becomes a thin orchestrator that delegates to injected sub-components.

### Theme 2: Convention-Based Discovery via globals()

**Description:** `extract/templates.py` uses `globals()` scanning four times to discover template, config, and record builder symbols. This pattern is fragile (relies on naming conventions), hard to navigate (no explicit registration), and prevents IDE-assisted refactoring.

**Root cause:** Templates were added incrementally with a convention that avoids explicit registration boilerplate. However, with 9+ extractors and 3 discovery dimensions, the convention creates more cognitive load than it saves.

**Affected principles:** 4, 19, 21

**Suggested approach:** Replace `globals()` scanning with an explicit `_TEMPLATE_REGISTRY` dict populated at module level. This makes the template set explicit, searchable, and IDE-friendly while adding minimal boilerplate.

### Theme 3: Duplicated SQL and Ledger Patterns

**Description:** Three copies of `_sql_identifier`, two structurally identical `ensure_cache_*_table` functions, and repeated `format_options` override parsing create semantic duplication that risks drift.

**Root cause:** Each module defined its own utility functions rather than sharing them from a common location.

**Affected principles:** 7

**Suggested approach:** Extract shared SQL helpers to `datafusion_engine/sql/helpers.py`. Extract generic ledger table bootstrapping to `cache/ledger_helpers.py`.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Extract `_sql_identifier`/`_sql_string_literal` to `sql/helpers.py` | small | Eliminates 3 copies of SQL escaping logic, prevents drift |
| 2 | P21 (Least astonishment) | Remove or implement `prefer_streaming` parameter on `write()` | small | Removes misleading API contract |
| 3 | P20 (YAGNI) | Remove empty `_LEGACY_DATASET_ALIASES`, dead `_DATASET_TEMPLATE_REGISTRY` initial assignment | small | Reduces cognitive load |
| 4 | P7 (DRY) | Extract `_ensure_ledger_table` generic helper from duplicate `ensure_cache_*` functions | small | Eliminates structural duplication in cache ledger |
| 5 | P1 (Info hiding) | Prefix `WritePipeline` internal attributes with underscore | small | Prevents external dependency on internal state |

## Recommended Action Sequence

1. **Extract shared SQL helpers** (P7): Create `datafusion_engine/sql/helpers.py` with `sql_identifier()` and `sql_string_literal()`. Update `io/write.py`, `kernels.py`, and `expr/spec.py` to import from the shared module. This is a low-risk, high-confidence change with no behavioral impact.

2. **Clean up YAGNI artifacts** (P20, P21): Remove `_LEGACY_DATASET_ALIASES`, dead `_DATASET_TEMPLATE_REGISTRY` assignment, unused `_delta_configuration`, and the `prefer_streaming` parameter (or add a deprecation warning). Each is an independent, zero-risk change.

3. **Prefix WritePipeline internals** (P1): Rename `ctx`, `sql_options`, `recorder`, `runtime_profile`, `dataset_resolver` to underscore-prefixed names. Update internal references within `write.py`.

4. **Consolidate cache ledger helpers** (P7): Extract `_ensure_ledger_table(ctx, profile, name, schema, operation)` from the two duplicate `ensure_cache_*_table` functions in `cache/ledger.py`.

5. **Parse format_options once** (P9, P10): Define a typed `DeltaFormatOptions` struct and parse `format_options` at the `write()` entry point. This eliminates repeated `_delta_*_override` calls and makes illegal states more visible.

6. **Decompose write.py** (P2, P3, P19, P23): This is the largest change. Extract Delta policy resolution, commit metadata assembly, feature mutation, and maintenance scheduling into separate sub-modules. `WritePipeline` becomes a thin orchestrator. This should happen after steps 1-5 to reduce the diff size.

7. **Replace globals() scanning in templates** (P4, P19): Replace the four `globals()` scans with explicit registration dicts. This can happen independently of other changes.

8. **Add convenience methods to reduce Demeter violations** (P14): Add `DatasetLocation.write_policy`, `DatasetLocation.schema_policy`, and `DataFusionRuntimeProfile.reserve_commit()` convenience methods to flatten the most common deep chains.
