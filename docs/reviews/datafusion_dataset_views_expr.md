# Design Review: src/datafusion_engine/ (dataset/, views/, expr/)

**Date:** 2026-02-16
**Scope:** `src/datafusion_engine/dataset/`, `src/datafusion_engine/views/`, `src/datafusion_engine/expr/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 16

## Executive Summary

The reviewed scope contains 9,078 lines across 16 files covering dataset registration, view graph management, and expression planning. The `dataset/registration.py` file at 3,350 lines is a critical structural concern -- it conflates DDL generation, Delta provider registration, caching, observability artifact recording, schema evolution, projection handling, and partition validation into a single module that changes for at least six distinct reasons. The expression subsystem (`expr/`) is well-structured with clean separation of concerns and strong contracts. The view graph (`views/graph.py`) has solid topological sorting and dependency-aware registration but shares duplicated schema-extraction logic with multiple other modules. The boundary between dataset registration and view registration is reasonably clean, though observability artifact recording is heavily interspersed with core logic throughout both subsystems.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | medium | medium | Module-level mutable caches (`_CACHED_DATASETS`, `_RESOLVED_LOCATION_CACHE`, `_resolver_identity_guard`) exposed as dict globals |
| 2 | Separation of concerns | 1 | large | high | `registration.py` mixes DDL generation, Delta provider management, schema evolution, caching, observability, and partition validation |
| 3 | SRP (one reason to change) | 0 | large | high | `registration.py` changes for at least 6 independent reasons (DDL syntax, Delta protocol, caching, observability specs, schema evolution, projection) |
| 4 | High cohesion, low coupling | 1 | large | high | `registration.py` imports from 25+ internal modules; `views/graph.py` duplicates schema extraction logic |
| 5 | Dependency direction | 2 | medium | medium | `views/registry_specs.py` depends on `semantics.pipeline` (core importing core is appropriate, but private function `_cpg_view_specs` is imported) |
| 6 | Ports & Adapters | 2 | medium | low | `DataFusionIOAdapter` acts as a proper adapter; Delta provider construction routes through `resolution.py` appropriately |
| 7 | DRY (knowledge) | 1 | medium | medium | `arrow_schema_from_df` duplicated in 4 locations; Delta registration artifact construction repeated across staging/output cache paths |
| 8 | Design by contract | 2 | small | low | `DatasetLocation`, `ViewNode`, `ExprIR` have explicit `__post_init__` validation; `SchemaContract` enforced at view boundaries |
| 9 | Parse, don't validate | 2 | small | low | `ScalarLiteralCodec` normalizes inputs once; `DatasetLocation.__post_init__` normalizes path; `ExprIR.__post_init__` normalizes literals |
| 10 | Make illegal states unrepresentable | 2 | medium | medium | `CachePolicy` is a string literal type not an enum; `ExprIR.op` is a raw string instead of an enum |
| 11 | CQS | 1 | medium | medium | `_register_view_with_cache` both registers (mutation) and returns DataFrame (query); `_register_delta_staging_cache` writes and returns |
| 12 | Dependency inversion | 2 | small | low | Context dataclasses (`ViewGraphContext`, `DataFusionRegistrationContext`) provide explicit DI; `MutableRegistry` protocol used |
| 13 | Prefer composition | 3 | - | - | No inheritance hierarchies; `ViewNode` composes behavior via `builder` and `contract_builder` callables |
| 14 | Law of Demeter | 1 | medium | medium | Deep property chains like `location.resolved.delta_write_policy.parquet_writer_policy.statistics_enabled` in `registry_snapshot()` |
| 15 | Tell, don't ask | 1 | medium | medium | `registry_snapshot()` manually extracts ~30 fields from `ResolvedDatasetLocation` instead of delegating serialization |
| 16 | Functional core, imperative shell | 1 | large | medium | Observability recording (`record_artifact`) is deeply interleaved with registration logic in both `registration.py` and `graph.py` |
| 17 | Idempotency | 2 | small | low | `register_dataset_df` supports `overwrite=True`; `DatasetCatalog.register` has `overwrite` parameter; view graph resets identity guard |
| 18 | Determinism / reproducibility | 3 | - | - | Fingerprints, identity hashes, and plan signatures tracked throughout; `_topo_sort_nodes` uses stable sorted ordering |
| 19 | KISS | 1 | large | high | `registration.py` at 3,350 LOC with ~80 private functions is the primary complexity hotspot |
| 20 | YAGNI | 2 | small | low | `_DatasetInputSource` plugin mechanism is used; `DataFusionCachePolicy` serves real configuration needs |
| 21 | Least astonishment | 2 | small | low | `arrow_schema_from_df` in `views/graph.py` is actually imported from `bundle_extraction.py` and re-exported -- import chain is indirect |
| 22 | Declare and version contracts | 2 | small | low | `__all__` exports declared in all modules; `ViewNode`, `ExprSpec`, `DatasetLocation` are stable public contracts |
| 23 | Design for testability | 1 | medium | high | Module-level mutable state (`_CACHED_DATASETS`, `_REGISTERED_CATALOGS`, `_RESOLVED_LOCATION_CACHE`) makes isolated testing difficult |
| 24 | Observability | 2 | medium | low | Comprehensive artifact recording throughout; structured payloads via `record_artifact`; spans in cache paths |

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Most internal decisions are properly hidden behind stable public APIs. `DatasetLocation` exposes a `resolved` property that caches computed configuration. However, module-level mutable dicts serve as implicit global caches.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:142-144`: Three module-level mutable dicts (`_CACHED_DATASETS`, `_REGISTERED_CATALOGS`, `_REGISTERED_SCHEMAS`) keyed by `id(ctx)` act as implicit global caches with no encapsulation, expiration, or thread-safety guarantees.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registry.py:122`: `_RESOLVED_LOCATION_CACHE` is a module-level dict keyed by `id(location)`, which is fragile since Python may reuse object IDs after garbage collection.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:60`: `_resolver_identity_guard` is a module-level mutable dict acting as a singleton state tracker across registration batches.

**Suggested improvement:**
Encapsulate the three `registration.py` caches and the `_RESOLVED_LOCATION_CACHE` into a `RegistrationStateManager` class that is explicitly constructed and passed via context objects. Replace `id()`-keyed caches with `WeakValueDictionary` or explicit scope management to prevent stale entries after GC.

**Effort:** medium
**Risk if unaddressed:** medium -- stale cache entries after GC could cause subtle bugs in long-running processes.

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`registration.py` is the primary violation. It handles at least six distinct concerns in a single 3,350-line module: DDL generation (lines 677-918), Delta provider management (lines 1457-1600), caching logic (lines 3095-3200+), observability artifact recording (8+ `_record_*` functions), schema evolution adapter management (lines 2561-2673), and partition validation (lines 2816-2978).

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:677-918`: DDL generation functions (`_ddl_identifier`, `_ddl_column_definitions`, `_ddl_statement`, `_external_table_ddl`) are pure string-building utilities that have no dependency on registration state and could exist independently.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:1602-1710`: Delta registration artifact recording functions are purely observability concerns interleaved with core registration logic.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:2561-2673`: Schema evolution adapter factory resolution is a distinct concern from dataset registration.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:2816-2978`: Partition schema validation is a validation concern mixed with registration infrastructure.

**Suggested improvement:**
Extract `registration.py` into at least four focused modules:
1. `dataset/ddl.py` -- DDL generation functions (pure string building, ~240 lines)
2. `dataset/registration_artifacts.py` -- All `_record_*` functions and artifact payload builders (~400 lines)
3. `dataset/schema_adapter.py` -- Schema evolution adapter factory resolution (~120 lines)
4. `dataset/partition_validation.py` -- Partition schema validation (~200 lines)

This would reduce `registration.py` to ~2,400 lines of core registration orchestration, which could then be further decomposed into listing-table and Delta-table registration paths.

**Effort:** large
**Risk if unaddressed:** high -- the file is difficult to navigate, test, and modify safely; changes to DDL syntax risk breaking Delta provider logic and vice versa.

---

#### P3. SRP (one reason to change) -- Alignment: 0/3

**Current state:**
`registration.py` changes for at least six independent reasons: (1) DataFusion DDL syntax changes, (2) Delta protocol version updates, (3) cache policy changes, (4) observability spec evolution, (5) schema evolution adapter API changes, (6) projection expression handling changes.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:197-211`: `_DDL_TYPE_ALIASES` dictionary encoding DataFusion type mapping -- changes when DataFusion type system evolves.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:440-487`: `_DEFAULT_SCAN_CONFIGS` -- changes when new extraction sources are added (CST, AST, bytecode, tree-sitter, symtable configs all in this file).
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:1826-1855`: `_delta_log_health_payload` -- changes when Delta protocol observability evolves.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:670-698`: `_register_view_with_cache` changes for both caching strategy and registration strategy reasons.

**Suggested improvement:**
Same decomposition as P2. Additionally, move `_DEFAULT_SCAN_CONFIGS` (lines 440-487) to a dedicated `dataset/scan_defaults.py` module since it encodes extraction-source-specific knowledge that changes when new extractors are added.

**Effort:** large
**Risk if unaddressed:** high -- accidental coupling between unrelated concerns means changes to one area risk regression in another.

---

#### P4. High cohesion, low coupling -- Alignment: 1/3

**Current state:**
`registration.py` has 25+ internal module imports at the top level plus numerous deferred imports within functions. The `views/graph.py` module imports `arrow_schema_from_df` from `views/bundle_extraction.py`, but `registration.py` imports it from `views/graph.py` (re-export chain). Related utilities are scattered.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:30-135`: 35 import statements at module scope, plus at least 20 deferred imports inside functions -- indicating this module touches far too many subsystems.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:121`: `from datafusion_engine.views.graph import arrow_schema_from_df` -- a dataset module importing from views module creates a cross-domain coupling; the function actually lives in `views/bundle_extraction.py`.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/registry_specs.py:438`: `from semantics.pipeline import CpgViewSpecsRequest, _cpg_view_specs` -- importing a private function (`_cpg_view_specs`) from another module creates tight coupling to implementation details.

**Suggested improvement:**
(1) Move `arrow_schema_from_df` to a shared utility module (e.g., `datafusion_engine/arrow/schema_utils.py`) and import from there in both `dataset` and `views`. (2) Expose `_cpg_view_specs` as a public function in `semantics.pipeline` (rename to `cpg_view_specs`) since it is used cross-module. (3) Reduce `registration.py` import surface by extracting the submodules described in P2.

**Effort:** large
**Risk if unaddressed:** high -- import chain complexity increases the chance of circular imports and makes the dependency graph opaque.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Generally well-aligned. Core types (`DatasetLocation`, `ViewNode`, `ExprIR`) are defined in leaf modules with minimal dependencies. The dataset and views subsystems appropriately depend on `schema_spec`, `serde_artifacts`, and `datafusion_engine.arrow` for foundational types.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/registry_specs.py:438`: `from semantics.pipeline import CpgViewSpecsRequest, _cpg_view_specs` -- views depend on semantics pipeline, which is directionally acceptable (views are a detail of the semantic pipeline), but the import of a private function signals missing abstraction.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/semantic_catalog.py:17`: `from semantics.catalog.dataset_rows import get_all_dataset_rows` -- dataset module depends on semantic catalog, which is appropriate since dataset registration is downstream of semantic definitions.

**Suggested improvement:**
Define a public `cpg_view_specs()` function in `semantics.pipeline` that replaces the private `_cpg_view_specs` import.

**Effort:** medium
**Risk if unaddressed:** medium -- private function dependencies silently break when the semantics module is refactored.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`DataFusionIOAdapter` serves as a proper adapter for DataFusion session operations. `DatasetResolution` pipeline cleanly separates provider construction from registration. The expression subsystem (`ExprIR`, `ExprSpec`) acts as a portable IR that can target both SQL text and DataFusion Expr objects.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/resolution.py:76-96`: `resolve_dataset_provider` cleanly dispatches between delta and delta_cdf providers through a well-defined request/response pattern.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/expr/spec.py:198-229`: `ExprIR` with `to_sql()` and `to_expr()` methods provides a clean port for expression compilation that adapts to both SQL text and DataFusion expression objects.

**Suggested improvement:**
No critical gaps. The `DataFusionIOAdapter` pattern could be more consistently used -- `registration.py` sometimes creates its own adapter instances (`DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)`) ad hoc rather than receiving one via context.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Knowledge

#### P7. DRY (knowledge) -- Alignment: 1/3

**Current state:**
The most significant DRY violation is the `arrow_schema_from_df` function, which exists in 4 variant implementations across the codebase. Additionally, the Delta cache registration logic (`_register_delta_staging_cache` and `_register_delta_output_cache` in `views/graph.py`) contains near-identical code blocks for cache hit checking, schema evolution enforcement, writing, and inventory recording.

**Findings:**
- `arrow_schema_from_df` implementations at:
  - `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/bundle_extraction.py:19` (canonical)
  - `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/bundle_artifact.py:2339` (`_arrow_schema_from_df`)
  - `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:2796` (`_arrow_schema_from_dataframe`)
  - `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/registry_specs.py:226` (`_arrow_schema_from_contract`)
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:701-885` vs `graph.py:888-1074`: `_register_delta_staging_cache` and `_register_delta_output_cache` are ~180 lines each with ~70% structural overlap (both check cache hits, enforce schema evolution, write via `WritePipeline`, register cached table, record inventory, and record artifacts).
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:1001-1021` and `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registry.py:206-233`: Scan details are serialized to dict in both `_scan_details()` and `registry_snapshot()` with overlapping field enumeration.

**Suggested improvement:**
(1) Consolidate all `arrow_schema_from_df` variants into a single canonical implementation in `datafusion_engine/arrow/schema_utils.py` and import everywhere. (2) Extract a shared `_write_and_register_delta_cache()` helper from the two delta cache registration functions in `views/graph.py`, parameterizing the differences (staging path vs output path, storage options). (3) Add a `to_snapshot_dict()` method to `DataFusionScanOptions` so serialization logic is not duplicated.

**Effort:** medium
**Risk if unaddressed:** medium -- divergence between duplicated implementations causes subtle schema resolution bugs.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Contracts are generally well-expressed. `DatasetLocation.__post_init__` normalizes paths. `ExprIR.__post_init__` normalizes literals. `SchemaContract` provides explicit validation at view boundaries. `ViewNode` clearly declares its dependencies and requirements.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/expr/spec.py:206-208`: `ExprIR.__post_init__` normalizes the `value` field to a canonical tagged spec -- good enforcement at construction time.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:248-308`: `register_view_graph` validates UDF snapshots, checks resolver identity, materializes nodes, and validates deps before proceeding -- clear precondition chain.

**Suggested improvement:**
Add explicit validation to `ExprIR` that rejects unknown `op` values at construction time rather than waiting for `to_sql()` or `to_expr()` calls to fail.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
The `ScalarLiteralCodec` is an exemplary implementation of parse-don't-validate: it converts raw Python values into tagged literal specs at the boundary. `DatasetLocation` normalizes path types at construction. The `_materialize_nodes` function resolves dependencies and UDF requirements from plan bundles into structured `ViewNode` instances once.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/expr/spec.py:118-133`: `ScalarLiteralCodec.from_input` converts heterogeneous input types into a tagged union once at the boundary.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:1199-1220`: `_materialize_nodes` resolves deps and UDFs from plan bundles, replacing raw `ViewNode` instances with fully-resolved versions.

**Suggested improvement:**
No critical gaps. The pattern is well-applied in this scope.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
`ScalarLiteralSpec` uses tagged unions effectively. However, several places use string literals where enums would prevent invalid states.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/artifacts.py:25`: `CachePolicy = Literal["none", "delta_staging", "delta_output"]` -- this is a string literal type, not an enum. The normalization function `_normalize_cache_policy` at `views/graph.py:1223-1228` silently converts unknown strings to `"none"` instead of failing.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/expr/spec.py:201`: `ExprIR.op` is `str` -- any string is accepted at construction time. Only 3 values are valid ("field", "literal", "call"), but this isn't enforced until `to_sql()`/`to_expr()` is called.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/resolution.py:42`: `DatasetProviderKind = Literal["delta", "delta_cdf"]` -- correctly restrictive.

**Suggested improvement:**
(1) Replace `CachePolicy` string literal with an `enum.StrEnum` and raise on unknown values in `_normalize_cache_policy` instead of silently defaulting. (2) Restrict `ExprIR.op` to `Literal["field", "literal", "call"]` with `__post_init__` validation.

**Effort:** medium
**Risk if unaddressed:** medium -- silent fallback to "none" cache policy could mask configuration errors.

---

#### P11. CQS -- Alignment: 1/3

**Current state:**
Several core functions violate CQS by both mutating state (registering tables/views) and returning values (DataFrames).

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:670-698`: `_register_view_with_cache` registers a view (command) and returns a DataFrame (query) in the same call.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:701-885`: `_register_delta_staging_cache` writes to Delta, registers a table, records cache artifacts, and returns a DataFrame.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:1115-1202`: `register_dataset_df` registers a dataset (command) and returns a DataFrame (query).
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:1331-1381`: `_register_dataset_with_context` registers, caches, applies projections, validates contracts, and returns a DataFrame.

**Suggested improvement:**
Separate the registration command from the DataFrame query. `register_dataset_df` could return a `RegistrationResult` containing a reference to the table name, and callers could separately call `ctx.table(name)` to get the DataFrame. This also makes the intent clearer in call sites.

**Effort:** medium
**Risk if unaddressed:** medium -- combining mutation and query in one call makes it harder to reason about side effects during testing and orchestration.

---

### Category: Composition

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
Context dataclasses are used effectively for dependency injection. `ViewGraphContext`, `DataFusionRegistrationContext`, `CacheRegistrationContext`, and `SemanticViewNodeContext` all bundle dependencies explicitly. However, some functions create their own adapter instances instead of receiving them.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:278-289`: `register_view_graph` constructs `DataFusionIOAdapter` and bundles it into `ViewGraphContext` -- good explicit composition.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:1280-1285`: `_register_object_store_for_location` creates its own `DataFusionIOAdapter` -- inconsistent with the context-based pattern used elsewhere.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/resolution.py:200`: `apply_scan_unit_overrides` creates its own adapter -- should receive it via parameter.

**Suggested improvement:**
Pass `DataFusionIOAdapter` instances through context objects consistently rather than constructing them ad hoc in leaf functions.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies in scope. `ViewNode` composes behavior via `builder` and `contract_builder` callables. `ScalarLiteralBase` uses tagged unions (discriminated by `tag_field`) rather than inheritance-based dispatch. `DatasetCatalog` extends `MutableRegistry` which is the single base class, and it adds only minimal registry-specific behavior.

**Suggested improvement:**
None needed. The scope consistently uses composition.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
Multiple deep property chain traversals violate the Law of Demeter.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registry.py:206-276`: `registry_snapshot()` contains deeply chained accesses like `resolved.delta_write_policy.parquet_writer_policy.statistics_enabled`, `resolved.delta_write_policy.parquet_writer_policy.bloom_filter_enabled`, etc. -- 6+ levels of attribute access.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:1149-1153`: `location.resolved.delta_schema_policy` followed by `getattr(policy, "schema_mode", None)` -- traversing through two intermediate objects.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:1248-1249`: `runtime_profile.features.enable_schema_evolution_adapter` -- 2-level chain used in many places.

**Suggested improvement:**
(1) Add a `to_snapshot_dict()` method on `ResolvedDatasetLocation` (or its sub-objects) so that `registry_snapshot()` delegates serialization rather than manually traversing nested objects. (2) Add an `allows_schema_evolution()` method on `DatasetLocation` that encapsulates the `resolved.delta_schema_policy.schema_mode` traversal.

**Effort:** medium
**Risk if unaddressed:** medium -- changes to the `DeltaWritePolicy` or `DeltaSchemaPolicy` structure will force updates throughout `registry_snapshot()` and registration code.

---

#### P15. Tell, don't ask -- Alignment: 1/3

**Current state:**
`registry_snapshot()` is the most prominent "ask" pattern -- it interrogates ~30 individual fields from `ResolvedDatasetLocation` and its nested objects to build a snapshot dictionary. This logic should be encapsulated on the data objects themselves.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registry.py:186-312`: `registry_snapshot()` is 127 lines of manual field extraction from `DatasetLocation`, `ResolvedDatasetLocation`, and nested policy objects. The function asks for data and reassembles it, rather than telling the objects to serialize themselves.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:1998-2016`: `_provider_pushdown_hints` inspects provider internals via `getattr` with multiple candidate attribute names -- reaching deep into provider implementation details.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:1955-1962`: `_table_provider_capsule` tries two different attribute names via `getattr` to extract a capsule -- protocol violation where the provider should declare its interface.

**Suggested improvement:**
(1) Add `to_snapshot_dict()` on `DatasetLocation` or `ResolvedDatasetLocation` to encapsulate serialization. (2) Define a `TableProviderProtocol` with a `capsule()` method instead of relying on `getattr` probing. (3) Define a `PushdownCapabilities` protocol for provider pushdown hints.

**Effort:** medium
**Risk if unaddressed:** medium -- fragile introspection via `getattr` breaks silently when provider APIs change.

---

### Category: Correctness

#### P16. Functional core, imperative shell -- Alignment: 1/3

**Current state:**
Observability recording (`record_artifact`) is deeply interleaved with core registration and view graph logic. This makes the core logic impure and harder to test without observability infrastructure.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py`: 6 calls to `record_artifact` scattered through core registration functions, plus 8 `_record_*` helper functions that are called inline during registration.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py`: 9 calls to `record_artifact` across validation, caching, and UDF audit functions that are interspersed with core view registration logic.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:392-421`: `_maybe_validate_schema_contract` catches `SchemaContractViolationError`, records an artifact, and silently returns -- mixing validation (pure) with artifact recording (side effect).

**Suggested improvement:**
Collect diagnostic events as a return value from the registration functions (e.g., a list of `DiagnosticEvent` dataclass instances) and let the caller (`register_view_graph`) flush them to `record_artifact` at the end. This separates the pure registration logic from the observability shell.

**Effort:** large
**Risk if unaddressed:** medium -- testing core registration logic requires mocking the observability system or accepting side effects.

---

#### P17. Idempotency -- Alignment: 2/3

**Current state:**
Registration operations support `overwrite=True` by default. The cache system checks for existing cache hits before writing. `_reset_resolver_identity_guard` is called at the start of each `register_view_graph` invocation.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:273`: `_reset_resolver_identity_guard()` ensures clean state for each batch.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:742-755`: Cache hit checking in `_register_delta_staging_cache` provides idempotent behavior -- re-running with the same plan identity hash returns the cached result.

**Suggested improvement:**
No critical gaps. Module-level caches (`_CACHED_DATASETS`, `_REGISTERED_CATALOGS`) lack explicit reset mechanisms, which could cause stale state across test runs.

**Effort:** small
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Determinism is a first-class concern throughout. Plan fingerprints, schema identity hashes, plan task signatures, and DDL fingerprints are computed and tracked. The topological sort uses `sorted()` for stable ordering. `_plan_task_signature` includes all relevant inputs (runtime hash, function registry hash, UDF snapshot hash, etc.).

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/artifacts.py:152-173`: `_plan_task_signature` includes 14 components for deterministic signature generation.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:1441`: `queue = deque(sorted(name for name, degree in indegree.items() if degree == 0))` -- stable initial ordering for topological sort.

**Suggested improvement:**
None needed. Determinism is well-addressed.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Simplicity

#### P19. KISS -- Alignment: 1/3

**Current state:**
`registration.py` at 3,350 lines with approximately 80 private functions is the dominant complexity hotspot. Many functions have high parameter counts and nested conditional logic.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py`: 3,350 LOC with ~80 private functions. A developer looking for DDL generation must scan past Delta provider logic, schema evolution, partition validation, and observability code.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:882-917`: `_external_table_ddl` returns a 5-tuple, making call sites hard to read and error-prone.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:701-885`: `_register_delta_staging_cache` at ~185 lines with 6+ levels of nesting and multiple try/except blocks.

**Suggested improvement:**
(1) Decompose `registration.py` as described in P2/P3. (2) Replace the 5-tuple return from `_external_table_ddl` with a `DdlComponents` dataclass. (3) Extract the try/except-heavy cache registration into a `CacheRegistrationPipeline` that uses a step-based pattern.

**Effort:** large
**Risk if unaddressed:** high -- cognitive load makes the module error-prone to modify.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
The code generally avoids speculative generality. The `_DatasetInputSource` plugin mechanism is actually wired and used. The expression IR supports exactly the operations needed by the pipeline. The domain planner system is minimal but extensible.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/expr/spec.py:17-34`: Many `_EXACT_*` and `_MIN_*` constants suggest the expression spec evolved incrementally to match real needs rather than being over-designed upfront.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/expr/domain_planner.py:55-63`: `DEFAULT_DOMAIN_PLANNERS` contains exactly one planner ("codeanatomy_domain") -- appropriately minimal.

**Suggested improvement:**
No critical gaps.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
API naming is generally clear. `register_view_graph`, `resolve_dataset_provider`, `build_view_artifact_from_bundle` all describe their behavior well.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:1223-1228`: `_normalize_cache_policy("memory")` silently maps to `"delta_staging"` -- this is documented in the `ViewNode` docstring (line 109) but is still potentially surprising behavior.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:121`: `from datafusion_engine.views.graph import arrow_schema_from_df` -- importing a function from `views/graph.py` that actually originates in `views/bundle_extraction.py` creates a confusing import chain.

**Suggested improvement:**
(1) Add a deprecation warning when `"memory"` cache policy is provided, directing callers to use `"delta_staging"` explicitly. (2) Import `arrow_schema_from_df` directly from its canonical source.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version contracts -- Alignment: 2/3

**Current state:**
All modules declare `__all__` exports. `ViewNode`, `ExprSpec`, `ExprIR`, `DatasetLocation`, and `DatasetCatalog` are stable public types. The views `__init__.py` includes a `__getattr__` deprecation notice for removed symbols.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/__init__.py:10-18`: `__getattr__` properly raises `DeprecationWarning` for `VIEW_SELECT_REGISTRY` -- good contract evolution practice.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registry.py:721-735`: `__getattr__` provides lazy re-exports for moved types (`DataFusionScanOptions`, `DeltaWritePolicy`, etc.) -- good backward compatibility.

**Suggested improvement:**
No critical gaps.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
Module-level mutable state is the primary testability concern. The three caches in `registration.py`, the location resolution cache in `registry.py`, and the resolver identity guard in `graph.py` all create implicit global state that makes isolated unit testing difficult.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:142-144`: `_CACHED_DATASETS`, `_REGISTERED_CATALOGS`, `_REGISTERED_SCHEMAS` -- module-level mutable dicts with no reset mechanism for tests.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registry.py:122`: `_RESOLVED_LOCATION_CACHE` keyed by `id(location)` -- persists across test cases and may cause cross-test pollution.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:60`: `_resolver_identity_guard` -- singleton state that persists across test calls.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:701-885`: `_register_delta_staging_cache` at 185 lines with 15 deferred imports -- difficult to test without a full DataFusion session, Delta Lake, and observability stack.

**Suggested improvement:**
(1) Wrap module-level caches in a `RegistrationStateManager` class that can be instantiated per-test. (2) Add public `clear_registration_caches()` and `clear_location_cache()` functions for test fixtures. (3) Extract pure logic (DDL building, payload construction, partition validation) into functions that can be tested without SessionContext.

**Effort:** medium
**Risk if unaddressed:** high -- test isolation is compromised, increasing the risk of flaky tests and making TDD difficult for registration logic.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Observability is comprehensive and structured. Every registration path records artifacts with typed payloads via `record_artifact`. Cache operations use OpenTelemetry spans (`cache_span`). Delta log health, provider mode diagnostics, and schema divergence are all tracked.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:732-741`: `cache_span("cache.view.delta_staging.read", ...)` provides structured span naming with attributes.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/dataset/registration.py:1836-1855`: `_delta_log_health_payload` includes diagnostic severity and category -- good structured observability.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/graph.py:577-585`: `_record_udf_audit` records UDF audit payloads -- comprehensive function-level tracing.

**Suggested improvement:**
As noted in P16, decouple observability from core logic by collecting diagnostic events as data and flushing them at the shell. This would make the observability layer itself more testable.

**Effort:** medium
**Risk if unaddressed:** low -- the current approach works but makes the core harder to test.

---

## Cross-Cutting Themes

### Theme 1: registration.py monolith (affects P2, P3, P4, P19, P23)

**Description:** `registration.py` at 3,350 lines is the single largest file in the reviewed scope and serves as a "God module" for dataset registration. It handles DDL generation, Delta provider management, caching, observability, schema evolution, projection, and partition validation -- at least six independent reasons to change.

**Root cause:** Organic growth of registration capabilities without periodic decomposition. Each new feature (schema evolution adapters, partition validation, projection scan overrides) was added to the existing module rather than extracted.

**Affected principles:** P2 (separation of concerns), P3 (SRP), P4 (cohesion/coupling), P19 (KISS), P23 (testability).

**Suggested approach:** Extract DDL generation, artifact recording, schema evolution, and partition validation into separate modules. This preserves the existing public API (`register_dataset_df`, `apply_projection_overrides`) while reducing the core module to ~2,000 lines focused on registration orchestration.

### Theme 2: Duplicated schema extraction logic (affects P7, P4)

**Description:** The `arrow_schema_from_df` function exists in 4 nearly-identical implementations across `bundle_extraction.py`, `bundle_artifact.py`, `registration.py`, and `registry_specs.py`. Each handles the same `df.schema()` -> `pa.Schema` coercion with the same `getattr(schema, "to_arrow", None)` fallback pattern.

**Root cause:** Different modules needed the same utility at different times and implemented it locally rather than importing from a shared location.

**Affected principles:** P7 (DRY), P4 (coupling).

**Suggested approach:** Consolidate into `datafusion_engine/arrow/schema_utils.py` (or add to the existing `datafusion_engine/arrow/interop.py`) and update all callers to import from the canonical location.

### Theme 3: Observability interleaved with core logic (affects P16, P23, P2)

**Description:** `record_artifact` calls and artifact payload construction functions are deeply interspersed with core registration and view graph logic. This makes the pure registration behavior harder to isolate, test, and reason about.

**Root cause:** The observability system was instrumented incrementally alongside feature development rather than being designed as a separate layer.

**Affected principles:** P16 (functional core), P23 (testability), P2 (separation of concerns).

**Suggested approach:** Adopt an "event collector" pattern: registration functions return both their primary result and a list of diagnostic events. The orchestration layer (`register_view_graph`, `register_dataset_df`) flushes events to the observability system at the end. This separates the pure registration logic from the IO-performing observability shell.

### Theme 4: Module-level mutable state (affects P1, P23)

**Description:** Five module-level mutable dicts (`_CACHED_DATASETS`, `_REGISTERED_CATALOGS`, `_REGISTERED_SCHEMAS`, `_RESOLVED_LOCATION_CACHE`, `_resolver_identity_guard`) create implicit global state that persists across test cases and pipeline runs.

**Root cause:** Caching and tracking state was added incrementally without a unified state management pattern.

**Affected principles:** P1 (information hiding), P23 (testability).

**Suggested approach:** Wrap these caches in explicit scope-managed objects passed through context parameters. Provide `clear_*` functions for test fixtures as an immediate mitigation.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 | Consolidate 4 `arrow_schema_from_df` implementations into one canonical location | small | Eliminates divergence risk across 4 files |
| 2 | P23 | Add `clear_registration_caches()` and `clear_location_cache()` functions for test fixtures | small | Immediate test isolation improvement |
| 3 | P10 | Restrict `ExprIR.op` to `Literal["field", "literal", "call"]` with `__post_init__` validation | small | Fail-fast on invalid expression ops |
| 4 | P21 | Import `arrow_schema_from_df` directly from `bundle_extraction.py` instead of via `views/graph.py` re-export | small | Clearer import chain |
| 5 | P19 | Replace `_external_table_ddl` 5-tuple return with a `DdlComponents` dataclass | small | Improved readability at call sites |

## Recommended Action Sequence

1. **Consolidate `arrow_schema_from_df`** (P7) -- Create a single canonical implementation in `datafusion_engine/arrow/schema_utils.py` and update all 4+ call sites. This is a low-risk, high-signal improvement that eliminates duplicated knowledge.

2. **Add test fixture cache-clearing functions** (P23) -- Expose `clear_registration_caches()`, `clear_location_resolution_cache()`, and `reset_resolver_identity_guard()` as public functions. This unblocks more thorough unit testing.

3. **Tighten `ExprIR.op` and `CachePolicy` types** (P10) -- Add `__post_init__` validation for `ExprIR.op` and convert `CachePolicy` to `StrEnum`. Quick wins that prevent invalid states.

4. **Extract DDL generation from registration.py** (P2, P3, P19) -- Move `_ddl_*`, `_sql_type_name`, `_external_table_ddl`, and related functions to `dataset/ddl.py`. This is the least-coupled extraction and reduces `registration.py` by ~240 lines.

5. **Extract artifact recording from registration.py** (P2, P16) -- Move all `_record_*` functions and their associated dataclasses to `dataset/registration_artifacts.py`. This reduces `registration.py` by another ~400 lines and begins separating observability from core logic.

6. **Refactor delta cache registration in graph.py** (P7, P19) -- Extract the shared pattern from `_register_delta_staging_cache` and `_register_delta_output_cache` into a parameterized helper. This eliminates ~130 lines of near-duplicate code.

7. **Add serialization methods to domain objects** (P14, P15) -- Add `to_snapshot_dict()` on `ResolvedDatasetLocation` and `DataFusionScanOptions` to replace the manual field extraction in `registry_snapshot()`.

8. **Adopt event collector pattern for observability** (P16, P23) -- Refactor `register_view_graph` and `_register_view_node` to collect diagnostic events as data and flush them at the shell. This is the largest change but provides the most testability improvement.
