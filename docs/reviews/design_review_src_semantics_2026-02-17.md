# Design Review: src/semantics

**Date:** 2026-02-17
**Scope:** `src/semantics/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 87 (23,772 LOC)

## Executive Summary

The `src/semantics/` module is the compiler and pipeline backbone of the CPG builder. It demonstrates strong architectural alignment overall: clean IR compile/infer/optimize/emit pipeline, well-designed type system with semantic annotations, declarative specs for normalization and relationships, and solid inference-driven join strategy selection. Key weaknesses center on (1) `pipeline_build.py` serving as an oversized orchestration module with multiple reasons to change, (2) significant coupling to `datafusion_engine` internals that violates the intended dependency direction, and (3) scattered join-key references in diagnostics and signals modules that partially undermine the centralized join inference.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `SemanticCompiler.ctx` is public; `_tables` dict accessed externally via `get_or_register` |
| 2 | Separation of concerns | 2 | medium | medium | `pipeline_build.py` mixes orchestration, CDF resolution, materialization, and diagnostics |
| 3 | SRP | 1 | medium | medium | `pipeline_build.py` has 5+ reasons to change; acknowledged in size-exception comment |
| 4 | High cohesion, low coupling | 2 | medium | medium | Pipeline builders tightly coupled to `_SemanticSpecContext` mega-struct |
| 5 | Dependency direction | 1 | large | high | 30+ imports from `datafusion_engine` across the module; middle ring depends on engine ring |
| 6 | Ports & Adapters | 1 | large | medium | No port abstractions; `datafusion_engine` types used directly in core semantics |
| 7 | DRY | 2 | small | low | `_resolve_join_keys` filtering logic duplicated in `ir_pipeline.py:_resolve_keys_from_inferred` |
| 8 | Design by contract | 3 | - | - | Strong explicit preconditions via `require_*` methods; `SemanticSchemaError` raised early |
| 9 | Parse, don't validate | 3 | - | - | `AnnotatedSchema.from_dataframe/from_arrow_schema` parses once at boundary |
| 10 | Illegal states | 2 | small | low | `SemanticIRView.kind` is a string literal union; `RelationOptions` allows contradictory `join_type` + `strategy_hint` |
| 11 | CQS | 3 | - | - | Compiler methods are pure transforms returning DataFrames; `register` is the only mutation |
| 12 | DI + explicit composition | 2 | medium | medium | `SemanticCompiler` receives `ctx` via constructor DI; but module-level singletons (`SEMANTIC_MODEL`) created at import time |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; all behavior via composition and delegation |
| 14 | Law of Demeter | 2 | small | low | `runtime_profile.data_sources.semantic_output.cache_overrides` chain in `pipeline_build.py:378` |
| 15 | Tell, don't ask | 2 | small | low | `pipeline_build.py` interrogates `runtime_profile.features.enable_delta_cdf` to decide behavior |
| 16 | Functional core | 2 | medium | medium | IR pipeline is nicely functional; but `pipeline_build.py` mixes IO (materialization) with compile logic |
| 17 | Idempotency | 3 | - | - | Compiler operations are stateless transforms; `register` is idempotent for same table |
| 18 | Determinism | 3 | - | - | Fingerprints (`model_hash`, `ir_hash`) and canonical hashing ensure reproducibility |
| 19 | KISS | 2 | small | low | `_SemanticSpecContext` dataclass has 10 fields; some builders require complex context |
| 20 | YAGNI | 2 | small | low | `RelationOptions.use_cdf` and CDF incremental join machinery adds complexity for an incomplete feature |
| 21 | Least astonishment | 2 | small | low | `__all__` exports include private names (`_CONSOLIDATED_BUILDER_HANDLERS`, `_dispatch_from_registry`) |
| 22 | Public contracts | 2 | small | low | `__all__` declared on all modules; but private names leaked through `pipeline_build.py` |
| 23 | Testability | 2 | medium | medium | Pure IR pipeline is highly testable; `pipeline_build.py` requires full runtime profile for testing |
| 24 | Observability | 3 | - | - | `stage_span` consistently applied with structured attributes on all compiler operations |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
`SemanticCompiler` exposes `self.ctx` as a public attribute (`compiler.py:195`). The `_tables` registry is nominally private but `get_or_register` provides open access to registered `TableInfo` objects, which expose `df`, `sem`, and `annotated` fields directly.

**Findings:**
- `compiler.py:195`: `self.ctx = ctx` -- public attribute exposes raw DataFusion session
- `compiler.py:108-111`: `TableInfo` exposes `df`, `sem`, `annotated` without encapsulation
- `compiler.py:256-259`: `schema_names` method delegates to static with `_ = self`, suggesting it should be static or removed

**Suggested improvement:**
Make `ctx` a private attribute `_ctx`. Provide only the `register` and `get_or_register` methods as the public interface, rather than exposing raw `TableInfo` internals. The `schema_names` and `prefix_df` instance methods that ignore `self` should be made static or removed from the public API.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
`pipeline_build.py` (754 LOC) mixes multiple concerns: semantic IR compilation, CDF input resolution, cache policy hierarchy, incremental output calculation, view node construction, materialization, and diagnostic emission. The module itself acknowledges this with a `NOTE(size-exception)` comment at line 11.

**Findings:**
- `pipeline_build.py:649-682`: `_resolve_semantic_input_mapping` handles CDF input resolution, mixing IO concerns
- `pipeline_build.py:685-712`: `_resolve_cdf_inputs` performs DataFusion registration side effects
- `pipeline_build.py:491-509`: `_execute_cpg_build` calls `materialize_semantic_outputs` and `emit_semantic_quality_diagnostics` -- IO effects mixed with build logic
- `pipeline_build.py:347-355`: Cache policy resolution is delegated cleanly to `pipeline_cache`

**Suggested improvement:**
Extract CDF resolution (`_resolve_cdf_inputs`, `_resolve_semantic_input_mapping`, `_has_cdf_inputs`) into a dedicated `semantics/cdf_resolution.py` module (already partially done). Move materialization orchestration into `output_materialization.py`. Keep `pipeline_build.py` as a thin orchestration entry point that wires together compile, materialize, and diagnostics phases.

**Effort:** medium
**Risk if unaddressed:** medium -- makes the module harder to test and reason about as features grow

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`pipeline_build.py` changes for at least five distinct reasons: (1) build orchestration logic, (2) CDF input resolution policy, (3) cache policy hierarchy, (4) materialization format changes, (5) diagnostic emission. The acknowledged `NOTE(size-exception)` at line 11 confirms this is a known debt.

**Findings:**
- `pipeline_build.py:11`: `NOTE(size-exception)` -- acknowledged as temporarily oversized
- `pipeline_build.py:86-124`: `CpgBuildOptions` bundles 8 unrelated configuration concerns
- `pipeline_build.py:520-572`: `build_cpg` entry point orchestrates all phases in one function

**Suggested improvement:**
Decompose `CpgBuildOptions` into focused option groups (e.g., `CacheOptions`, `CdfOptions`, `MaterializationOptions`). Extract CDF resolution, materialization, and diagnostics into separate modules as outlined in the existing implementation plan. Target `pipeline_build.py` as a thin facade under 200 LOC.

**Effort:** medium
**Risk if unaddressed:** medium -- growing module becomes a bottleneck for independent feature development

---

#### P5. Dependency direction -- Alignment: 1/3

**Current state:**
`src/semantics/` is conceptually "middle ring" (domain logic), but it imports heavily from `datafusion_engine/` (engine ring). Over 30 import sites pull in DataFusion session types, UDF helpers, schema contracts, write pipelines, and lineage extraction.

**Findings:**
- `compiler.py:201-202`: `from datafusion_engine.udf.extension_core import rust_udf_snapshot, validate_required_udfs` -- core compiler depends on engine UDF internals
- `output_materialization.py:9-10`: `from datafusion_engine.delta.schema_guard import SchemaEvolutionPolicy` and `from datafusion_engine.io.write_core import WritePipeline` -- direct engine IO dependency
- `ir_pipeline.py:619`: `from datafusion_engine.extract.registry import dataset_schema` -- IR compilation depends on engine extraction registry
- `pipeline_build.py:21-22`: Multiple engine-ring imports at module level
- `validation/policy.py:8`: `from datafusion_engine.schema import validate_semantic_types` -- validation depends on engine

**Suggested improvement:**
Define port protocols in `semantics/ports.py` for: (1) `UdfResolver` protocol for UDF validation, (2) `SchemaProvider` protocol for dataset schema lookup, (3) `OutputWriter` protocol for materialization. Inject these via `SemanticExecutionContext` rather than importing engine internals directly. This preserves the architectural intent while allowing engine swaps.

**Effort:** large
**Risk if unaddressed:** high -- prevents independent evolution and testing of semantic logic without engine dependencies

---

#### P6. Ports & Adapters -- Alignment: 1/3

**Current state:**
No formal port abstractions exist between semantics and datafusion_engine. The semantic module imports concrete engine types directly. The only protocol-like patterns are `TaskGraphLike` and `ScanOverrideLike` in `relspec/contracts.py`, which partially decouple the policy compiler.

**Findings:**
- `compiler.py:51-52`: `from datafusion import DataFrame, SessionContext` -- concrete dependency, not protocol
- `expr_builder.py:53`: `from datafusion_engine.udf.expr import udf_expr` -- concrete UDF dependency
- `scip_normalize.py:12-15`: Four direct engine imports for schema, introspection, UDF operations
- Contrast with `relspec/contracts.py:41-47`: `TaskGraphLike` and `OutDegreeGraph` protocols show the pattern is known but not applied in semantics

**Suggested improvement:**
Follow the `relspec/contracts.py` pattern to define `DataFrameProvider`, `UdfExprFactory`, and `SchemaLookup` protocols in `semantics/ports.py`. The `SemanticCompiler` constructor should accept protocol-typed collaborators rather than concrete `SessionContext`.

**Effort:** large
**Risk if unaddressed:** medium -- coupling prevents testing compiler logic without full DataFusion runtime

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 2/3

**Current state:**
The FILE_IDENTITY join-key filtering logic is duplicated between `compiler.py:_resolve_join_keys` (lines 321-373) and `ir_pipeline.py:_resolve_keys_from_inferred` (lines 179-215). Both implement the same semantic rule: filter to exact-name pairs from the FILE_IDENTITY compatibility group.

**Findings:**
- `compiler.py:343-354`: FILE_IDENTITY filtering with `left_col_name != right_col_name` check and `CompatibilityGroup.FILE_IDENTITY` membership test
- `ir_pipeline.py:206-214`: Parallel implementation using `_FILE_IDENTITY_NAMES` frozenset with same-name check
- `ir_pipeline.py:967`: `_FILE_IDENTITY_NAMES: frozenset[str] = frozenset({"file_id", "path"})` -- hardcoded knowledge of file identity column names

**Suggested improvement:**
Extract the FILE_IDENTITY join-key resolution into a shared utility in `semantics/types/core.py` or `semantics/joins/inference.py`. Both the compiler and IR pipeline should call this single authority. The `_FILE_IDENTITY_NAMES` constant should be derived from the `CompatibilityGroup.FILE_IDENTITY` definition.

**Effort:** small
**Risk if unaddressed:** low -- but drift between the two implementations could cause subtle join-key mismatches

---

#### P8. Design by contract -- Alignment: 3/3

**Current state:**
The compiler has excellent explicit preconditions via `require_*` methods. `SemanticSchema.require_entity`, `require_symbol_source`, `require_unambiguous_spans`, and `require_span_unit` enforce preconditions with clear error messages. `SemanticSchemaError` provides domain-specific error types.

**Findings:**
- `compiler.py:750-751`: `sem.require_unambiguous_spans(table=table_name)` and `sem.require_evidence(table=table_name)` -- explicit preconditions
- `compiler.py:951-952`: `left_info.sem.require_entity(table=left_table)` and `right_info.sem.require_symbol_source(table=right_table)` -- relationship preconditions
- `compiler.py:275-284`: `_ensure_columns_present` validates required columns with descriptive error

No action needed.

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
`RelationOptions` allows contradictory configuration: `join_type` and `strategy_hint` can both be set, potentially conflicting. The `SemanticIRView.kind` field uses a string literal union rather than an enum, making it possible to construct views with invalid kinds at runtime.

**Findings:**
- `compiler.py:160-166`: `RelationOptions` allows both `join_type` and `strategy_hint` simultaneously; `_relation_hint` at line 853 resolves the conflict silently
- `ir.py:107-114`: `SemanticIRView` uses `ViewKindStr` (string literal union) rather than an enum

**Suggested improvement:**
Consider making `join_type` and `strategy_hint` mutually exclusive in `RelationOptions`, or replace both with a single `strategy: JoinStrategyType | Literal["infer"]` field. The `ViewKindStr` pattern is acceptable for serialization but consider adding runtime validation in `SemanticIRView.__post_init__`.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 2/3

**Current state:**
`SemanticCompiler` receives `SessionContext` via constructor injection, which is good. However, `SEMANTIC_MODEL` is constructed at module import time via `build_semantic_model()` in `registry.py:419`, creating a global singleton. This makes it difficult to test with alternative model configurations.

**Findings:**
- `compiler.py:185-198`: `SemanticCompiler.__init__` uses constructor injection for `ctx` and `config`
- `registry.py:419`: `SEMANTIC_MODEL: SemanticModel = build_semantic_model()` -- import-time singleton
- `ir_pipeline.py:1344`: `resolved_model = SEMANTIC_MODEL if model is None else model` -- fallback to singleton
- `pipeline_builders.py:94-97`: Closures capture `config` and `table` via DI in builder factories

**Suggested improvement:**
Make `SEMANTIC_MODEL` a lazy property or factory function rather than a module-level constant. This allows test isolation without monkeypatching and supports multi-model scenarios.

**Effort:** medium
**Risk if unaddressed:** medium -- test contamination and inability to run isolated model experiments

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
`pipeline_build.py` reaches deep into runtime profile internals to access cache overrides and CDF settings.

**Findings:**
- `pipeline_build.py:378`: `request.runtime_profile.data_sources.semantic_output.cache_overrides` -- 4-level chain
- `pipeline_build.py:551`: `runtime_profile.features.enable_delta_cdf` -- 2-level chain (acceptable)
- `ir_pipeline.py:1308`: `except (AttributeError, KeyError, TypeError, ValueError)` -- broad exception catching suggests fragile data access

**Suggested improvement:**
Add helper methods on `DataFusionRuntimeProfile` like `semantic_cache_overrides()` and `cdf_enabled()` to encapsulate the traversal. The `pipeline_build.py` code should call these methods instead of traversing the object graph.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The IR pipeline (`ir_pipeline.py`) is an excellent example of functional core: `compile_semantics -> infer_semantics -> optimize_semantics -> emit_semantics` is a pure transformation chain. However, `pipeline_build.py` mixes IO (Delta writes, view registration) with compilation logic in the same control flow.

**Findings:**
- `ir_pipeline.py:1329-1348`: `build_semantic_ir()` is a clean pure function composition
- `pipeline_build.py:405-517`: `_execute_cpg_build` interleaves pure compile steps with IO: view graph registration (467), materialization (491), and diagnostics emission (500)
- `compiler.py:623-720`: `normalize_from_spec` is a pure transform -- good

**Suggested improvement:**
Separate `_execute_cpg_build` into a pure compile phase (returns build result + plan) and an imperative execute phase (performs registration, materialization, diagnostics). The pure phase can be tested without IO fixtures.

**Effort:** medium
**Risk if unaddressed:** medium -- impedes pure-logic testing of build orchestration

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Compiler operations are stateless transforms on DataFrames. The `register` method is idempotent (re-analyzing same table produces same `TableInfo`). Hashing and fingerprinting ensure cache correctness.

No action needed.

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Excellent determinism design. `model_hash` and `ir_hash` on `SemanticIR`, `semantic_model_fingerprint()`, `semantic_ir_fingerprint()`, and `_compute_policy_fingerprint()` all use canonical hashing. `ExecutionPackageArtifact` captures composite fingerprint for full reproducibility.

No action needed.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The `_SemanticSpecContext` dataclass has 10 fields, creating a wide context object. Builder factories use closure capture patterns that are effective but sometimes create deep call chains.

**Findings:**
- `pipeline_builders.py:388-399`: `_SemanticSpecContext` has 10 fields, some only used by specific builder kinds
- `pipeline_builders.py:671-680`: `_CONSOLIDATED_BUILDER_HANDLERS` dispatch table adds indirection but consolidates well

**Suggested improvement:**
Consider splitting `_SemanticSpecContext` into `NormalizationContext`, `RelationshipContext`, and `OutputContext`, each carrying only the fields needed by their respective builder kinds.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
The `__all__` export in `pipeline_build.py` includes private names prefixed with underscore, which is surprising for a public contract.

**Findings:**
- `pipeline_build.py:736-753`: `__all__` includes `"_CONSOLIDATED_BUILDER_HANDLERS"`, `"_builder_for_project_kind"`, `"_diagnostic_registry"`, `"_dispatch_from_registry"`, `"_finalize_df_to_contract"`, `"_span_unnest_registry"`, `"_symtable_registry"` -- all underscore-prefixed
- `pipeline_cache.py:104-111`: `__all__` also exports `"_cache_policy_for"`, `"_normalize_cache_policy"`, `"_resolve_cache_policy_hierarchy"` alongside their public wrappers

**Suggested improvement:**
Remove private names from `__all__` exports. If external modules need access, rename them to public names or provide public wrappers (some wrappers already exist in `pipeline_cache.py`).

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
The IR pipeline is highly testable: `compile_semantics`, `infer_semantics`, `optimize_semantics`, and `emit_semantics` are pure functions accepting `SemanticModel` and `SemanticIR`. However, `pipeline_build.py` requires `DataFusionRuntimeProfile`, `SessionContext`, and full execution contexts, making integration testing expensive.

**Findings:**
- `ir_pipeline.py:660-881`: `compile_semantics` is purely testable with model input
- `pipeline_build.py:520-526`: `build_cpg` requires `SessionContext`, `DataFusionRuntimeProfile`, and `SemanticExecutionContext`
- `compile_context.py:55-115`: `build_semantic_execution_context` imports from 4 engine modules

**Suggested improvement:**
Introduce a `SemanticBuildPlan` data class that `build_cpg` produces as a pure compilation result. A separate `execute_build_plan` function performs the IO. This allows testing the build plan logic without engine fixtures.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P24. Observability -- Alignment: 3/3

**Current state:**
Comprehensive `stage_span` instrumentation across all compiler operations with structured attributes (`codeanatomy.table_name`, `codeanatomy.join_type`, etc.). Logging uses structured extras for inference confidence. `SCOPE_SEMANTICS` provides consistent scope naming.

No action needed.

---

## Cross-Cutting Themes

### Theme 1: Engine Ring Coupling

The most systemic issue is the tight coupling between `src/semantics/` (middle ring) and `src/datafusion_engine/` (engine ring). Over 30 import sites create a dependency direction violation. This manifests across P5, P6, P12, and P23. The root cause is the absence of port/adapter abstractions between the semantic domain and the DataFusion execution engine.

**Affected principles:** 5, 6, 12, 23
**Suggested approach:** Define port protocols in `semantics/ports.py` and inject engine adapters via `SemanticExecutionContext`. Start with `SchemaProvider` and `UdfResolver` as the highest-value ports.

### Theme 2: pipeline_build.py Overload

`pipeline_build.py` is acknowledged as oversized and serves as a mixing point for orchestration, CDF resolution, materialization, and diagnostics. This creates a single point of contention for changes and makes isolated testing difficult.

**Affected principles:** 2, 3, 16, 23
**Suggested approach:** Follow the existing implementation plan to decompose into focused modules. Prioritize extracting materialization and CDF resolution first.

### Theme 3: Knowledge Duplication in Join Resolution

The FILE_IDENTITY join-key filtering logic is encoded in two places (`compiler.py` and `ir_pipeline.py`), and `_FILE_IDENTITY_NAMES` is a hardcoded constant rather than derived from the type system. This creates a drift risk.

**Affected principles:** 7
**Suggested approach:** Centralize in `semantics/types/core.py` and derive from `CompatibilityGroup.FILE_IDENTITY` column declarations.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P21 | Remove private names from `__all__` exports in `pipeline_build.py` and `pipeline_cache.py` | small | Clarifies public API surface |
| 2 | P1 | Make `SemanticCompiler.ctx` private (`_ctx`) | small | Prevents accidental external access |
| 3 | P7 | Extract shared FILE_IDENTITY join-key filtering into `types/core.py` | small | Eliminates knowledge duplication |
| 4 | P14 | Add `semantic_cache_overrides()` helper to RuntimeProfile | small | Reduces Demeter violations |
| 5 | P10 | Make `join_type` and `strategy_hint` mutually exclusive in `RelationOptions` | small | Prevents contradictory config |

## Recommended Action Sequence

1. **P21/P22** -- Clean `__all__` exports (removes private names from public surface). Zero-risk, immediate clarity gain.
2. **P7** -- Centralize FILE_IDENTITY join-key filtering. Prevents drift between compiler and IR pipeline.
3. **P1** -- Make `SemanticCompiler.ctx` private. Small change, cleaner encapsulation.
4. **P3/P2** -- Decompose `pipeline_build.py` per existing plan. Extract CDF resolution and materialization modules.
5. **P5/P6** -- Define port protocols for `SchemaProvider` and `UdfResolver`. Largest effort but highest architectural value.
6. **P16/P23** -- Introduce `SemanticBuildPlan` to separate pure compilation from IO execution.
