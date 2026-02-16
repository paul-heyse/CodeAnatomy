# Design Review: src/semantics/ Core Pipeline & IR

**Date:** 2026-02-16
**Scope:** `src/semantics/{pipeline.py, compiler.py, ir_pipeline.py, exprs.py, ir.py, ir_optimize.py}`
**Focus:** All principles (1-24), with special attention to SRP, Information Hiding, CQS, Dependency Direction, and Interface Segregation
**Depth:** deep
**Files reviewed:** 6 (approximately 6,831 LOC)

## Executive Summary

The Core Pipeline & IR subsystem has a strong conceptual architecture: a clean compile/infer/optimize/emit IR pipeline (`ir_pipeline.py`, `ir.py`, `ir_optimize.py`) and a well-designed expression DSL (`exprs.py`). However, the two largest files -- `pipeline.py` (2,256 LOC) and `compiler.py` (1,818 LOC) -- have accumulated multiple responsibilities that erode SRP and information hiding. `pipeline.py` conflates view-graph orchestration, output materialization, CDF resolution, and diagnostics emission into a single file. The compiler mixes pure semantic rule application with mutable table caching. A key information-hiding violation exists where `_cpg_view_specs` (a private function) is imported by `datafusion_engine/views/registry_specs.py`, creating a hidden cross-module coupling. The module-level singleton `SEMANTIC_MODEL` is accessed directly by both `pipeline.py` and `ir_pipeline.py`, coupling both to a global mutable binding rather than receiving the model via injection.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | high | `_cpg_view_specs` exported to external module; `SEMANTIC_MODEL` globally coupled |
| 2 | Separation of concerns | 1 | large | medium | `pipeline.py` mixes orchestration, CDF, materialization, diagnostics |
| 3 | SRP | 1 | large | medium | `pipeline.py` has 6+ reasons to change; `compiler.py` has 3+ |
| 4 | High cohesion, low coupling | 2 | medium | medium | CDF logic in pipeline.py is unrelated to view graph construction |
| 5 | Dependency direction | 2 | medium | medium | `pipeline.py` imports from `ir_pipeline.py` for incremental output resolution |
| 6 | Ports & Adapters | 2 | medium | low | No explicit port for Delta/storage; inline lazy imports serve as implicit adapter |
| 7 | DRY | 1 | medium | medium | `build_cpg` and `build_cpg_from_inferred_deps` duplicate compile-resolution and view-node construction |
| 8 | Design by contract | 2 | small | low | `SemanticIR` and IR data are frozen dataclasses with clear contracts; but `pipeline.py` builder closures lack contracts |
| 9 | Parse, don't validate | 2 | small | low | `CpgBuildOptions` parses early; but cache policy normalized late in `_view_nodes_for_cpg` |
| 10 | Make illegal states unrepresentable | 2 | small | low | `InferredViewProperties` uses optionals for graceful degradation; `GraphPosition` is well-typed |
| 11 | CQS | 1 | medium | medium | `SemanticCompiler.register()` mutates `_tables` and returns `TableInfo`; `_SemanticDiagnosticsContext` methods mutate and return |
| 12 | Dependency inversion | 1 | medium | medium | `SEMANTIC_MODEL` singleton hard-wired into `ir_pipeline` and `pipeline`; no injection point |
| 13 | Prefer composition over inheritance | 3 | - | low | No inheritance hierarchies; compiler uses composition throughout |
| 14 | Law of Demeter | 2 | small | low | `runtime_profile.data_sources.cdf_cursor_store` chain in `_cdf_changed_inputs` |
| 15 | Tell, don't ask | 2 | small | low | `_compile_relationship_with_quality_inner` retrieves state step-by-step but encapsulates logic well |
| 16 | Functional core, imperative shell | 2 | medium | low | IR pipeline is purely functional; compiler has mutable `_tables` cache |
| 17 | Idempotency | 3 | - | low | IR pipeline functions are idempotent; compiler re-registration overwrites safely |
| 18 | Determinism / reproducibility | 3 | - | low | Fingerprinting via `hash_msgpack_canonical`; nondeterminism validation in `validate_expr_spec` |
| 19 | KISS | 2 | medium | low | Expression DSL is clean; pipeline.py builder-dispatch is over-layered |
| 20 | YAGNI | 2 | small | low | `emit_semantics` is a placeholder with minimal logic; `_builder_for_artifact_spec` always raises |
| 21 | Least astonishment | 1 | small | medium | `_cpg_view_specs` begins with underscore but is part of external API |
| 22 | Declare public contracts | 1 | medium | medium | `__all__` lists are present but `_cpg_view_specs` and `CpgViewSpecsRequest` leak without being in `__all__` |
| 23 | Design for testability | 2 | medium | medium | Compiler requires `SessionContext`; IR pipeline uses global `SEMANTIC_MODEL` |
| 24 | Observability | 3 | - | low | Consistent `stage_span` tracing on all compiler operations; structured attributes |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
The most significant information-hiding violation is the cross-module import of a private function. `_cpg_view_specs` (prefixed with underscore, indicating private intent) is imported and called from `src/datafusion_engine/views/registry_specs.py:438`:

```python
from semantics.pipeline import CpgViewSpecsRequest, _cpg_view_specs
```

This means an external module depends on an internal implementation detail of `pipeline.py`. Additionally, `CpgViewSpecsRequest` is not listed in `pipeline.py`'s `__all__` (line 2249-2256) but is imported externally.

The module-level singleton `SEMANTIC_MODEL` from `semantics.registry` is accessed as a global in both `pipeline.py:49` and `ir_pipeline.py:27`, leaking the model's internal structure across both files. Both modules directly access `.relationship_specs`, `.outputs`, `.normalization_specs`, and `.normalization_output_names()` on the global, spreading knowledge of the model's shape.

**Findings:**
- `src/semantics/pipeline.py:778` -- `_cpg_view_specs` is private but imported externally by `src/datafusion_engine/views/registry_specs.py:438`
- `src/semantics/pipeline.py:2249-2256` -- `__all__` omits `CpgViewSpecsRequest` and `CpgViewNodesRequest`, yet both are used externally
- `src/semantics/pipeline.py:49` and `src/semantics/ir_pipeline.py:27` -- `SEMANTIC_MODEL` accessed as global singleton, scattering model-shape knowledge
- `src/semantics/compiler.py:210` -- `self.ctx` is a public attribute on `SemanticCompiler`, exposing the raw `SessionContext`

**Suggested improvement:**
Promote `_cpg_view_specs` to a public function with a stable name (e.g., `cpg_view_specs`) and add it to `__all__`. Add `CpgViewSpecsRequest` and `CpgViewNodesRequest` to `__all__` as well. Consider making `SemanticCompiler.ctx` private (`_ctx`) and exposing only the operations callers need.

**Effort:** medium
**Risk if unaddressed:** high -- External code depending on underscored functions will break silently if the function is renamed or removed during a refactor.

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`pipeline.py` is a single 2,256-line file that handles at least six distinct concerns:

1. **View-graph orchestration** (lines 778-1267): `_cpg_view_specs`, `_view_nodes_for_cpg`, `_semantic_view_specs`, and the builder-dispatch table
2. **CDF (Change Data Feed) resolution** (lines 649-776): `_cdf_changed_inputs`, `_resolve_cdf_location`, `_input_has_cdf_changes`, `_cdf_enabled_for_location`, `_outputs_from_changed_inputs`, `_views_downstream_of_inputs`
3. **Output materialization** (lines 1635-1849): `_materialize_semantic_outputs`, `_write_semantic_output`, `_semantic_output_locations`, `_ensure_semantic_output_locations`
4. **Diagnostics emission** (lines 1851-2050): `_SemanticDiagnosticsContext`, `_emit_semantic_quality_view`, `_emit_semantic_quality_views`, `_emit_semantic_quality_diagnostics`
5. **Builder factories** (lines 336-583): Eleven `_*_builder` factory functions
6. **Cache policy resolution** (lines 268-288, 1193-1224): `_cache_policy_for`, `_normalize_cache_policy`, `_resolve_cache_policy_hierarchy`

These concerns change for different reasons: CDF logic changes when the incremental strategy changes; materialization changes when storage formats evolve; diagnostics changes when quality metrics are added.

**Findings:**
- `src/semantics/pipeline.py:649-776` -- 127 lines of CDF resolution logic unrelated to CPG pipeline orchestration
- `src/semantics/pipeline.py:1635-1849` -- 214 lines of Delta-write materialization logic
- `src/semantics/pipeline.py:1851-2050` -- 199 lines of diagnostics emission, including the mutable `_SemanticDiagnosticsContext` class

**Suggested improvement:**
Extract three modules from `pipeline.py`:
1. `semantics/cdf_resolution.py` -- All CDF-related functions and protocols (`_CdfCursorLike`, `_CdfCursorStoreLike`, `_DeltaServiceLike`, `_cdf_changed_inputs`, etc.)
2. `semantics/output_materialization.py` -- `_materialize_semantic_outputs`, `_write_semantic_output`, `SemanticOutputWriteContext`, location resolution
3. `semantics/diagnostics_emission.py` -- `_SemanticDiagnosticsContext`, `_emit_semantic_quality_view`, `_emit_semantic_quality_views`

This would reduce `pipeline.py` to approximately 1,200 lines focused on orchestration and builder dispatch.

**Effort:** large
**Risk if unaddressed:** medium -- As the pipeline evolves, the mixed concerns make it harder to reason about change impact and increase merge conflicts.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`pipeline.py` changes for at least six reasons (see P2 above). `compiler.py` changes for at least three reasons:

1. **Semantic rule application** (normalize, relate, union, aggregate, dedupe): The core compile logic
2. **Quality-aware relationship compilation** (lines 1254-1818): An entirely separate compilation pathway with its own expression evaluation, feature computation, ranking, and output projection
3. **Table caching/registration** (lines 545-578): Mutable state management for analyzed tables

The quality-relationship compilation in `compiler.py` (lines 1254-1818, ~564 lines) is a substantially different concern from the rule-based semantic compilation. It handles feature scoring, confidence computation, ranking, and ambiguity grouping -- none of which are part of the original 10 semantic rules documented in the module docstring.

**Findings:**
- `src/semantics/compiler.py:1254-1818` -- Quality-relationship compilation (~564 lines) is a distinct concern from the 10-rule semantic compiler
- `src/semantics/pipeline.py:649-776` -- CDF resolution is an independent concern from pipeline orchestration
- `src/semantics/ir_pipeline.py:352-627` -- Dataset row building (~275 lines) is a catalog concern, not an IR compilation concern

**Suggested improvement:**
Extract quality-relationship compilation into `semantics/quality_compiler.py`, keeping `compiler.py` focused on the 10 semantic rules. Move dataset-row building from `ir_pipeline.py` into `semantics/catalog/ir_rows.py`.

**Effort:** large
**Risk if unaddressed:** medium -- The compiler file will continue to grow as new quality signals or ranking strategies are added.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The IR layer (`ir.py`, `ir_optimize.py`, `ir_pipeline.py`) is well-cohesive: all three files are tightly related and have clear data flow. The expression DSL (`exprs.py`) is also self-contained and highly cohesive.

The coupling concern is in `pipeline.py`, where CDF resolution (lines 649-776) is tightly entangled with the main `build_cpg` flow despite being conceptually independent. The CDF logic depends on `DataFusionRuntimeProfile.data_sources.cdf_cursor_store` (line 662), `RuntimeProfile.delta_ops.delta_service()` (line 668), and `ManifestDatasetResolver` -- none of which are related to semantic view construction.

**Findings:**
- `src/semantics/pipeline.py:662-668` -- CDF resolution reaches deep into runtime profile internals (`data_sources.cdf_cursor_store`, `delta_ops.delta_service()`)
- `src/semantics/ir_pipeline.py:947,953,986-987,1326-1327` -- Five direct accesses to `SEMANTIC_MODEL` global, coupling IR pipeline to module-level state

**Suggested improvement:**
Pass CDF resolution results into `build_cpg` as a pre-computed input rather than computing them inline. Accept `SemanticModel` as a parameter to IR pipeline functions instead of importing the global.

**Effort:** medium
**Risk if unaddressed:** medium -- Testing the IR pipeline requires the global `SEMANTIC_MODEL` to be in the correct state.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The dependency graph between the reviewed files flows generally in the right direction:

```
pipeline.py --> compiler.py --> exprs.py (OK)
pipeline.py --> ir_pipeline.py --> ir.py --> ir_optimize.py (OK)
```

However, there is a problematic coupling: `pipeline.py:750` imports `compile_semantics` from `ir_pipeline.py` inside `_outputs_from_changed_inputs`. This means the orchestration layer (pipeline) reaches into the IR compilation layer to re-compile the model, creating a circular conceptual dependency (pipeline uses IR compilation results, but also triggers IR compilation for incremental optimization).

The `datafusion_engine/views/registry_specs.py` importing from `semantics.pipeline` creates an inversion where the execution infrastructure depends on the semantic pipeline internals rather than the other way around.

**Findings:**
- `src/semantics/pipeline.py:750-755` -- Pipeline imports `compile_semantics` for incremental output resolution, blurring the pipeline/IR boundary
- `src/datafusion_engine/views/registry_specs.py:438` -- DataFusion engine depends on pipeline internals (`_cpg_view_specs`)

**Suggested improvement:**
Move incremental output resolution to use `build_semantic_ir()` (the clean public API) rather than reaching into `compile_semantics`. Provide a public facade function for external modules that need view specs.

**Effort:** medium
**Risk if unaddressed:** medium -- The boundary between pipeline and IR layers becomes increasingly blurred.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The expression DSL (`exprs.py`) effectively implements a port: the `ExprContext` protocol (line 30) allows different implementations (`ExprContextImpl` for runtime, `ExprValidationContext` for validation). This is a good hexagonal pattern.

However, the pipeline layer has no explicit ports for storage, CDF, or diagnostics. Delta writes, CDF cursor stores, and diagnostics sinks are accessed through deep chains on `RuntimeProfile` rather than through clean port interfaces. The protocols `_CdfCursorLike`, `_CdfCursorStoreLike`, and `_DeltaServiceLike` (pipeline.py lines 232-255) are steps in the right direction but are private and unused outside the module.

**Findings:**
- `src/semantics/exprs.py:30-76` -- `ExprContext` protocol is a well-designed port
- `src/semantics/pipeline.py:232-255` -- Private protocols for CDF are good but not formalized as module-boundary ports

**Suggested improvement:**
Promote CDF protocols to a shared contracts module if they are needed across the codebase. This is a "design seam" that is correctly placed but could be more explicit.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
The most significant DRY violation is between `build_cpg` (line 1477) and `build_cpg_from_inferred_deps` (line 2052). Both functions:

1. Resolve `CpgBuildOptions` defaults
2. Compute `effective_use_cdf`
3. Open a `stage_span`
4. Call `_resolve_cpg_compile_artifacts`
5. Validate the manifest
6. Call `_view_nodes_for_cpg` with a nearly identical `CpgViewNodesRequest`

`build_cpg_from_inferred_deps` calls `build_cpg` first (line 2104) then re-executes steps 4-5 to extract lineage. This means `_resolve_cpg_compile_artifacts` is called twice for the same inputs -- once inside `build_cpg` and again at line 2114.

Within `ir_pipeline.py`, the expression `{spec.name: spec for spec in SEMANTIC_MODEL.relationship_specs}` is repeated at lines 947 and 1327. The `_dataset_rows_for_model(SEMANTIC_MODEL)` call appears at lines 953, 986, and indirectly via `_build_schema_index`.

**Findings:**
- `src/semantics/pipeline.py:2104-2145` -- `build_cpg_from_inferred_deps` duplicates compile-resolution and view-node construction from `build_cpg`
- `src/semantics/ir_pipeline.py:947,1327` -- `{spec.name: spec for spec in SEMANTIC_MODEL.relationship_specs}` dictionary comprehension duplicated
- `src/semantics/ir_pipeline.py:953,986` -- `_dataset_rows_for_model(SEMANTIC_MODEL)` called redundantly

**Suggested improvement:**
Refactor `build_cpg_from_inferred_deps` to receive the compiled artifacts from `build_cpg` via a shared result object rather than re-computing them. Extract the relationship-spec-by-name dictionary to a module-level helper or cache within `SEMANTIC_MODEL`.

**Effort:** medium
**Risk if unaddressed:** medium -- Divergence between the two code paths will lead to subtle bugs.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
The IR data structures (`SemanticIR`, `SemanticIRView`, `InferredViewProperties`) are frozen dataclasses with clear type signatures -- they serve as implicit contracts. The `validate_expr_spec` function (exprs.py:1045) explicitly validates preconditions (column availability, determinism).

However, builder closure factories in `pipeline.py` (e.g., `_normalize_builder`, `_relationship_builder`) return `DataFrameBuilder` callables with no documented contract about what they require from the `SessionContext` or what schema they produce. The contract is implicit in the closure's captures.

**Findings:**
- `src/semantics/exprs.py:1045-1078` -- `validate_expr_spec` enforces explicit preconditions for column availability and determinism
- `src/semantics/pipeline.py:336-583` -- Builder factories produce closures without documented schema contracts

**Suggested improvement:**
Add docstrings to builder factories documenting expected session-context state (which tables must be registered) and output schema invariants.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
`CpgBuildOptions` (pipeline.py:88) parses configuration at the boundary, which is good. `CpgViewNodesRequest` and `CpgViewSpecsRequest` are frozen dataclasses that structure inputs for the view-building pipeline.

However, cache policy normalization happens late: `_normalize_cache_policy_mapping` is called inside `_view_nodes_for_cpg` (line 1247), deep in the pipeline, rather than at the `CpgBuildOptions` boundary. This means invalid cache policy strings propagate through several layers before being caught.

**Findings:**
- `src/semantics/pipeline.py:277-288` -- `_normalize_cache_policy` validates strings but is called at line 1247, not at the entry point
- `src/semantics/pipeline.py:88-125` -- `CpgBuildOptions` accepts `Mapping[str, CachePolicy]` but `compiled_cache_policy` accepts `Mapping[str, str]` -- two different representations for the same concept

**Suggested improvement:**
Normalize `compiled_cache_policy` to `Mapping[str, CachePolicy]` in `CpgBuildOptions.__post_init__` or in `_resolve_cache_policy_hierarchy`, so invalid values are caught at the boundary.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
`GraphPosition` (ir.py:16-31) uses `Literal` to constrain to four valid positions. `InferredViewProperties` uses `None` defaults for graceful degradation. `SemanticIRView.kind` uses `ViewKindStr` which is a typed string.

A minor gap: `CpgBuildOptions` has both `cache_policy` and `compiled_cache_policy` with independent `None`-ability. The resolution hierarchy (line 1193-1223) makes the precedence clear, but the dual-optional representation allows a state where both are set, which is handled but could be prevented by construction.

**Findings:**
- `src/semantics/ir.py:16-31` -- `GraphPosition` is well-constrained via `Literal`
- `src/semantics/pipeline.py:117-118` -- Dual cache-policy fields on `CpgBuildOptions` allow redundant specification

**Suggested improvement:**
Consider a union type or a dedicated `CachePolicySource` that enforces mutual exclusivity between explicit and compiled cache policies.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS (Command-Query Separation) -- Alignment: 1/3

**Current state:**
`SemanticCompiler.register()` (compiler.py:545-561) both mutates `self._tables` and returns a `TableInfo` object. `SemanticCompiler.get()` (compiler.py:563-578) implicitly calls `register()` as a side effect when the table is not cached, making a "query" method trigger mutation.

`_SemanticDiagnosticsContext.ensure_incremental_runtime()` (pipeline.py:1864-1873) mutates `self.incremental_runtime` and returns it. `_SemanticDiagnosticsContext.write_snapshot()` (pipeline.py:1875-1898) performs IO (writing snapshots) and returns a path string.

In the IR pipeline, all four core functions (`compile_semantics`, `infer_semantics`, `optimize_semantics`, `emit_semantics`) are pure queries that take IR and return IR -- this is an exemplary CQS design.

**Findings:**
- `src/semantics/compiler.py:545-561` -- `register()` mutates `_tables` dict AND returns `TableInfo`
- `src/semantics/compiler.py:563-578` -- `get()` triggers side-effectful `register()` on cache miss, making it impure
- `src/semantics/pipeline.py:1864-1873` -- `ensure_incremental_runtime()` mutates state and returns value
- `src/semantics/pipeline.py:1875-1898` -- `write_snapshot()` performs IO and returns path
- `src/semantics/ir_pipeline.py:693-917,920-975,978-995,1299-1357` -- IR pipeline functions are pure and CQS-compliant

**Suggested improvement:**
Split `SemanticCompiler.register()` into `register(name)` (command, returns None) and `get(name)` (query, raises if not registered). Or make `get()` explicitly named `get_or_register()` to signal its dual nature. The `_SemanticDiagnosticsContext` methods could be refactored: `ensure_incremental_runtime` could be a property that lazy-initializes, and `write_snapshot` could return `None` and store the path internally.

**Effort:** medium
**Risk if unaddressed:** medium -- The implicit registration in `get()` creates surprising behavior where reading a table can fail with DataFusion errors if the table doesn't exist in the session context.

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 1/3

**Current state:**
The `SEMANTIC_MODEL` singleton (from `semantics.registry`) is accessed as a module-level global in both `pipeline.py` (line 49) and `ir_pipeline.py` (line 27). Functions like `optimize_semantics` (ir_pipeline.py:920) and `emit_semantics` (ir_pipeline.py:978) hard-wire to `SEMANTIC_MODEL` rather than accepting the model as a parameter. This prevents testing these functions with alternative models and couples them to the global registry state.

The `compile_semantics(model)` function (ir_pipeline.py:693) correctly accepts the model as a parameter, demonstrating the intended pattern. But `optimize_semantics` and `emit_semantics` break this pattern by reading `SEMANTIC_MODEL` directly.

**Findings:**
- `src/semantics/ir_pipeline.py:947` -- `optimize_semantics` reads `SEMANTIC_MODEL.relationship_specs` directly instead of receiving it as input
- `src/semantics/ir_pipeline.py:986-987` -- `emit_semantics` calls `_dataset_rows_for_model(SEMANTIC_MODEL)` and `semantic_model_fingerprint(SEMANTIC_MODEL)` using the global
- `src/semantics/ir_pipeline.py:1326-1327` -- `infer_semantics` reads `SEMANTIC_MODEL.relationship_specs` directly
- `src/semantics/pipeline.py:627,752,754,1626,1632` -- Five direct accesses to `SEMANTIC_MODEL` in pipeline functions

**Suggested improvement:**
Thread `model: SemanticModel` as a parameter through `optimize_semantics`, `emit_semantics`, and `infer_semantics`, matching the pattern already used by `compile_semantics`. Then `build_semantic_ir` becomes the single composition root that provides the global model. This makes all four IR pipeline phases independently testable.

**Effort:** medium
**Risk if unaddressed:** medium -- Testing IR pipeline phases requires the global `SEMANTIC_MODEL` to be in a specific state, which is fragile.

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies exist in the reviewed files. `SemanticCompiler` is a concrete class that composes behavior from `SemanticSchema`, `AnnotatedSchema`, `JoinStrategy`, and `ExprContextImpl`. The expression DSL uses function composition (higher-order functions like `and_`, `or_`, `alias`) rather than class hierarchies.

**Findings:**
None -- this principle is well-satisfied.

**Effort:** N/A
**Risk if unaddressed:** low

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most interactions are with direct collaborators. However, `_cdf_changed_inputs` (pipeline.py:649) navigates through `runtime_profile.data_sources.cdf_cursor_store` and `runtime_profile.delta_ops.delta_service()` -- three levels deep. Similarly, `_view_nodes_for_cpg` (pipeline.py:1244) accesses `request.runtime_profile.data_sources.semantic_output.cache_overrides` -- four levels deep.

**Findings:**
- `src/semantics/pipeline.py:662` -- `runtime_profile.data_sources.cdf_cursor_store` (3 levels)
- `src/semantics/pipeline.py:668` -- `runtime_profile.delta_ops.delta_service()` (3 levels, plus method call)
- `src/semantics/pipeline.py:1244` -- `request.runtime_profile.data_sources.semantic_output.cache_overrides` (4 levels)

**Suggested improvement:**
Introduce helper methods on `DataFusionRuntimeProfile` that return the needed values directly, e.g., `runtime_profile.cdf_cursor_store()` and `runtime_profile.semantic_cache_overrides()`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The compiler's `_compile_relationship_with_quality_inner` method (compiler.py:1366-1444) follows a step-by-step pipeline pattern where each step takes the current joined DataFrame and transforms it. This is "tell" style -- each step tells the DataFrame what to do rather than asking it about its state.

However, the output projection methods (`_project_quality_output`, `_select_quality_default_output`) do a fair amount of "asking" by inspecting `schema_names` to determine which columns exist before projecting them.

**Findings:**
- `src/semantics/compiler.py:1720-1756` -- `_select_quality_default_output` inspects schema names extensively to conditionally include columns

**Suggested improvement:**
This is inherent to the problem domain (projecting available columns from a dynamically-constructed join). The current approach is acceptable.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The IR pipeline (`compile_semantics`, `infer_semantics`, `optimize_semantics`, `emit_semantics`) is purely functional: each function takes an IR and returns a new IR. This is the ideal "functional core."

The `SemanticCompiler` has a mutable `_tables` cache (line 211), which means the compiler is not purely functional. However, the mutation is limited to caching analyzed tables, and all actual data transformations produce new DataFrames without mutating the inputs.

`pipeline.py`'s `build_cpg` is correctly structured as an imperative shell that coordinates the functional IR pipeline and the impure operations (registration, materialization, diagnostics).

**Findings:**
- `src/semantics/ir_pipeline.py:693-1375` -- All four IR phases are purely functional
- `src/semantics/compiler.py:211,560` -- `_tables` dict introduces mutable state in the compiler

**Suggested improvement:**
Consider making the table cache external to the compiler (e.g., pass in a pre-analyzed table index) to make the compiler fully stateless. This would simplify testing and make the functional/imperative boundary crisper.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Re-running `build_semantic_ir` with the same model produces the same IR (deterministic fingerprints). The compiler's `register` method overwrites existing entries, making re-registration safe. Output materialization uses `WriteMode.OVERWRITE`, ensuring idempotent writes.

**Findings:**
None -- this principle is well-satisfied.

**Effort:** N/A
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
The system has strong determinism guarantees:
- `semantic_model_fingerprint` and `semantic_ir_fingerprint` (ir_pipeline.py:98-155) use `hash_msgpack_canonical` for deterministic hashing
- `validate_expr_spec` (exprs.py:1045-1078) explicitly checks for nondeterministic tokens (`random`, `uuid`, `now()`, etc.)
- View ordering in `optimize_semantics` uses `VIEW_KIND_ORDER` and stable name sorting
- `SemanticIR` carries `model_hash` and `ir_hash` for reproducibility tracking

**Findings:**
None -- this principle is well-satisfied.

**Effort:** N/A
**Risk if unaddressed:** low

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The expression DSL (`exprs.py`) is a model of simplicity: each helper is a small, composable function that produces an `ExprSpec`. The builder pattern (`Callable[[ExprContext], Expr]`) is intuitive and easy to extend.

The builder-dispatch system in `pipeline.py` (lines 874-1186) has accumulated significant indirection: `_builder_for_semantic_spec` dispatches to `_CONSOLIDATED_BUILDER_HANDLERS`, which dispatches to kind-specific handlers like `_builder_for_normalize_kind`, which further dispatches to specific handlers like `_builder_for_normalize_spec`. This three-level dispatch adds cognitive overhead.

**Findings:**
- `src/semantics/exprs.py` -- Clean, minimal DSL design
- `src/semantics/pipeline.py:874-1186` -- Three-level dispatch hierarchy adds indirection: `_builder_for_semantic_spec` -> `_CONSOLIDATED_BUILDER_HANDLERS[kind]` -> `_builder_for_*_kind` -> `_builder_for_*_spec`

**Suggested improvement:**
Consider flattening to a two-level dispatch: map `ViewKind` values directly to handler functions, eliminating the intermediate "consolidated kind" layer. The consolidated-kind grouping could be documented without requiring a runtime dispatch step.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
`_builder_for_artifact_spec` (pipeline.py:1069-1075) always raises `ValueError`, indicating a speculative extension point that was never implemented. `emit_semantics` (ir_pipeline.py:978-995) is described as a "placeholder" in its docstring but performs real work (computing dataset rows and fingerprints). These are minor YAGNI signals.

**Findings:**
- `src/semantics/pipeline.py:1069-1075` -- `_builder_for_artifact_spec` always raises, dead extension point
- `src/semantics/ir_pipeline.py:978` -- `emit_semantics` docstring says "placeholder" but has real logic

**Suggested improvement:**
Remove `_builder_for_artifact_spec` and its registration in the dispatch table until the artifact spec pattern is needed. Update the `emit_semantics` docstring to accurately describe its current behavior.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 1/3

**Current state:**
The most astonishing pattern is `_cpg_view_specs` being imported as a "public" API despite its underscore prefix. A reader would reasonably assume underscore-prefixed functions are internal and safe to rename. This violates the naming convention's implied contract.

`SemanticCompiler.get()` (compiler.py:563) is named like a query but has the side effect of registering the table. A reader would not expect a `get` method to modify state.

The `_SemanticDiagnosticsContext` (pipeline.py:1851) is a mutable dataclass (no `frozen=True`) embedded in a file where all other dataclasses are frozen. This breaks the local convention.

**Findings:**
- `src/semantics/pipeline.py:778` -- `_cpg_view_specs` is private by name but public by usage
- `src/semantics/compiler.py:563-578` -- `get()` triggers side-effectful registration
- `src/semantics/pipeline.py:1851-1852` -- `_SemanticDiagnosticsContext` is mutable, unlike all other dataclasses in the file

**Suggested improvement:**
Rename `_cpg_view_specs` to `cpg_view_specs`. Rename `SemanticCompiler.get()` to `get_or_register()` to signal its behavior. Make `_SemanticDiagnosticsContext` frozen and pass runtime state separately.

**Effort:** small
**Risk if unaddressed:** medium -- Developers will be surprised by the implicit registration and the naming inconsistency.

---

#### P22. Declare and version public contracts -- Alignment: 1/3

**Current state:**
Each file has an `__all__` list, which is good. However, the `__all__` lists are incomplete:

- `pipeline.py:2249-2256` lists only 6 symbols but `CpgViewSpecsRequest`, `CpgViewNodesRequest`, `CpgBuildOptions`, `_cpg_view_specs`, and `SemanticOutputWriteContext` are all used externally
- `ir_pipeline.py:1377-1385` lists 7 symbols but `compile_semantics` is also used by `pipeline.py` internally

The `semantics/__init__.py` (line 38-57) re-exports `SemanticCompiler` and `TableInfo` but not `build_cpg` or `CpgBuildOptions`, which are the primary public API for pipeline consumers.

**Findings:**
- `src/semantics/pipeline.py:2249-2256` -- `__all__` omits at least 4 externally-used symbols
- `src/semantics/__init__.py:38-57` -- Package re-exports do not include `build_cpg`, the primary entry point

**Suggested improvement:**
Audit `__all__` lists and add all externally-imported symbols. Add `build_cpg` and `CpgBuildOptions` to the package `__init__.py`.

**Effort:** medium
**Risk if unaddressed:** medium -- No clear signal about what is stable API vs. internal detail.

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
The IR pipeline is highly testable: all four phases accept inputs and return outputs without requiring a session context or global state. The expression DSL is also easily testable with `ExprValidationContext`.

`SemanticCompiler` requires a real `SessionContext` with registered tables, making unit testing require DataFusion setup. The global `SEMANTIC_MODEL` dependency in `ir_pipeline.py` means testing `optimize_semantics`, `emit_semantics`, or `infer_semantics` requires the global to be populated.

The builder factories in `pipeline.py` return closures that capture configuration but require a `SessionContext` to execute, making them testable only with integration-level setup.

**Findings:**
- `src/semantics/ir_pipeline.py:693-1375` -- IR pipeline phases are pure and easily testable
- `src/semantics/compiler.py:200-213` -- `SemanticCompiler.__init__` requires `SessionContext`
- `src/semantics/ir_pipeline.py:947,986,1326` -- `optimize_semantics`, `emit_semantics`, `infer_semantics` use `SEMANTIC_MODEL` global

**Suggested improvement:**
Thread `model: SemanticModel` as a parameter to all IR pipeline functions (see P12). For the compiler, consider a factory method that accepts pre-analyzed table metadata to enable testing without a full session.

**Effort:** medium
**Risk if unaddressed:** medium -- Integration-only testing of core logic increases CI time and makes failure diagnosis harder.

---

#### P24. Observability -- Alignment: 3/3

**Current state:**
The compiler instruments every major operation with `stage_span` from OpenTelemetry:
- `normalize_from_spec` (compiler.py:593)
- `normalize` (compiler.py:705)
- `normalize_text` (compiler.py:781)
- `relate` (compiler.py:870)
- `union_nodes` (compiler.py:1093)
- `union_edges` (compiler.py:1123)
- `aggregate` (compiler.py:1169)
- `dedupe` (compiler.py:1223)
- `compile_relationship_with_quality` (compiler.py:1794)

Each span includes structured attributes (`codeanatomy.*` keys) with relevant metadata. The `_record_semantic_compile_artifacts` function (pipeline.py:1269) records comprehensive artifacts including explain plans, view plan stats, and join group statistics.

**Findings:**
None -- this principle is well-satisfied.

**Effort:** N/A
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Pipeline.py as a "God Module"

`pipeline.py` at 2,256 LOC contains at least six distinct concerns (view orchestration, CDF resolution, output materialization, diagnostics emission, builder factories, cache policy resolution). This concentrates change risk and makes the file difficult to navigate. The root cause is organic growth where new pipeline features were added to the existing orchestration file rather than being extracted into focused modules.

**Affected principles:** P2, P3, P4, P19
**Suggested approach:** Extract CDF resolution, output materialization, and diagnostics emission into separate modules. This should be done incrementally, starting with CDF resolution (the most self-contained concern).

### Theme 2: Global Singleton Coupling via SEMANTIC_MODEL

Both `pipeline.py` and `ir_pipeline.py` directly access the `SEMANTIC_MODEL` global from `semantics.registry`. This creates implicit coupling to a module-level binding, prevents independent testing of IR pipeline phases, and forces callers to ensure the global is populated before calling any function.

**Affected principles:** P1, P12, P23
**Suggested approach:** Thread `model: SemanticModel` as an explicit parameter through all IR pipeline functions. `build_semantic_ir` becomes the composition root that provides the global model. This is a single refactor with high testability payoff.

### Theme 3: Implicit Public API via Underscore-Prefixed Exports

`_cpg_view_specs` is imported by `datafusion_engine/views/registry_specs.py` despite being underscore-prefixed. This indicates the public surface is larger than declared, creating a gap between documentation/naming and reality.

**Affected principles:** P1, P21, P22
**Suggested approach:** Audit all cross-module imports from `semantics.pipeline`, promote any function that is genuinely part of the external API to a public name, and add it to `__all__`.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P21, P22 | Rename `_cpg_view_specs` to `cpg_view_specs`, add to `__all__` | small | Eliminates naming violation and documents public API |
| 2 | P20 | Remove dead `_builder_for_artifact_spec` and update `emit_semantics` docstring | small | Removes misleading code and documentation |
| 3 | P11, P21 | Rename `SemanticCompiler.get()` to `get_or_register()` | small | Signals the method's dual query+command behavior |
| 4 | P9 | Normalize `compiled_cache_policy` at `CpgBuildOptions` boundary | small | Catches invalid cache policies earlier |
| 5 | P8 | Add schema/contract docstrings to builder factory functions | small | Documents implicit contracts for builder closures |

## Recommended Action Sequence

1. **P21/P22: Fix the naming/export violations** (small). Rename `_cpg_view_specs` to `cpg_view_specs`, add it and `CpgViewSpecsRequest` to `__all__`, update the import in `registry_specs.py`. This is zero-risk and eliminates the most surprising behavior.

2. **P12: Thread `model` parameter through IR pipeline** (medium). Add `model: SemanticModel` parameter to `optimize_semantics`, `emit_semantics`, and `infer_semantics`. Update `build_semantic_ir` to pass `SEMANTIC_MODEL`. This unlocks independent testing of all four IR phases.

3. **P7: Eliminate duplication in `build_cpg_from_inferred_deps`** (medium). Refactor to return compile artifacts from `build_cpg` rather than re-computing them. This prevents the double `_resolve_cpg_compile_artifacts` call and keeps the two entry points in sync.

4. **P2/P3: Extract CDF resolution from pipeline.py** (medium). Move all CDF-related functions and protocols to `semantics/cdf_resolution.py`. This is self-contained and removes ~130 lines from pipeline.py.

5. **P2/P3: Extract output materialization** (medium). Move Delta-write logic to `semantics/output_materialization.py`. Removes ~215 lines.

6. **P2/P3: Extract diagnostics emission** (medium). Move diagnostics context and emission to `semantics/diagnostics_emission.py`. Removes ~200 lines.

7. **P3: Extract quality-relationship compilation from compiler.py** (large). Move the quality-relationship compilation (lines 1254-1818) to `semantics/quality_compiler.py`, keeping compiler.py focused on the 10 semantic rules. This is the largest change and should be done after steps 1-6 stabilize.

8. **P11: Refine CQS in SemanticCompiler** (small). Rename `get()` to `get_or_register()` or split into separate command/query methods.
