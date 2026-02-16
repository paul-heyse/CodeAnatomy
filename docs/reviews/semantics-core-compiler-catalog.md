# Design Review: src/semantics/ Core Compiler & Catalog

**Date:** 2026-02-16
**Scope:** `src/semantics/` root-level files + `src/semantics/catalog/`
**Focus:** All principles (1-24), with emphasis on SRP in compiler, information hiding, dependency direction, DRY across registries
**Depth:** Deep (exhaustive file-by-file)
**Files reviewed:** 54 files (~15,363 lines)

## Executive Summary

The semantic compiler and catalog subsystem demonstrates strong foundational design in its IR pipeline (compile/infer/optimize/emit), quality compilation protocol pattern, and view kind consolidation. However, three systemic issues reduce alignment: (1) `pipeline.py` (1,671 lines) accumulates five distinct responsibilities (builder dispatch, view node construction, materialization, CDF resolution, artifact recording), violating SRP; (2) `spec_registry.py` is a pure re-export facade over `registry.py` that adds indirection without value; and (3) the module-level singleton caching pattern in `catalog/dataset_rows.py` and `catalog/dataset_specs.py` is duplicated structurally and bypasses dependency injection. The quality compilation subsystem (`quality_compiler.py`) stands out as an exemplar of protocol-based dependency inversion and testability.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | medium | `SemanticCompiler.ctx` exposed as public attr; `_tables` dict internals leak |
| 2 | Separation of concerns | 1 | large | medium | `pipeline.py` mixes builder dispatch, materialization, CDF, artifacts, markdown |
| 3 | SRP | 1 | large | high | `pipeline.py` has 5+ reasons to change; `SemanticCompiler` has 3 |
| 4 | High cohesion, low coupling | 2 | medium | medium | `pipeline.py` couples to 15+ modules; catalog modules are well-cohesive |
| 5 | Dependency direction | 2 | medium | medium | `pipeline.py` imports from execution layer (`register_view_graph`, `WritePipeline`) |
| 6 | Ports & Adapters | 2 | medium | low | `quality_compiler.py` exemplary; `pipeline.py` lacks port boundaries for IO |
| 7 | DRY | 1 | medium | medium | `spec_registry.py` re-exports `registry.py`; caching pattern duplicated in catalog |
| 8 | Design by contract | 2 | small | low | Good frozen dataclasses; `SemanticDatasetRow` has 23 fields with many optional |
| 9 | Parse, don't validate | 2 | small | low | `SemanticConfig` parsed once; some scattered validation in builder functions |
| 10 | Illegal states | 2 | medium | medium | `SemanticDatasetRow` allows impossible field combinations; `ViewKind` well-typed |
| 11 | CQS | 2 | small | low | `register()` both mutates and returns; most functions pure otherwise |
| 12 | DI + explicit composition | 2 | medium | medium | `quality_compiler.py` protocol-based; `pipeline.py` hard-wires singleton imports |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; composition throughout |
| 14 | Law of Demeter | 2 | small | low | `runtime_profile.data_sources.semantic_output.cache_overrides` chain in pipeline.py |
| 15 | Tell, don't ask | 2 | small | low | `SemanticSchema` encapsulates rules well; `_build_cpg_output_rows()` exposes raw tuples |
| 16 | Functional core | 2 | medium | low | IR pipeline is pure transforms; `pipeline.py` mixes IO at the same level |
| 17 | Idempotency | 3 | - | - | IR compilation and optimization are stateless; deterministic hashing |
| 18 | Determinism | 3 | - | - | `policy_hash`, `ddl_fingerprint`, sorted outputs throughout |
| 19 | KISS | 2 | small | low | `naming.py` identity mapping is a useful seam but `internal_name()` is a no-op |
| 20 | YAGNI | 2 | small | low | `ViewKindParams` struct is speculative; `output_policy` field unused |
| 21 | Least astonishment | 2 | small | low | `schema_names()` instance method delegates to static; `spec_registry` misleading name |
| 22 | Public contracts | 2 | small | low | `__all__` declared everywhere; `_reset_cache` exported in `__all__` |
| 23 | Testability | 2 | medium | medium | Singleton caches resist DI; quality_compiler protocol is highly testable |
| 24 | Observability | 3 | - | - | Consistent `stage_span` instrumentation; structured attributes on all operations |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
`SemanticCompiler` exposes its `SessionContext` as a public attribute (`self.ctx`) at `compiler.py:196`, allowing callers to bypass the compiler's semantic operations and interact directly with the DataFusion session. The internal `_tables` registry, while prefixed with underscore, is a mutable `dict` that could be inadvertently accessed. The `quality_compiler.py` protocol (`SemanticCompilerLike`) at line 48 explicitly requires `ctx: SessionContext` as a public field, cementing the leak into the protocol contract.

**Findings:**
- `compiler.py:196` -- `self.ctx = ctx` exposes the full SessionContext as a public attribute
- `compiler.py:197` -- `self._tables: dict[str, TableInfo] = {}` is a mutable dict; no accessor boundary
- `quality_compiler.py:48` -- `SemanticCompilerLike` protocol requires `ctx: SessionContext` publicly
- `pipeline.py:1095` -- `request.runtime_profile.data_sources.semantic_output.cache_overrides` deep accessor chain reaches through multiple layers
- `ir_pipeline.py:305-306` -- `getattr(row, "name", None)` and `getattr(row, "metadata_extra", None)` defensive access suggests unstable internal shape

**Suggested improvement:**
Narrow `SemanticCompilerLike` to not expose `ctx` directly. Instead, add targeted methods to the protocol for the specific capabilities that `quality_compiler.py` needs from the session (e.g., `table(name) -> DataFrame`). On `SemanticCompiler`, expose `ctx` as a read-only property or keep it private and provide the needed accessor methods.

**Effort:** medium
**Risk if unaddressed:** medium -- any caller can reach into the DataFusion session and create undocumented side effects

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`pipeline.py` (1,671 lines) contains five distinct concern areas: (1) builder factory functions (lines 303-529), (2) view node construction and graph registration (lines 1075-1119), (3) Delta Lake materialization orchestration (lines 1416-1424), (4) CDF input resolution (lines 1574-1657), and (5) artifact recording and markdown report generation (lines 1122-1327). These five concerns change for independent reasons.

**Findings:**
- `pipeline.py:303-529` -- 10+ builder factory functions (`_normalize_builder`, `_scip_norm_builder`, etc.) -- builder dispatch concern
- `pipeline.py:1075-1119` -- `_view_nodes_for_cpg()` -- view graph assembly concern
- `pipeline.py:1122-1327` -- `_record_semantic_compile_artifacts()` + `_semantic_explain_markdown()` -- artifact recording and report generation concern
- `pipeline.py:1416-1424` -- materialization call inside `_execute_cpg_build()` -- IO/write concern
- `pipeline.py:1574-1657` -- `_resolve_cdf_inputs()`, `_has_cdf_inputs()` -- CDF resolution concern
- `pipeline.py:1270-1327` -- `_semantic_explain_markdown()` generates Markdown inline -- presentation concern mixed with domain logic

**Suggested improvement:**
Extract the five concerns into separate modules: (1) `semantics/builder_dispatch.py` for `_CONSOLIDATED_BUILDER_HANDLERS` and all `_builder_for_*` functions; (2) keep view node assembly in `pipeline.py`; (3) `semantics/artifact_recording.py` for `_record_semantic_compile_artifacts` and `_semantic_explain_markdown`; (4) move CDF resolution to `semantics/cdf_resolution.py` (already partially exists); (5) materialization is already in `output_materialization.py` -- remove the inline call and let the orchestrator coordinate.

**Effort:** large
**Risk if unaddressed:** medium -- pipeline.py is the most-changed file; any new view kind, artifact, or materialization format touches it

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`pipeline.py` changes for at least five reasons: new view kinds, new builder types, cache policy changes, artifact format changes, and materialization policy changes. `SemanticCompiler` (1,343 lines) changes for three reasons: new semantic rules, new quality compilation patterns, and table registration changes.

**Findings:**
- `pipeline.py:1010-1019` -- `_CONSOLIDATED_BUILDER_HANDLERS` dispatch table is a separate concern from view node assembly
- `pipeline.py:1330-1442` -- `_execute_cpg_build()` orchestrates compilation, view graph registration, artifact recording, materialization, AND quality diagnostics emission in a single 112-line function
- `compiler.py:179-199` -- `SemanticCompiler.__init__` owns table registration, UDF validation, AND all semantic operations
- `compiler.py:1290-1340` -- Quality delegation methods (`build_join_group`, `compile_relationship_from_join`, `compile_relationship_with_quality`) are thin forwarding wrappers to `quality_compiler.py` module functions

**Suggested improvement:**
For `pipeline.py`: Extract `_execute_cpg_build()` into an orchestrator that composes distinct steps via explicit phase functions. For `SemanticCompiler`: The quality delegation methods (lines 1290-1340) are already properly decomposed to `quality_compiler.py`; the remaining concern is that table registration and semantic rule application are interleaved. Consider separating `TableRegistry` from `SemanticRuleEngine`.

**Effort:** large
**Risk if unaddressed:** high -- `pipeline.py` is the primary coordination point and its growth makes it progressively harder to reason about

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The catalog submodules (`dataset_rows.py`, `dataset_specs.py`, `spec_builder.py`, `tags.py`, `projections.py`) are well-cohesive, each handling a specific dataset metadata concern. However, `pipeline.py` imports from 15+ modules and couples to both core semantic types and execution-layer types (`ViewNode`, `ViewGraphOptions`, `WritePipeline`).

**Findings:**
- `pipeline.py:18-55` -- imports from `datafusion_engine.delta`, `datafusion_engine.identity`, `datafusion_engine.views`, `datafusion_engine.plan`, `datafusion_engine.session`, `semantics.cdf_resolution`, `semantics.diagnostics`, `semantics.naming`, `semantics.output_materialization`, `semantics.quality`, `semantics.specs`, `semantics.view_kinds`, `utils.hashing` -- high fan-out
- `catalog/dataset_rows.py` and `catalog/dataset_specs.py` -- well-cohesive, narrow interface
- `quality_compiler.py:39-73` -- `SemanticCompilerLike` and `TableInfoLike` protocols are narrow, intention-revealing interfaces

**Suggested improvement:**
Reduce `pipeline.py` fan-out by extracting builder dispatch and artifact recording into focused modules, each with a narrower import surface.

**Effort:** medium
**Risk if unaddressed:** medium -- high fan-out makes `pipeline.py` fragile to changes in any of its 15+ dependencies

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The core semantic rules in `compiler.py` correctly depend inward on pure types (`SemanticSchema`, `SemanticConfig`, `JoinStrategy`). The IR pipeline (`ir_pipeline.py`) is largely pure. However, `pipeline.py` imports execution-layer details including `register_view_graph`, `ViewGraphOptions`, `WritePipeline`, `register_cdf_inputs_for_profile`, and `semantic_output_locations_for_profile` -- all of which are infrastructure details that should depend on the semantic core, not the reverse.

**Findings:**
- `pipeline.py:1362-1364` -- `from datafusion_engine.views.graph import ViewGraphOptions, ViewGraphRuntimeOptions, register_view_graph` -- execution layer imported into semantic pipeline
- `pipeline.py:1375` -- `from datafusion_engine.session.runtime import semantic_output_locations_for_profile` -- session runtime detail
- `pipeline.py:1630` -- `from datafusion_engine.session.runtime import register_cdf_inputs_for_profile` -- session detail
- `compiler.py:40-49` -- imports from `obs.otel`, `relspec`, `semantics.*` -- appropriate inward dependencies
- `ir_pipeline.py:11-32` -- imports only from `semantics.*` and `utils.*` -- well-aligned

**Suggested improvement:**
`pipeline.py`'s execution-layer imports should be pushed to the edges. The `_execute_cpg_build` function should receive execution services (view registration, materialization, CDF resolution) as injected callables or a protocol rather than importing them directly.

**Effort:** medium
**Risk if unaddressed:** medium -- semantic core cannot be tested or reused without pulling in the full execution runtime

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`quality_compiler.py` is an exemplary Ports & Adapters implementation: the `SemanticCompilerLike` protocol (lines 45-72) defines exactly the port the quality compilation subsystem needs, and `SemanticCompiler` implements it. However, `pipeline.py` has no port boundary for its IO operations (Delta writes, CDF registration, artifact recording) -- it directly imports and calls concrete infrastructure.

**Findings:**
- `quality_compiler.py:45-72` -- `SemanticCompilerLike` protocol is a well-defined port
- `pipeline.py:1416-1424` -- Direct call to `materialize_semantic_outputs()` with no port abstraction
- `pipeline.py:1392-1405` -- Direct call to `register_view_graph()` with no port abstraction
- `output_materialization.py:206-241` -- `materialize_semantic_outputs()` directly imports and uses `WritePipeline`

**Suggested improvement:**
Define a `SemanticBuildPorts` protocol (or similar) that captures the IO operations `_execute_cpg_build` needs: `register_views(nodes)`, `materialize(view_names)`, `record_artifacts(ir)`. This would make the orchestration testable without heavyweight DataFusion sessions.

**Effort:** medium
**Risk if unaddressed:** low -- the current approach works but limits testability and reusability

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
Three forms of knowledge duplication exist: (1) `spec_registry.py` is a pure re-export facade over `registry.py`, duplicating every symbol name; (2) the module-level singleton cache pattern is structurally duplicated between `catalog/dataset_rows.py` and `catalog/dataset_specs.py`; (3) CPG output field names are hardcoded in `ir_pipeline.py:455-595` as raw tuples when they should be derived from schema definitions.

**Findings:**
- `spec_registry.py:22-32` -- 11 re-export assignments that exactly mirror `registry.py` -- pure duplication of the name→module mapping
- `catalog/dataset_rows.py:105-128` -- `_SemanticDatasetRowCache` with lazy init pattern
- `catalog/dataset_specs.py:15-22` -- `_DatasetSpecCache` with identical lazy init pattern -- structural duplication
- `ir_pipeline.py:455-595` -- `_build_cpg_output_rows()` hardcodes field tuples like `("node_kind", "node_id", "task_name", "task_priority")` for 6 CPG outputs instead of deriving from `cpg/` schema contracts
- `ir_pipeline.py:366-415` -- `_build_relationship_rows()` rebuilds field tuples from relationship specs inline rather than referencing a shared schema

**Suggested improvement:**
(1) Eliminate `spec_registry.py` as a separate module: move `SemanticSpecIndex` and `SEMANTIC_SPEC_INDEX` into `registry.py` and update all import sites. (2) Extract the cache pattern into a generic `_LazySingletonCache[T]` helper or use `functools.cache`. (3) Derive CPG output field names from `cpg/` schema definitions rather than hardcoding them.

**Effort:** medium
**Risk if unaddressed:** medium -- the `spec_registry.py` facade already has import sites that sometimes import from one and sometimes the other, causing confusion

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Frozen dataclasses with explicit fields provide strong structural contracts. `SemanticCompiler` methods validate preconditions (e.g., `_ensure_columns_present` at `compiler.py:276-285`, span unit compatibility at `compiler.py:559-574`). However, `SemanticDatasetRow` has 23 fields with many optional, making it unclear which combinations are valid for different dataset categories.

**Findings:**
- `compiler.py:276-285` -- `_ensure_columns_present()` enforces explicit preconditions
- `compiler.py:559-574` -- `_require_span_unit_compatibility()` enforces invariant
- `catalog/dataset_rows.py:80-102` -- `SemanticDatasetRow` has 23 fields; `merge_keys`, `template`, `view_builder`, `semantic_id`, `entity`, `grain`, etc. are all optional with no enforcement of valid combinations
- `ir.py:68-103` -- `InferredViewProperties` all optional with clear docstring rationale ("graceful degradation")
- `compile_invariants.py:1-131` -- `CompileTracker` with thread-safe enforcement is good

**Suggested improvement:**
Consider splitting `SemanticDatasetRow` into category-specific variants (e.g., `InputDatasetRow`, `SemanticOutputRow`, `DiagnosticOutputRow`) that enforce valid field combinations at construction time. This aligns with P10 (make illegal states unrepresentable).

**Effort:** medium
**Risk if unaddressed:** low -- the current broad dataclass works but makes it possible to construct rows with nonsensical field combinations

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
`SemanticConfig` and `SemanticConfigSpec` (in `config.py`) parse configuration once at the boundary. `SemanticTableSpec` and related frozen structs (in `specs.py`) are well-typed parsed representations. However, builder functions in `pipeline.py` perform scattered validation checks (e.g., `pipeline.py:667-668` checking for missing runtime profile) that could be pushed to construction time.

**Findings:**
- `config.py:1-135` -- `SemanticConfig` parsed from `SemanticConfigSpec` once via `semantic_config_from_spec()`
- `specs.py:1-157` -- `SpanBinding`, `IdDerivation`, `ForeignKeyDerivation`, `SemanticTableSpec` are all frozen msgspec structs
- `pipeline.py:666-668` -- Runtime validation `if request.runtime_profile is None: raise ValueError` happens inside `cpg_view_specs()` instead of at construction
- `pipeline.py:1369-1371` -- `if validation is None: raise ValueError` validation check deep inside `_execute_cpg_build()`

**Suggested improvement:**
Make `CpgViewNodesRequest` and `CpgViewSpecsRequest` require non-optional `runtime_profile` fields (remove the `None` option), pushing validation to construction time.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
`ViewKind` StrEnum (`view_kinds.py`) and `ViewKindStr` Literal type effectively prevent invalid view kinds at both runtime and type-check time. `GraphPosition` Literal type (`ir.py:16-21`) constrains position values. However, `SemanticDatasetRow` can represent impossible states (e.g., `role="input"` with `supports_cdf=True`, or `materialization="delta"` with `merge_keys=None`).

**Findings:**
- `view_kinds.py:42-63` -- `ViewKind` StrEnum with 14 well-defined values
- `ir.py:16-21` -- `GraphPosition` Literal constrains to 4 valid values
- `catalog/dataset_rows.py:80-102` -- `SemanticDatasetRow` allows `role="input"` with `register_view=True` and `supports_cdf=True`, which is contradictory for input datasets
- `pipeline.py:101-109` -- `CpgBuildOptions` has both `cache_policy` and `compiled_cache_policy` with no indication of when each should be set -- the resolution hierarchy (`_resolve_cache_policy_hierarchy` at line 1042) is implicit

**Suggested improvement:**
Add a `__post_init__` validation to `SemanticDatasetRow` that enforces invariants per `role` (e.g., input rows must have `merge_keys=None`, `register_view=False`). For `CpgBuildOptions`, consider using a union type or tagged variant to make the cache policy source explicit.

**Effort:** medium
**Risk if unaddressed:** medium -- silent construction of invalid dataset rows could cause subtle runtime failures

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. The IR pipeline functions (`compile_semantics`, `infer_semantics`, `optimize_semantics`, `emit_semantics`) are pure queries that return new IR objects. However, `SemanticCompiler.register()` both mutates `_tables` and returns `TableInfo`, mixing command and query.

**Findings:**
- `compiler.py:576-592` -- `register()` mutates `self._tables[name] = info` AND returns `info`
- `compiler.py:594-609` -- `get_or_register()` conditionally mutates AND returns
- `ir_pipeline.py:661-883` -- `compile_semantics()` is a pure function returning new `SemanticIR`
- `ir_pipeline.py:1270-1328` -- `infer_semantics()` is a pure function returning enriched IR

**Suggested improvement:**
Split `register()` into two: `register(name)` (command, returns None) and `get(name) -> TableInfo` (query). The `get_or_register()` pattern could become `ensure_registered(name)` (command) + `get(name)` (query), though the ergonomic cost may not justify the change given the internal usage pattern.

**Effort:** small
**Risk if unaddressed:** low -- the current pattern is pragmatic and contained within the compiler

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
`quality_compiler.py` is an exemplar: `SemanticCompilerLike` protocol at line 45 defines an abstract dependency that `SemanticCompiler` satisfies. However, `pipeline.py` hard-wires dependencies via module-level imports and inline `from X import Y` patterns, and `ir_pipeline.py:28` imports `SEMANTIC_MODEL` singleton directly.

**Findings:**
- `quality_compiler.py:39-72` -- `TableInfoLike` and `SemanticCompilerLike` protocols enable dependency inversion
- `pipeline.py:1359-1364` -- Direct import of `register_view_graph` inside function body -- hidden dependency
- `ir_pipeline.py:28` -- `from semantics.registry import SEMANTIC_MODEL` -- module-level singleton import
- `catalog/dataset_rows.py:111` -- `_SEMANTIC_DATASET_ROWS_CACHE = _SemanticDatasetRowCache()` -- module-level mutable singleton
- `catalog/dataset_specs.py:22` -- `_CACHE = _DatasetSpecCache()` -- another module-level mutable singleton

**Suggested improvement:**
For module-level singletons, provide factory functions that accept optional injected values for testing. The `_SEMANTIC_DATASET_ROWS_CACHE` and `_CACHE` singletons already have `_reset_cache()` functions, but these are test-only escape hatches rather than proper DI. Consider a `CatalogProvider` that can be injected.

**Effort:** medium
**Risk if unaddressed:** medium -- module-level singletons make isolated testing require monkeypatching

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
No class inheritance hierarchies exist in the reviewed scope. All behavior composition is achieved through function composition, protocol satisfaction, and dataclass aggregation.

**Findings:**
- No inheritance chains found. `SemanticCompiler`, `SemanticModel`, `SemanticIR`, `SemanticDatasetRow` are all standalone dataclasses or classes.
- `quality_compiler.py` uses protocol-based structural typing rather than inheritance.

No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code respects Demeter boundaries. A few violations exist in `pipeline.py` where deep accessor chains reach through multiple objects.

**Findings:**
- `pipeline.py:1095` -- `request.runtime_profile.data_sources.semantic_output.cache_overrides` -- 4-deep chain
- `pipeline.py:275` -- `runtime_profile.session_runtime()` called inside `_bundle_for_builder` when the session runtime could be passed directly
- `output_materialization.py:163` -- `write_context.runtime_profile.policies.delta_store_policy` -- 3-deep chain
- `output_materialization.py:173` -- `write_context.runtime_profile.features.enable_schema_evolution_adapter` -- 3-deep chain

**Suggested improvement:**
Pass the specific values needed (e.g., `cache_overrides`, `delta_store_policy`) as parameters to functions rather than reaching through the runtime profile's internal structure.

**Effort:** small
**Risk if unaddressed:** low -- the chains are stable internal APIs

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
`SemanticSchema` (in `schema.py`) encapsulates column discovery rules well, exposing methods like `require_entity()`, `require_evidence()`, `entity_id_expr()` rather than raw data. However, `ir_pipeline.py:455-595` exposes raw tuples of field names in `_build_cpg_output_rows()` and asks external consumers to interpret them.

**Findings:**
- `schema.py` -- `SemanticSchema.require_entity()`, `require_evidence()`, `entity_id_expr()` -- good tell-don't-ask
- `ir_pipeline.py:455-595` -- `_build_cpg_output_rows()` returns raw `SemanticDatasetRow` objects constructed from inline tuples -- the knowledge of what fields belong to each CPG output is exposed as data rather than encapsulated
- `compiler.py:257-260` -- `schema_names()` instance method exists solely to forward to static method, exposing internal delegation

**Suggested improvement:**
Let the CPG schema module (`src/cpg/`) provide a method like `cpg_output_dataset_rows()` that encapsulates the field knowledge, rather than `ir_pipeline.py` hardcoding the field tuples.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The IR pipeline (`ir_pipeline.py`) is a clean functional core: `compile_semantics`, `infer_semantics`, `optimize_semantics`, and `emit_semantics` are all pure functions that take input data and return new IR objects. The imperative shell is in `pipeline.py`, but it mixes orchestration with IO (Delta writes, view registration, artifact recording) rather than isolating them cleanly.

**Findings:**
- `ir_pipeline.py:661-883` -- `compile_semantics()` is purely functional
- `ir_pipeline.py:886-943` -- `optimize_semantics()` is purely functional
- `ir_pipeline.py:946-966` -- `emit_semantics()` is purely functional
- `ir_pipeline.py:1270-1328` -- `infer_semantics()` is purely functional
- `pipeline.py:1330-1442` -- `_execute_cpg_build()` mixes pure orchestration with three IO calls: view registration (line 1392), materialization (line 1417), and diagnostics emission (line 1425)

**Suggested improvement:**
Restructure `_execute_cpg_build()` to first compute all pure results (nodes, bundles, artifacts), then execute IO operations in a separate phase. This would make the pure compilation logic testable without IO mocks.

**Effort:** medium
**Risk if unaddressed:** low -- the current mixing works but makes testing require full DataFusion sessions

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
IR compilation is stateless and idempotent. `compile_semantics(model)` produces the same IR for the same model. `semantic_model_fingerprint()` and `semantic_ir_fingerprint()` produce deterministic hashes. Materialization uses Delta Lake merge semantics with explicit merge keys, supporting idempotent writes.

**Findings:**
- `ir_pipeline.py:99-157` -- `semantic_model_fingerprint()` and `semantic_ir_fingerprint()` use `hash_msgpack_canonical()` for deterministic hashing
- `catalog/dataset_rows.py:87` -- `merge_keys` on `SemanticDatasetRow` enables idempotent Delta merges
- `compile_invariants.py` -- `CompileTracker` prevents duplicate compilation within a session

No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
The pipeline achieves determinism through multiple mechanisms: sorted output ordering in `ir_pipeline.py:930-937`, canonical hashing via `hash_msgpack_canonical`, model and IR fingerprints for cache validation, and `VIEW_KIND_ORDER` for deterministic execution ordering.

**Findings:**
- `ir_pipeline.py:930-937` -- Views sorted by `(VIEW_KIND_ORDER, inputs, name)` for deterministic ordering
- `pipeline.py:258-300` -- `_bundle_for_builder()` computes `semantic_cache_hash` from model + IR + plan hashes
- `view_kinds.py:66-81` -- `VIEW_KIND_ORDER` provides stable execution ordering

No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The IR pipeline is elegantly simple: four phases (compile/infer/optimize/emit) with clear data flow. `naming.py` provides a useful seam for output naming, though the `internal_name()` function at line 64 is a pure identity function that adds no value.

**Findings:**
- `ir_pipeline.py:1331-1350` -- `build_semantic_ir()` is 20 lines orchestrating 4 phases -- clean and simple
- `naming.py:64-78` -- `internal_name()` is a no-op identity function (`return output_name`)
- `naming.py:81-103` -- `output_name_map_from_views()` builds an identity mapping (`view.name -> view.name`)
- `pipeline.py:570-603` -- `_ordered_semantic_specs()` implements topological sort inline rather than using a library

**Suggested improvement:**
Remove `internal_name()` since it is a no-op. The identity mapping functions in `naming.py` are useful as seams for future versioned naming, so they should be retained with a comment explaining the seam purpose.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most abstractions have clear current use cases. Two items appear speculative: `ViewKindParams` in `view_kinds.py` and the `output_policy` field on `SemanticSpecIndex`.

**Findings:**
- `view_kinds.py:127-152` -- `ViewKindParams` struct with 5 fields described as "for future consolidated kinds" -- speculative generality
- `spec_registry.py:43` -- `output_policy: Literal["raw", "v1"] = "v1"` field on `SemanticSpecIndex` -- no code reads this field

**Suggested improvement:**
Mark `ViewKindParams` with a clear "roadmap" comment or remove it until needed. Remove the `output_policy` field from `SemanticSpecIndex` if no consumer uses it.

**Effort:** small
**Risk if unaddressed:** low -- small dead code, not a structural issue

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Most APIs behave as expected. A few naming/behavior surprises exist.

**Findings:**
- `compiler.py:257-260` -- `schema_names(self, df)` instance method that ignores `self` (`_ = self`) and delegates to `SemanticCompiler._schema_names(df)` -- surprising that it is an instance method
- `spec_registry.py` -- Named "spec_registry" but is a pure re-export facade with no registry of its own -- the actual registry is in `registry.py`
- `catalog/dataset_rows.py:281` -- `_reset_cache` exported in `__all__` despite being prefixed with underscore -- mixed signals about public vs internal

**Suggested improvement:**
Make `schema_names` and `prefix_df` static methods (or classmethods) since they don't use `self`. Rename `spec_registry.py` to `spec_index.py` to reflect its actual content (it provides `SemanticSpecIndex`, not a registry). Remove `_reset_cache` from `__all__` or rename to `reset_cache` if it is intentionally public.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
All modules declare `__all__` lists. The `__init__.py` lazy-import facade provides a clear public surface. `SEMANTIC_SCHEMA_VERSION` at `catalog/dataset_rows.py:18` provides schema versioning. However, no explicit stability markers distinguish stable vs unstable APIs.

**Findings:**
- `__init__.py:36-58` -- `__all__` with 18 public symbols
- `catalog/dataset_rows.py:18` -- `SEMANTIC_SCHEMA_VERSION: Final[int] = 1`
- `registry.py:380-400` -- `__all__` with 19 symbols
- `ir_pipeline.py:1353-1361` -- `__all__` with 7 public functions
- No stability markers (e.g., `@public`, `@experimental`) on any API

**Suggested improvement:**
Consider adding docstring annotations like `.. versionadded::` or `.. stability:: stable` for key public APIs (`SemanticCompiler`, `build_cpg`, `SemanticModel`) vs internal helpers.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
`quality_compiler.py` is highly testable due to its protocol-based design -- tests can provide a mock `SemanticCompilerLike` without a DataFusion session. The IR pipeline functions are pure and trivially testable. However, module-level singleton caches in `catalog/` and the `SEMANTIC_MODEL` singleton in `registry.py:378` make isolated testing difficult.

**Findings:**
- `quality_compiler.py:45-72` -- Protocol-based design enables lightweight test doubles
- `ir_pipeline.py:661-1350` -- Pure functions with no hidden state -- trivially testable
- `registry.py:378` -- `SEMANTIC_MODEL: SemanticModel = build_semantic_model()` -- module-level singleton computed at import time
- `catalog/dataset_rows.py:111` -- `_SEMANTIC_DATASET_ROWS_CACHE` -- mutable singleton requiring `_reset_cache()` for testing
- `catalog/dataset_specs.py:22` -- `_CACHE` -- another mutable singleton requiring `_reset_cache()` for testing
- `pipeline.py:311-316` -- Builder closures capture context, making them hard to test in isolation

**Suggested improvement:**
Provide `build_semantic_model()` as the primary API and make `SEMANTIC_MODEL` a convenience alias. For catalog caches, convert to `@functools.lru_cache` or accept an optional injected source. For builder closures, consider making them actual classes with `__call__` to enable inspection in tests.

**Effort:** medium
**Risk if unaddressed:** medium -- testing requires monkeypatching or full-stack setup for what should be unit-level verification

---

#### P24. Observability -- Alignment: 3/3

**Current state:**
Comprehensive OpenTelemetry instrumentation throughout. All compiler operations, pipeline entry points, and quality compilation steps have `stage_span` calls with structured attributes. Artifact recording captures IR fingerprints, view plan stats, and join group statistics.

**Findings:**
- `compiler.py:624-633` -- `stage_span("semantics.normalize_from_spec")` with table, namespace, foreign key count attributes
- `compiler.py:935-946` -- `stage_span("semantics.relate.*")` with left/right table, join type, origin, filter, CDF attributes
- `quality_compiler.py:586-596` -- `stage_span("semantics.compile_relationship_with_quality")` with spec attributes
- `pipeline.py:1479-1489` -- `stage_span("semantics.build_cpg")` with validate, CDF, cache policy attributes
- `pipeline.py:1122-1238` -- `_record_semantic_compile_artifacts()` captures model hash, IR hash, view counts, join group stats

No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

## Cross-Cutting Themes

### Theme 1: `pipeline.py` is a God Module

`pipeline.py` at 1,671 lines accumulates five distinct responsibilities (builder dispatch, view node construction, materialization orchestration, CDF resolution, and artifact recording/report generation). This is the root cause of low scores on P2 (Separation of concerns), P3 (SRP), P4 (cohesion/coupling), and P5 (dependency direction). All five concerns change independently and import from different subsystems, creating a high-fan-out coordination point that is fragile to changes in any dependency.

**Affected principles:** P2, P3, P4, P5, P16, P23
**Root cause:** The module grew organically as the pipeline evolved, accumulating responsibilities that were initially small but have each grown substantial.
**Suggested approach:** Decompose into focused modules: `builder_dispatch.py` (lines 303-1035), `artifact_recording.py` (lines 1122-1327), and keep `pipeline.py` as a thin orchestrator (~400 lines) that coordinates the phases.

### Theme 2: Facade/Re-export Indirection

`spec_registry.py` is a pure re-export facade over `registry.py`, contributing 97 lines that add no logic -- only indirection. Import sites are split between the two modules, creating confusion about which is the authority. This pattern violates P7 (DRY) and P21 (Least astonishment).

**Affected principles:** P7, P21
**Root cause:** Historical migration from a different module structure left the facade in place.
**Suggested approach:** Merge `SemanticSpecIndex` and `SEMANTIC_SPEC_INDEX` into `registry.py`, update import sites, and delete `spec_registry.py`.

### Theme 3: Module-Level Singleton Caches

Three module-level mutable singletons (`SEMANTIC_MODEL` in `registry.py:378`, `_SEMANTIC_DATASET_ROWS_CACHE` in `dataset_rows.py:111`, `_CACHE` in `dataset_specs.py:22`) create hidden global state that requires `_reset_cache()` escape hatches for testing. This pattern resists dependency injection and makes test isolation depend on monkeypatching.

**Affected principles:** P12, P23
**Root cause:** Lazy initialization of expensive computations (IR compilation, dataset row assembly) was expedient but became entrenched.
**Suggested approach:** Replace with `functools.lru_cache` on factory functions, or introduce a `SemanticCatalogProvider` protocol that tests can substitute.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 | Merge `spec_registry.py` into `registry.py` -- eliminate 97-line re-export facade | small | Removes confusion about authority, simplifies imports |
| 2 | P19 | Remove no-op `internal_name()` function in `naming.py:64-78` | small | Reduces dead code and confusion |
| 3 | P21 | Make `schema_names()` and `prefix_df()` static methods on `SemanticCompiler` | small | Eliminates surprising `_ = self` pattern |
| 4 | P20 | Remove unused `output_policy` field from `SemanticSpecIndex` | small | Reduces speculative generality |
| 5 | P22 | Remove `_reset_cache` from `__all__` in `dataset_rows.py` and `dataset_specs.py` | small | Clarifies public vs internal surface |

## Recommended Action Sequence

1. **Merge `spec_registry.py` into `registry.py`** (P7, P21). Move `SemanticSpecIndex`, `SEMANTIC_SPEC_INDEX`, and `_build_semantic_spec_index()` into `registry.py`. Update all import sites. Delete `spec_registry.py`. This is a safe refactor with no behavioral change.

2. **Extract builder dispatch from `pipeline.py`** (P2, P3). Move `_CONSOLIDATED_BUILDER_HANDLERS`, all `_builder_for_*` functions, `_SemanticSpecContext`, and `_semantic_view_specs()` into a new `semantics/builder_dispatch.py`. This reduces `pipeline.py` by ~700 lines and isolates the builder concern.

3. **Extract artifact recording from `pipeline.py`** (P2, P3). Move `_record_semantic_compile_artifacts()`, `_view_plan_stats()`, and `_semantic_explain_markdown()` into `semantics/artifact_recording.py`. This removes ~200 lines and the markdown generation concern.

4. **Replace module-level caches with injectable providers** (P12, P23). Convert `_SemanticDatasetRowCache` and `_DatasetSpecCache` to use `functools.lru_cache` on their factory functions, removing the mutable singleton pattern and the need for `_reset_cache()` escape hatches.

5. **Narrow `SemanticCompilerLike` protocol** (P1, P6). Remove `ctx: SessionContext` from the protocol and add a `table(name: str) -> DataFrame` method instead. Update `quality_compiler.py` to use the method. This hides the session context behind a proper abstraction boundary.

6. **Restructure `_execute_cpg_build` into phases** (P3, P16). Separate the pure computation phase (node building, bundle assembly, artifact preparation) from the IO phase (view registration, materialization, diagnostics emission). This makes the orchestration function testable without IO mocks.

7. **Add `__post_init__` validation to `SemanticDatasetRow`** (P8, P10). Enforce category-specific invariants (e.g., input rows must not have `register_view=True`; output rows must have non-None `template`). This catches invalid dataset row construction early.
