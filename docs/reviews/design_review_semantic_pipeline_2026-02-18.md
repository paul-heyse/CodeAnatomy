# Design Review: src/semantics/

**Date:** 2026-02-18
**Scope:** src/semantics/
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 103

---

## Executive Summary

The semantic pipeline is a notably mature codebase. Its strongest aspects are an
explicit compile/infer/optimize/emit IR pipeline (`ir_pipeline.py`), a clean
ports-and-adapters boundary in `ports.py`, deep use of immutable `msgspec.Struct`
and frozen dataclasses for all spec types, and a well-tested join inference engine
with quantified confidence scores. The most significant gaps are: (1) a dual-spec
problem — `RelationshipSpec` in `specs.py` and `QualityRelationshipSpec` in
`quality.py` coexist with incomplete migration, causing DRY and CQS friction; (2)
`SemanticCompiler.get_or_register` violates CQS by performing registration as a
side-effect of a query; (3) the `_STRATEGY_CONFIDENCE` constants are duplicated
verbatim between `joins/inference.py` and `ir_pipeline.py`, creating a silent drift
risk for the determinism contract; and (4) `pipeline_build.py` imports private
underscore-prefixed symbols from `pipeline_builders.py`, leaking internal coupling
across a module boundary.

---

## Alignment Scorecard

| #  | Principle                         | Alignment | Effort | Risk   | Key Finding                                                         |
|----|-----------------------------------|-----------|--------|--------|---------------------------------------------------------------------|
| 1  | Information hiding                | 2         | small  | low    | Private `_pipeline_builders` symbols imported into `pipeline_build` |
| 2  | Separation of concerns            | 3         | —      | —      | IR pipeline cleanly separates compile/infer/optimize/emit           |
| 3  | SRP                               | 2         | medium | low    | `SemanticCompiler` owns both compilation and registry side-effects  |
| 4  | High cohesion, low coupling       | 3         | —      | —      | Subpackages have tight internal cohesion                            |
| 5  | Dependency direction              | 3         | —      | —      | Core logic imports nothing from IO or storage                       |
| 6  | Ports & Adapters                  | 3         | —      | —      | `ports.py` exposes clean protocol surface                           |
| 7  | DRY                               | 1         | medium | medium | Confidence constants duplicated; two relationship spec types coexist |
| 8  | Design by contract                | 3         | —      | —      | Explicit preconditions in compiler, schema, joins                   |
| 9  | Parse, don't validate             | 3         | —      | —      | `AnnotatedSchema.from_arrow_schema` converts at boundary            |
| 10 | Make illegal states unrepresentable| 3        | —      | —      | Frozen dataclasses, `StructBaseStrict`, enums throughout            |
| 11 | CQS                               | 1         | medium | medium | `get_or_register` and `TableRegistry.resolve` violate CQS          |
| 12 | Dependency inversion              | 3         | —      | —      | `SemanticCompilerLike` protocol in quality_compiler                 |
| 13 | Composition over inheritance      | 3         | —      | —      | No inheritance hierarchies; composition used everywhere             |
| 14 | Law of Demeter                    | 2         | small  | low    | Several chained attribute accesses through IR properties            |
| 15 | Tell, don't ask                   | 2         | small  | low    | `ir_pipeline.py` asks for inferred properties to make decisions     |
| 16 | Functional core, imperative shell | 3         | —      | —      | Pure IR transformations; IO at materialization boundary             |
| 17 | Idempotency                       | 3         | —      | —      | CDF protocol, merge keys, idempotent view registration              |
| 18 | Determinism / reproducibility     | 2         | small  | medium | Confidence constants duplicated; `_hash_string("unknown_plan")` fallback |
| 19 | KISS                              | 2         | medium | low    | `ir_pipeline.py` is 1359 lines; `compile_semantics` has 100+ line body |
| 20 | YAGNI                             | 3         | —      | —      | Abstractions are justified by active use                            |
| 21 | Least astonishment                | 2         | small  | low    | `canonical_output_name` is now an identity function; naming is misleading |
| 22 | Declare and version public contracts | 2      | small  | low    | `__all__` used, but `RelationshipSpec` vs `QualityRelationshipSpec` split is undocumented |
| 23 | Design for testability            | 2         | medium | medium | `SemanticCompiler` requires a live `SessionContext`; no pure-logic test seam |
| 24 | Observability                     | 3         | —      | —      | `stage_span` with structured attributes on every major operation    |

---

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding — Alignment: 2/3

**Current state:**
`pipeline_build.py` imports five private (underscore-prefixed) symbols directly from
`pipeline_builders.py`, bypassing whatever public contract `pipeline_builders` might
expose.

**Findings:**
- `src/semantics/pipeline_build.py:38-41`: imports `_bundle_for_builder`,
  `_cache_policy_for`, `_finalize_output_builder`, `_normalize_cache_policy`,
  `_ordered_semantic_specs`, `_semantic_view_specs`, `_SemanticSpecContext` — seven
  private names crossed through a module boundary.
- `src/semantics/pipeline_build.py:301`: `normalization_by_output = {spec.output_name: spec
  for spec in SEMANTIC_NORMALIZATION_SPECS}` re-derives a dict that already lives as
  `_NORMALIZATION_BY_OUTPUT` inside `registry.py:104`, bypassing the accessor
  `normalization_spec_for_output()`.

**Suggested improvement:**
Promote the six private helpers in `pipeline_builders.py` that are consumed by
`pipeline_build.py` to a small, explicitly named public surface (e.g., move them to
a new `_build_helpers` module or rename without leading underscore and add to
`__all__`). Replace the ad-hoc dict reconstruction in `pipeline_build.py:301` with a
call to `normalization_spec_for_output()` from the registry.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns — Alignment: 3/3

The IR pipeline cleanly separates policy (compile/infer/optimize/emit phases), domain
transforms (join inference, span normalization), and IO concerns (materialization
delegated to `output_materialization.py`). The `ports.py` module isolates the
engine boundary. No action needed.

---

#### P3. SRP — Alignment: 2/3

**Current state:**
`SemanticCompiler` in `compiler.py` is responsible for: (a) table-info caching via
`get_or_register`, (b) all 10 normalization rules, (c) join strategy dispatch, (d)
union and aggregation, (e) deduplication, and (f) quality-relationship delegation.
While each responsibility is well-scoped within the file, the class has six distinct
change reasons.

**Findings:**
- `src/semantics/compiler.py:203-1365`: single class with 600+ lines of method bodies
  covering normalization, relationship building, deduplication, aggregation, and
  quality compilation.
- `src/semantics/compiler.py:615-631`: `get_or_register` performs both lookup and
  registration — two responsibilities in one method (also a CQS issue, see P11).

**Suggested improvement:**
Extract the table-info cache layer into `TableRegistry` more completely (it already
exists as `table_registry.py` but is only called via `resolve`). Consider extracting
a `SemanticNormalizer` class for rules 1-3 and a `SemanticJoiner` for rules 5-7, with
`SemanticCompiler` orchestrating both. This reduces the compiler's reason-to-change
surface.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P4-P6. High cohesion, Dependency direction, Ports & Adapters — Alignment: 3/3

The subpackage structure is coherent: `joins/` owns strategy inference, `incremental/`
owns CDF operations, `types/` owns the annotated schema type system, `catalog/` owns
view builder registries. `ports.py` provides protocol abstractions (`SessionPort`,
`DataFramePort`, `SessionContextProviderPort`, `OutputWriterPort`). Core logic
(`ir.py`, `specs.py`, `quality.py`, `exprs.py`) has no runtime IO imports.

---

### Category: Knowledge (7-11)

#### P7. DRY — Alignment: 1/3

**Current state:**
Two concrete violations:

1. **Confidence constants duplicated across modules.** The span/FK/symbol/equi-join
   confidence thresholds are defined twice with identical numeric values in two
   modules:

   - `src/semantics/joins/inference.py:49-55`: `_SPAN_CONFIDENCE = 0.95`,
     `_FK_CONFIDENCE = 0.85`, `_SYMBOL_CONFIDENCE = 0.75`, `_FILE_EQUI_CONFIDENCE = 0.6`.
   - `src/semantics/ir_pipeline.py:1088-1093`: `_STRATEGY_CONFIDENCE = {"span_overlap":
     0.95, "foreign_key": 0.85, "symbol_match": 0.75, "equi_join": 0.6}` — same four
     numeric values in a different form.

   If the join model's confidence calibration is updated in one file, it will silently
   diverge from the other, breaking reproducibility guarantees.

2. **Dual relationship spec types.** `src/semantics/specs.py:75-147` defines
   `RelationshipSpec` and `src/semantics/quality.py:236-340` defines
   `QualityRelationshipSpec`. Both represent relationship declarations. The comment in
   `quality.py:246-248` acknowledges the coexistence: *"This is a NEW TYPE that
   coexists with the existing RelationshipSpec in specs.py."* The registry
   (`registry.py:196-201`) uses only `QualityRelationshipSpec`, but `pipeline_build.py:48`
   still imports `RelationshipSpec` from `specs.py` and re-exports it through `__all__`.
   `pipeline_builders.py:13` also imports `RelationshipSpec`. The spec is no longer
   in active use for the primary relationship registry but exists as a partially live
   type.

**Suggested improvement:**
1. Extract the confidence constants to a single canonical location — the natural home
   is `src/semantics/joins/inference.py` since that module owns the inference semantics.
   Re-import them in `ir_pipeline.py` instead of redefining.
2. Complete the migration from `RelationshipSpec` to `QualityRelationshipSpec`. Once
   no callers remain, deprecate and remove `RelationshipSpec` from `specs.py`. Remove
   its import from `pipeline_build.py:48` and `pipeline_builders.py:13`.

**Effort:** medium (confidence constants: small; spec migration: medium)
**Risk if unaddressed:** medium (silent numeric drift in determinism contract)

---

#### P8. Design by contract — Alignment: 3/3

Preconditions are enforced explicitly:
- `src/semantics/compiler.py:306-316`: `_ensure_columns_present` raises
  `SemanticSchemaError` with specific missing column lists.
- `src/semantics/compiler.py:418-463`: `_validate_join_keys` checks key count parity
  and semantic type compatibility.
- `src/semantics/joins/inference.py:82`: `JoinInferenceError` raised on failure with
  diagnostic details.
- `src/semantics/schema.py:233-291`: `require_evidence`, `require_entity`,
  `require_symbol_source`, `require_span_unit` each provide named precondition
  enforcement.

No action needed.

---

#### P9. Parse, don't validate — Alignment: 3/3

Conversion happens at the boundary:
- `src/semantics/types/annotated_schema.py:52`: `AnnotatedSchema.from_arrow_schema`
  converts Arrow schemas to typed, annotated structures at construction time.
- `src/semantics/schema.py:69-177`: `SemanticSchema.from_df` analyzes a DataFrame
  once, emitting a typed `SemanticSchema`; all downstream code works with the
  structured result.
- `src/semantics/config.py:100-127`: `semantic_config_from_spec` parses a serializable
  spec into a runtime config with compiled `re.Pattern` objects.

No action needed.

---

#### P10. Make illegal states unrepresentable — Alignment: 3/3

- All spec types (`SemanticTableSpec`, `SpanBinding`, `IdDerivation`,
  `RelationshipSpec`, `QualityRelationshipSpec`) use `StructBaseStrict(frozen=True)`.
- Enums used for strategy types (`JoinStrategyType`), merge strategies
  (`CDFMergeStrategy`), view kinds (`ViewKindStr`), graph positions (`GraphPosition`).
- `RelationOptions.__post_init__` (compiler.py:175-178) enforces mutual exclusion
  between `join_type` and `strategy_hint`.
- `IncrementalRuntimeBuildRequest` enforces the profile-or-service precondition via
  `ValueError` (incremental/runtime.py:67-72).

No action needed.

---

#### P11. CQS — Alignment: 1/3

**Current state:**
Two systematic CQS violations:

1. **`SemanticCompiler.get_or_register`** (compiler.py:615-631) is named as a query
   ("get") but performs registration as a side-effect when the table is absent. The
   same pattern appears in `register` (compiler.py:597-613) — both delegate to
   `TableRegistry.resolve`, which also combines get and register.

2. **`TableRegistry.resolve`** (table_registry.py:42-49): "Return existing table info
   or register a newly-created one" — the docstring itself documents the CQS
   violation. This pattern forces callers to either always call `resolve` (mixing
   query+command) or split into two calls (losing atomicity).

The consequence is that any caller invoking `get_or_register` for a read-only purpose
(e.g., schema inspection, diagnostics) silently mutates the registry.

**Suggested improvement:**
Separate the concepts. `TableRegistry.get(name)` is the pure query (already exists).
Add a `register_if_absent(name, factory)` command that is clearly a mutation. Rename
`SemanticCompiler.get_or_register` to `ensure_registered` to signal command intent,
or split: `get_table_info(name)` for pure query (raises if absent) and
`register_table(name)` for explicit registration at setup time. The lazy-populate
pattern can be kept in `ensure_registered` if needed, but it should be clearly named.

**Effort:** medium
**Risk if unaddressed:** medium (test isolation, reasoning about state)

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition — Alignment: 3/3

- `quality_compiler.py:44-72`: `SemanticCompilerLike` protocol is a structural
  abstraction over `SemanticCompiler`, used by all quality compilation helpers.
  `SemanticCompiler` is never imported at runtime in `quality_compiler.py`.
- `compiler.py:210-233`: `SemanticCompiler.__init__` accepts `TableRegistry` as an
  explicit injection parameter — dependency is not silently created.
- `ports.py`: `SessionPort`, `DataFramePort`, `UdfResolverPort`, `SchemaProviderPort`,
  `OutputWriterPort` — all engine-specific protocols are inverted.

No action needed.

---

#### P13. Composition over inheritance — Alignment: 3/3

No inheritance hierarchies detected. All behavior extension is via composition:
`SemanticCompiler` delegates quality compilation to helpers in `quality_compiler.py`
rather than subclassing. `AnnotatedSchema` wraps `AnnotatedColumn` tuples without
inheritance. No action needed.

---

#### P14. Law of Demeter — Alignment: 2/3

**Current state:**
Several call sites chain through intermediate objects in ways that expose structure:

**Findings:**
- `src/semantics/ir_pipeline.py:1192-1193`: `schema_index.get(left_name)` on an
  index that is itself built from `model.relationship_specs` iterated through
  `_dataset_rows_for_model(model)`. The path from `model` through several intermediate
  collections before reaching `AnnotatedSchema` is indirect but contained.
- `src/semantics/compiler.py:365-376`: `left_info.annotated.infer_join_keys(...)` and
  then `left_info.annotated.get(name)` for `CompatibilityGroup` — caller navigates
  `TableInfo.annotated.get(name).compatibility_groups`, crossing two object levels.
- `src/semantics/incremental/runtime.py:104-135`: `registry_facade` property navigates
  `self._dataset_resolver.names()`, `self._dataset_resolver.location(name)`,
  `self.profile.udf_catalog(ctx)`, `self.profile.function_factory_policy_hash(ctx)`,
  `self.profile.view_registry` — five different sub-objects of `profile` accessed from
  a single method.

**Suggested improvement:**
For `incremental/runtime.py:99-135`, extract a `build_registry_facade(runtime)` free
function that hides the construction logic and accepts the runtime as a single
collaborator. For the join-key resolution in compiler.py, consider adding
`TableInfo.infer_join_keys(other: TableInfo) -> list[tuple[str,str]]` so the
compiler only talks to `TableInfo` rather than `TableInfo.annotated`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask — Alignment: 2/3

**Current state:**
`ir_pipeline.py` asks for inferred properties (`view.inferred_properties`) and then
makes decisions about those properties externally. The view itself is a passive data
bag.

**Findings:**
- `src/semantics/ir_pipeline.py:1163-1172`: checks `view.kind not in {"relate",
  "join_group"}` and then conditionally builds `InferredViewProperties` — the kind
  discrimination is done externally.
- `src/semantics/ir_pipeline.py:1174-1190`: asks whether `rel_spec is not None`,
  whether `len(view.inputs) >= 2`, and routes accordingly — external ask pattern.
- `src/semantics/compiler.py:533-553`: `if strategy.strategy_type ==
  JoinStrategyType.SPAN_OVERLAP ... elif strategy.strategy_type ==
  JoinStrategyType.SPAN_CONTAINS ... else` — asking the strategy type to dispatch, when
  `JoinStrategy` could provide a method for execution.

**Suggested improvement:**
Add a `is_join_kind(self) -> bool` or `requires_schema_inference(self) -> bool`
predicate to `SemanticIRView`. For the strategy dispatch in compiler.py, add a
`execute_join(left_df, right_df, left_sem, right_sem)` method to `JoinStrategy` so
the compiler tells it to execute rather than asking its type.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell — Alignment: 3/3

The IR pipeline (`compile_semantics`, `infer_semantics`, `optimize_semantics`,
`emit_semantics`) consists of pure functions returning new `SemanticIR` values with no
IO. DataFusion plan construction is lazy (plans are not executed, only built).
Materialization is isolated to `output_materialization.py` and triggered at the
`build_cpg` boundary. No action needed.

---

#### P17. Idempotency — Alignment: 3/3

- `TableRegistry.register` raises on duplicate registration, preventing accidental
  double-registration.
- CDF cursors in `incremental/cdf_cursors.py` track versions to make incremental reads
  re-runnable.
- `SemanticIncrementalConfig.merge_strategy` allows UPSERT and REPLACE strategies for
  idempotent materialization.
- `semantic_model_fingerprint` and `semantic_ir_fingerprint` (ir_pipeline.py:101-158)
  produce stable hashes for plan caching.

No action needed.

---

#### P18. Determinism / reproducibility — Alignment: 2/3

**Current state:**
Two gaps:

1. **Confidence constants are duplicated**, not referenced (P7 cross-reference). When
   `_STRATEGY_CONFIDENCE` in `ir_pipeline.py` drifts from `_SPAN_CONFIDENCE` etc. in
   `joins/inference.py`, the confidence score emitted on `SemanticIRView` will differ
   from the score used at join time. Since `InferenceConfidence` flows into plan
   bundles, this is a correctness issue for determinism-sensitive consumers.

2. **Fingerprint fallback to `"unknown_plan"`** in `plans/fingerprints.py:223`: when
   both logical plan and unoptimized plan extraction fail, `_compute_logical_plan_hash`
   returns `_hash_string("unknown_plan")`. This means two genuinely different plans can
   produce the same fingerprint if both fail plan extraction. The fallback is a constant
   string, not a nonce, so it silently conflates different plan states.

**Suggested improvement:**
1. Centralize confidence constants (see P7 recommendation).
2. In `plans/fingerprints.py:223`, replace `"unknown_plan"` with a per-invocation
   fallback incorporating at least the `view_name` and a monotonic counter, e.g.,
   `_hash_string(f"unknown_plan:{view_name}:{id(df)}")`. This does not restore true
   determinism but at least prevents silent conflation.

**Effort:** small
**Risk if unaddressed:** medium (determinism contract, plan caching correctness)

---

### Category: Simplicity (19-22)

#### P19. KISS — Alignment: 2/3

**Current state:**
`ir_pipeline.py` is 1359 lines with a deeply nested `compile_semantics` function (220
lines, lines 655-876) that manually constructs `SemanticIRView` for every view kind
using hard-coded conditional logic. It contains both the view graph construction logic
and the inference/optimization/fingerprinting code. The function body crosses the 200
line threshold.

**Findings:**
- `src/semantics/ir_pipeline.py:655-876`: `compile_semantics` builds views in a large
  sequential block. Each view kind (normalize, scip_normalize, bytecode_line_index,
  span_unnest, symtable, relate, union_edges, union_nodes, projection, diagnostic,
  export, finalize) is handled by ad-hoc inline code or `_emit_output_spec`. There is
  no dispatch table or extensible registry for view kind constructors.
- `src/semantics/ir_pipeline.py:790-823`: `export_inputs` and `diagnostic_inputs` are
  local dicts keyed by spec name, hardcoding which views feed which diagnostics —
  knowledge that arguably belongs in the view catalog or registry.

**Suggested improvement:**
Extract a small `_ViewBuilder` protocol or dict mapping `ViewKindStr` to a factory
function. Each factory receives the model and returns `SemanticIRView`. This reduces
`compile_semantics` to a loop over view-kind factories. The `diagnostic_inputs` and
`export_inputs` dicts can move to `catalog/diagnostics_registry.py` and
`catalog/export_registry.py` respectively, making the registry the single source of
truth for each category.

**Effort:** medium
**Risk if unaddressed:** low (maintainability; adding new view kinds requires editing
a 200-line function)

---

#### P20. YAGNI — Alignment: 3/3

The `DataFramePort` and `SessionPort` protocols in `ports.py` are minimally scoped.
The `ExprContext` protocol in `exprs.py` has exactly one implementation (`ExprContextImpl`)
plus `ExprValidationContext` which has active use for validation. `EntityDeclaration`
reduces to `SemanticTableSpec` without adding unused fields. The `quality_templates.py`
factory is justified by the four active `entity_symbol_relationship` invocations. No
speculative abstractions detected.

---

#### P21. Least astonishment — Alignment: 2/3

**Current state:**
Two naming issues create astonishment:

**Findings:**
- `src/semantics/naming.py:33-61`: `canonical_output_name(internal_name)` returns
  `internal_name` unchanged (identity function) unless a manifest with an
  `output_name_map` is provided — and even then, the map is built as identity pairs
  (see `output_name_map_from_views:99-103`). The function name implies a
  transformation but the default behavior is a no-op. The name `_v1` suffix convention
  referenced in the module docstring no longer applies.
- `src/semantics/naming.py:64-78`: `internal_name(output_name)` unconditionally returns
  `output_name` unchanged. This function appears to exist for symmetry but the symmetry
  is vacuous — both directions are identity functions.
- `src/semantics/compiler.py:597-613` and `615-631`: `register` and `get_or_register`
  both delegate to `_registry.resolve` with an identical body. They are
  indistinguishable in behavior (both are effectively `get_or_register`) but have
  different names, astonishing callers who expect `register` to unconditionally write.

**Suggested improvement:**
Rename `canonical_output_name` to `view_output_name` (or document clearly that it is
an identity function by default). Remove `internal_name` since it is an identity
function. Clarify the compiler's `register`/`get_or_register` pair — if they are
truly identical, remove one.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts — Alignment: 2/3

**Current state:**
`__all__` is consistently present in all modules. However, the coexistence of
`RelationshipSpec` (specs.py) and `QualityRelationshipSpec` (quality.py) without an
explicit migration plan or deprecation signal in `specs.py` creates an unclear public
contract. Callers cannot determine which type is current without reading `quality.py:246`.

**Findings:**
- `src/semantics/specs.py:75-147`: `RelationshipSpec` is exported in `__all__` but no
  deprecation warning or comment signals it is in the process of being replaced.
- `src/semantics/pipeline_build.py:48` and `__all__:685`: `RelationshipSpec` is
  re-exported from `pipeline_build.__all__`, giving it the appearance of a primary
  stable surface when it is not.

**Suggested improvement:**
Add a `DeprecationWarning` to `RelationshipSpec.__init_subclass__` or to its module
docstring, and open a migration plan issue. Remove `RelationshipSpec` from
`pipeline_build.__all__` since it is not used in the CPG relationship registry.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability — Alignment: 2/3

**Current state:**
The IR pipeline (`compile_semantics`, `infer_semantics`, `optimize_semantics`) is
fully pure and testable without any infrastructure. Similarly, all `ExprSpec`
callables in `exprs.py`, `JoinStrategy` inference in `joins/inference.py`, and all
spec types are trivially unit-testable.

However, `SemanticCompiler` requires a live `SessionContext` for construction (all
normalization and join operations call `self.ctx.table(name)` and UDF expressions). The
`TableInfo.analyze` classmethod calls `AnnotatedSchema.from_dataframe(df)` which
requires a real DataFusion `DataFrame`. There is no `SemanticCompilerLike` that covers
the normalization path (only quality compilation has the `SemanticCompilerLike`
protocol).

**Findings:**
- `src/semantics/compiler.py:203-233`: constructor requires `SessionContext | SessionPort`
  — no lightweight fake path exists for testing normalization logic without DataFusion.
- `src/semantics/compiler.py:234-242`: `_require_udfs` validates UDFs are available —
  requires UDF registration in the session before any normalize/relate call can be tested.
- `src/semantics/compiler.py:614`: `TableInfo.analyze(name, self.ctx.table(name), ...)` —
  even `get_or_register` lazily calls `ctx.table()`, which requires a registered table.

**Suggested improvement:**
Introduce a `SemanticCompilerLike` protocol (parallel to `quality_compiler.py`'s
existing protocol) that covers the normalization methods (`normalize_from_spec`,
`normalize`, `relate`). This allows unit tests to supply a fake that operates on
pre-built DataFrames without a session, and makes the `SemanticCompiler` testable by
protocol coverage alone.

**Effort:** medium
**Risk if unaddressed:** medium (tests require full DataFusion session setup, increasing test
brittleness and runtime)

---

#### P24. Observability — Alignment: 3/3

Every meaningful operation in `SemanticCompiler` is wrapped in `stage_span` with
structured `codeanatomy.*` attributes:
- `src/semantics/compiler.py:646-654`: `semantics.normalize_from_spec` with table name,
  namespace, and foreign key count.
- `src/semantics/compiler.py:756-763`: `semantics.normalize` with table name and prefix.
- `src/semantics/compiler.py:813-823`: `semantics.normalize_text` with column count and
  form.
- `src/semantics/compiler.py:957-968`: `semantics.relate.*` with join type, origin, CDF
  flag.

`build_cpg` and `build_cpg_from_inferred_deps` each emit a parent span. The
`SemanticModel` fingerprint and `SemanticIR` fingerprint are recorded in artifacts.
Diagnostic views (`relationship_quality_metrics`, `relationship_ambiguity_report`)
provide structured quality telemetry. No action needed.

---

## Cross-Cutting Themes

### Theme 1: Incomplete spec-type migration

**Root cause:** `QualityRelationshipSpec` was introduced as an additive replacement for
`RelationshipSpec`, but the migration was never completed. The old type remains exported
through `pipeline_build.__all__` and imported in `pipeline_builders.py`, keeping it
artificially alive and causing DRY violations (P7), contract ambiguity (P22), and
unnecessary cognitive load.

**Affected principles:** P7, P11, P21, P22

**Suggested approach:** Audit all import sites of `RelationshipSpec` (approximately 4
files). Confirm none feed into `RELATIONSHIP_SPECS` in `registry.py`. Add a module-level
deprecation notice to `specs.py`. Remove `RelationshipSpec` from `pipeline_build.__all__`.
Target removal after one release cycle.

---

### Theme 2: Confidence constant duplication undermining the determinism contract

**Root cause:** The numeric thresholds for join strategy confidence were introduced in
`joins/inference.py` and then redefined independently in `ir_pipeline.py` when the IR
inference phase was added. Since both paths are authoritative in their respective
contexts, numeric divergence is possible with no compile-time signal.

**Affected principles:** P7, P18

**Suggested approach:** Consolidate all confidence constants into
`src/semantics/joins/inference.py` (the authority for join inference). Export them
via `joins/__init__.py`. Import in `ir_pipeline.py` by name. This creates a single
source of truth and makes divergence a visible import error rather than a silent
mismatch.

---

### Theme 3: CQS violation at the table-registry boundary

**Root cause:** The `get_or_register` / `resolve` pattern was introduced for
convenience (lazy population during compilation), but it makes the registry's mutation
surface implicit. Every query has a potential side-effect, making isolated testing and
audit of registration order difficult.

**Affected principles:** P3, P11, P23

**Suggested approach:** Rename `TableRegistry.resolve` to `ensure_and_get(name,
factory)` with a clear docstring that states it is a command that may register. Add a
pure `get(name)` for read-only callers. Rename `SemanticCompiler.get_or_register` to
`ensure_registered(name)` to signal command intent at the call site.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 / P18 | Centralize confidence constants from `joins/inference.py` and `ir_pipeline.py` into a single exported dict | small | Prevents silent numeric drift in determinism contract |
| 2 | P22 / P7 | Remove `RelationshipSpec` from `pipeline_build.__all__`; add deprecation notice in `specs.py` | small | Clears contract ambiguity; first step to P7 cleanup |
| 3 | P18 | Replace `_hash_string("unknown_plan")` fallback in `plans/fingerprints.py:223` with view-name-qualified string | small | Prevents silent plan fingerprint conflation |
| 4 | P1 | Remove `RelationshipSpec` import from `pipeline_build.py:48` (follows from priority 2) | small | Eliminates cross-module private-boundary import |
| 5 | P21 | Rename `internal_name` (vacuous identity) and document `canonical_output_name` as identity-by-default | small | Reduces naming astonishment for new contributors |

---

## Recommended Action Sequence

1. **Centralize confidence constants** (P7, P18). In `src/semantics/joins/inference.py`,
   export `CONFIDENCE_BY_STRATEGY: dict[str, float]`. Import and use it in
   `src/semantics/ir_pipeline.py` instead of `_STRATEGY_CONFIDENCE`.

2. **Deprecate `RelationshipSpec`** (P7, P22). Add a deprecation notice in
   `src/semantics/specs.py`. Remove it from `pipeline_build.__all__` and from
   `pipeline_builders.py` imports. Confirm `RELATIONSHIP_SPECS` in `registry.py` does
   not reference it.

3. **Fix the plan fingerprint fallback** (P18). In `src/semantics/plans/fingerprints.py:223`,
   replace `"unknown_plan"` with `f"unknown_plan:{view_name}"`.

4. **Rename CQS-violating methods** (P11, P3). In `src/semantics/table_registry.py`,
   rename `resolve` to `ensure_and_get`. In `src/semantics/compiler.py`, rename
   `get_or_register` to `ensure_registered`. Update all call sites (confined to
   `compiler.py` itself).

5. **Introduce `SemanticCompilerLike` for normalization path** (P23). Define a
   protocol in `quality_compiler.py` or a new `compiler_protocol.py` that covers
   `normalize_from_spec` and `relate`. Use it in internal helpers to enable unit testing
   without a live `SessionContext`.

6. **Decompose `compile_semantics`** (P19). Extract per-kind view factory functions
   into a dispatch dict or factory registry in `ir_pipeline.py`. Move
   `diagnostic_inputs` and `export_inputs` to their respective catalog registry modules.

7. **Fix Law of Demeter violations** (P14). In `incremental/runtime.py:99-135`,
   extract `_build_registry_facade(runtime)` free function. In `compiler.py:365-376`,
   add `TableInfo.infer_join_keys(other)` method.
