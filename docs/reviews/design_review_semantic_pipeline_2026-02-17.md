# Design Review: src/semantics/

**Date:** 2026-02-17
**Scope:** src/semantics/
**Focus:** Knowledge (7-11), Composition (12-15), Correctness (16-18)
**Depth:** moderate
**Files reviewed:** 20 (of 102 total; representative sample per depth=moderate)

Files examined: `compiler.py`, `ir_pipeline.py`, `pipeline_build.py`, `pipeline_builders.py`, `pipeline_cache.py`, `pipeline_dispatch.py`, `ir.py`, `ir_optimize.py`, `entity_model.py`, `entity_registry.py` (via registry), `view_kinds.py`, `ports.py`, `registry.py`, `join_helpers.py`, `normalization_helpers.py`, `span_normalize.py`, `joins/inference.py`, `joins/strategies.py`, `types/core.py`, `types/annotated_schema.py`

---

## Executive Summary

The semantic pipeline is broadly well-engineered. The compile/infer/optimize/emit pipeline in `ir_pipeline.py` is deterministic, clean, and strongly aligned with the functional-core principle. The semantic type system (`types/`) and entity model (`entity_model.py`) are exemplary DRY implementations. The main design gaps are: (1) a mutable table-analysis cache inside `SemanticCompiler` that conflates CQS and makes test setup stateful, (2) confidence-constant duplication between `ir_pipeline.py` and `joins/inference.py`, (3) silent exception swallowing in `infer_semantics()` that hides real failures and erodes observability, (4) the `_tables` mutable dict converting `SemanticCompiler` from a stateless transformer into a stateful registry — complicating reuse and testing. DF52 introduces no breaking changes to current API usage, but the dynamic filtering and `df.cache()` capabilities present concrete optimization opportunities. The byte-span canonicalization path (`span_normalize.py`) and `stable_id` UDF dispatch in `compiler.py` are the strongest Rust migration candidates.

---

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 7 | DRY | 1 | small | medium | Confidence floats duplicated in `ir_pipeline.py` and `joins/inference.py` |
| 8 | Design by contract | 2 | small | low | `infer_semantics()` silently swallows `(AttributeError, KeyError, TypeError, ValueError)` |
| 9 | Parse, don't validate | 3 | — | — | Well-aligned; `AnnotatedSchema` + boundary parsing |
| 10 | Illegal states | 2 | small | low | `JoinStrategy.filter_expr: str | None` and `left_keys` checked only in `__post_init__` |
| 11 | CQS | 1 | medium | medium | `SemanticCompiler.get_or_register()` returns data AND mutates `self._tables` |
| 12 | DI + explicit composition | 2 | small | low | `SemanticCompiler._spec_for_table()` has late import of `semantics.registry` |
| 13 | Composition over inheritance | 3 | — | — | N/A; no inheritance hierarchies |
| 14 | Law of Demeter | 2 | small | low | `pipeline_build.py` chains through several internal helpers to resolve resolver |
| 15 | Tell, don't ask | 2 | small | low | `compiler.py:505` accesses `schema.names` with raw `hasattr` guard instead of schema method |
| 16 | Functional core | 2 | medium | medium | `SemanticCompiler` holds mutable `_tables` dict, mixing IO and state |
| 17 | Idempotency | 2 | small | low | Re-running `register()` on same table name always overwrites; no guard against race |
| 18 | Determinism | 3 | — | — | IR fingerprinting (`semantic_model_fingerprint`, `semantic_ir_fingerprint`) is robust |

---

## Detailed Findings

### Category: Knowledge

#### P7. DRY (knowledge, not lines) — Alignment: 1/3

**Current state:**
Join strategy confidence thresholds are encoded in two places with no shared authority.

**Findings:**
- `src/semantics/joins/inference.py:49-54` defines `_SPAN_CONFIDENCE = 0.95`, `_FK_CONFIDENCE = 0.85`, `_SYMBOL_CONFIDENCE = 0.75`, `_FILE_EQUI_CONFIDENCE = 0.6`.
- `src/semantics/ir_pipeline.py:1086-1091` defines `_STRATEGY_CONFIDENCE: dict[str, float] = {"span_overlap": 0.95, "foreign_key": 0.85, "symbol_match": 0.75, "equi_join": 0.6}` — identical values keyed by string, not by the strategy enum.
- The docstring at `ir_pipeline.py:1083` acknowledges the duplication: `"Confidence scores mirror those in semantics.joins.inference"`. That comment is an admission that a DRY invariant has been split.
- If a confidence value is tuned in `inference.py`, the corresponding value in `ir_pipeline.py` must be manually kept in sync with no compile-time enforcement.

**Suggested improvement:**
Introduce a single `STRATEGY_CONFIDENCE_MAP: dict[JoinStrategyType, float]` constant in `semantics/joins/strategies.py` (already the authority for `JoinStrategyType`). Both `inference.py` and `ir_pipeline.py` import from this single source. The string-keyed dict in `ir_pipeline.py` becomes `{str(k): v for k, v in STRATEGY_CONFIDENCE_MAP.items()}`.

**Effort:** small
**Risk if unaddressed:** medium — silent drift between tuning the live inference path and the IR fingerprinting path.

---

#### P8. Design by contract — Alignment: 2/3

**Current state:**
`infer_semantics()` wraps each view's property inference in a bare multi-exception catch that silently drops all errors.

**Findings:**
- `src/semantics/ir_pipeline.py:1292-1301`:
  ```python
  try:
      props = _infer_view_properties(...)
  except (AttributeError, KeyError, TypeError, ValueError):
      props = None
  ```
- The caught exception set includes `TypeError` and `AttributeError`, which are programming errors, not expected domain failures. A bug in `_infer_view_properties` (e.g., accessing a missing attribute on a wrong type) will be silently converted to `props = None`, resulting in views that execute with no inferred strategy — degrading join quality invisibly.
- No log line is emitted when an exception is caught, violating the observability contract. The outer caller has no way to know that inference partially failed.
- The method's docstring says "Graceful degradation" but does not distinguish between expected partial failure (schema not in index) and unexpected programming errors.

**Suggested improvement:**
Split the expected failure surface from the unexpected one. Expected schema misses (view name not in `schema_index`) are already handled before the exception boundary. Narrow the catch to `KeyError` only (for schema lookups), and re-raise `TypeError` / `AttributeError` after logging a structured warning. Add a `logger.warning(...)` on any caught exception so degradation is visible in telemetry.

```python
try:
    props = _infer_view_properties(...)
except KeyError:
    logger.warning("infer_semantics: schema miss for view %r", view.name)
    props = None
```

**Effort:** small
**Risk if unaddressed:** medium — bugs in `_infer_view_properties` become invisible during normal operation.

---

#### P9. Parse, don't validate — Alignment: 3/3

The `AnnotatedSchema.from_arrow_schema()` and `AnnotatedSchema.from_dataframe()` constructors parse raw Arrow/DataFusion schemas into a typed representation at the boundary. Downstream logic queries `has_semantic_type()` and `has_compatibility_group()` on well-formed values. `SpanNormalizeConfig` parses column config options into a typed `dataclass`. No action needed.

---

#### P10. Make illegal states unrepresentable — Alignment: 2/3

**Current state:**
`JoinStrategy.filter_expr` allows a SQL string that may reference non-existent columns; no validation occurs at construction time.

**Findings:**
- `src/semantics/joins/strategies.py:41-64`: `JoinStrategy.filter_expr: str | None = None`. The `__post_init__` validates key presence and confidence bounds but not `filter_expr` content. An invalid SQL filter passes construction and is only discovered at DataFusion plan time.
- `src/semantics/compiler.py:543-548`: `filter_sql` is applied with `joined.filter(filter_expr)` using the raw string. A typo in a caller-supplied SQL expression fails at execution, not at spec construction.
- `RelationOptions.filter_sql: str | None` has the same characteristic at `compiler.py:169`.

**Suggested improvement:**
For `RelationOptions.filter_sql`, this is a caller boundary parameter and raw SQL is acceptable. However, `JoinStrategy.filter_expr` is an internal IR value: consider changing it from `str | None` to `Expr | None` using DataFusion's `Expr` type, so the filter must be constructed via the DataFusion expression API rather than raw SQL. This converts a runtime parse failure into a construction-time type error.

**Effort:** small
**Risk if unaddressed:** low — errors surface at plan time with clear DataFusion messages.

---

#### P11. CQS — Alignment: 1/3

**Current state:**
`SemanticCompiler.get_or_register()` is the primary access method for tables but both queries the table registry and mutates `self._tables` as a side effect. Callers cannot safely call it for read-only access.

**Findings:**
- `src/semantics/compiler.py:610-625`:
  ```python
  def get_or_register(self, name: str) -> TableInfo:
      if name not in self._tables:
          return self.register(name)  # MUTATES self._tables
      return self._tables[name]
  ```
- `register()` at line 592 mutates `self._tables[name] = info` and returns `TableInfo` — a command that returns a value.
- Every subsequent method (`normalize`, `relate`, `normalize_text`, `aggregate`, `dedupe`, `union_with_discriminator`) delegates to `get_or_register`, so all public API calls accumulate side effects into `self._tables` without the caller being aware.
- The `_tables` dict also serves as a cache, a registry, and a lazy-initialization trigger — three distinct responsibilities in one field.
- This makes `SemanticCompiler` stateful in a way that is not announced at construction time, complicating test isolation (test A's `register()` side effects leak into test B if the same instance is shared).

**Suggested improvement:**
Separate the query path from the mutation path. The simplest improvement is to rename `get_or_register()` to `_ensure_registered()` (making the side effect obvious) and provide a pure `get(name: str) -> TableInfo` that raises `KeyError` if not registered. The pipeline builders that construct `SemanticCompiler` instances per-invocation are not affected; any caller that expected lazy registration explicitly becomes explicit. Alternatively, move `_tables` initialization to the pipeline orchestration layer (`pipeline_builders.py`) and pass a pre-populated `dict[str, TableInfo]` to `SemanticCompiler`'s constructor, making it stateless.

**Effort:** medium — `get_or_register` is called in many internal methods; each needs auditing.
**Risk if unaddressed:** medium — shared compiler instances in tests or re-entrant builds accumulate stale state silently.

---

### Category: Composition

#### P12. DI + explicit composition — Alignment: 2/3

**Current state:**
`SemanticCompiler._spec_for_table()` performs a late import of `semantics.registry` inside the method body to break a circular import.

**Findings:**
- `src/semantics/compiler.py:268-274`:
  ```python
  def _spec_for_table(self, table_name: str) -> SemanticTableSpec | None:
      config_spec = self._config.spec_for(table_name)
      if config_spec is not None:
          return config_spec
      from semantics.registry import spec_for_table  # late import
      return spec_for_table(table_name)
  ```
- The late import is a symptom of a circular dependency: `compiler.py` wants to use `registry.py`, but `registry.py` imports from `compiler.py` (via the `quality_compiler` chain). This pattern hides the architectural tension rather than resolving it.
- The registry lookup is effectively a hidden dependency that is not injectable, making it impossible to test `SemanticCompiler` with a custom spec registry without patching module globals.

**Suggested improvement:**
Accept an optional `spec_registry: Callable[[str], SemanticTableSpec | None]` parameter in `SemanticCompiler.__init__`. Callers that want the default registry pass `spec_for_table` explicitly. This dissolves the circular import while making the dependency visible and injectable. The circular import root should be investigated separately — the registry should not know about the compiler's internals.

**Effort:** small
**Risk if unaddressed:** low — late imports work correctly but mask the circular dependency.

---

#### P13. Composition over inheritance — Alignment: 3/3

The module uses composition throughout. `SemanticCompiler` composes `TableInfo`, `JoinStrategy`, `RelationOptions`, etc. No inheritance hierarchies exist. `SemanticCompilerLike` and `SessionPort` in `ports.py` use `Protocol` rather than abstract base classes. No action needed.

---

#### P14. Law of Demeter — Alignment: 2/3

**Current state:**
`pipeline_build.py`'s internal resolution chain traverses multiple levels of collaborator internals.

**Findings:**
- `src/semantics/pipeline_build.py:213-214`:
  ```python
  early_resolver = execution_context.dataset_resolver
  manifest = execution_context.manifest
  model = execution_context.model
  ```
  Then passes `early_resolver` to `_resolve_semantic_input_mapping(...)`. The function accesses three attributes of `execution_context` and then passes sub-objects to helpers that access those sub-objects' sub-attributes. This is acceptable at a single level but the chain extends through `_resolve_cpg_compile_artifacts` → `_resolve_semantic_input_mapping` → internal dataset catalog calls.
- `src/semantics/compiler.py:222`: `self.ctx = _resolve_session_context(ctx)` stores both the original `ctx` (in `self._ctx`) and the resolved concrete `SessionContext` (in `self.ctx`). Callers can access either. The dual reference is a minor Demeter violation where the internal vs. external representation is exposed simultaneously.

**Suggested improvement:**
Remove `self._ctx` from `SemanticCompiler.__init__` (it is only used in the port-resolution path which is already delegated to `_resolve_session_context`). Store only `self.ctx` (the resolved concrete context). This eliminates the exposed raw port handle.

**Effort:** small
**Risk if unaddressed:** low — the dual storage is confusing but not functionally harmful.

---

#### P15. Tell, don't ask — Alignment: 2/3

**Current state:**
Multiple locations in `compiler.py` use `hasattr(schema, "names")` guards instead of calling a stable method.

**Findings:**
- `src/semantics/compiler.py:279-281`:
  ```python
  schema = df.schema()
  if hasattr(schema, "names"):
      return tuple(schema.names)
  return tuple(field.name for field in schema)
  ```
- `src/semantics/compiler.py:1107-1110` contains an identical pattern for `union_with_discriminator`.
- Rather than asking the schema object what interface it supports, the caller should tell a wrapper to return column names. A `_schema_names()` static method already exists at line 276 that encodes this guard — but it is called inconsistently. The `union_with_discriminator` method reimplements the same guard inline instead of reusing `_schema_names`.

**Suggested improvement:**
`_schema_names()` is already the right abstraction. Remove the duplicated inline implementation in `union_with_discriminator` (lines 1107-1110) and replace it with `self._schema_names(df)`. This is a two-line fix that eliminates semantic duplication.

**Effort:** small
**Risk if unaddressed:** low — duplicate logic risks divergence if the `hasattr` guard ever needs changing.

---

### Category: Correctness

#### P16. Functional core, imperative shell — Alignment: 2/3

**Current state:**
`SemanticCompiler` accumulates mutable state in `_tables` and `_udf_snapshot`, making the core transformation logic impure.

**Findings:**
- `src/semantics/compiler.py:223-224`:
  ```python
  self._tables: dict[str, TableInfo] = {}
  self._udf_snapshot: dict[str, object] | None = None
  ```
- `_tables` grows as methods are called. A `SemanticCompiler` instance invoked twice with different view sets carries stale state from the first invocation into the second. The pipeline code avoids this by constructing fresh instances per builder invocation (`pipeline_builders.py:93-96`), but this is a convention, not an enforcement.
- `_udf_snapshot` is populated lazily on first `_require_udfs()` call and cached permanently. If UDFs are registered after the compiler is constructed and then `_require_udfs()` is called, the snapshot is stale. This is a correctness hazard if the compiler lifetime spans UDF registration.
- The compile/infer/optimize/emit pipeline in `ir_pipeline.py` is fully functional — pure functions that take `SemanticIR` + `SemanticModel` and return new `SemanticIR`. This is excellent and should be the model for the compiler layer as well.

**Suggested improvement:**
Lift the `_udf_snapshot` population to constructor time (capture the UDF registry state at construction, fail fast if required UDFs are absent). For `_tables`, either make the compiler stateless by accepting a pre-populated `Mapping[str, TableInfo]` at construction, or at minimum document the lifetime contract explicitly and add an `assert_clean()` method that verifies `_tables` is empty before first use in pipelines.

**Effort:** medium — changing constructor semantics requires auditing all construction sites.
**Risk if unaddressed:** medium — stale UDF snapshots and leaking table state are latent correctness bugs.

---

#### P17. Idempotency — Alignment: 2/3

**Current state:**
`SemanticCompiler.register()` unconditionally overwrites an existing `TableInfo` entry for the same name.

**Findings:**
- `src/semantics/compiler.py:592-608`:
  ```python
  def register(self, name: str) -> TableInfo:
      df = self.ctx.table(name)
      info = TableInfo.analyze(name, df, config=self._config)
      self._tables[name] = info  # always overwrites
      return info
  ```
- `get_or_register()` guards against re-registration by checking `if name not in self._tables`, but direct callers of `register()` will silently overwrite.
- There is no check that the new `TableInfo` is semantically compatible with the existing one. If `register()` is called after the underlying DataFusion table has been replaced (e.g., after `ctx.deregister_table` + `ctx.register_record_batches`), the old `TableInfo` in `_tables` could become stale without detection.

**Suggested improvement:**
Add an early-exit guard to `register()`:
```python
if name in self._tables:
    return self._tables[name]
```
If intentional re-registration (after table mutation) is needed, provide an explicit `reregister(name: str) -> TableInfo` method with documented semantics. This makes intentional re-registration visible at call sites.

**Effort:** small
**Risk if unaddressed:** low — the current pipeline creates fresh compiler instances so stale state is not triggered in practice.

---

#### P18. Determinism / reproducibility — Alignment: 3/3

The `ir_pipeline.py` pipeline produces `semantic_model_fingerprint` and `semantic_ir_fingerprint` via `hash_msgpack_canonical`, which provides stable byte-order determinism. View ordering uses `VIEW_KIND_ORDER` (a `Final` dict) plus `sorted()` with a stable sort key (line 921-927 in `ir_pipeline.py`). `infer_semantics()` iterates `ir.views` in insertion order (Python dict/tuple ordering is stable). No random seed or timestamp inputs are visible in any of the reviewed paths. No action needed.

---

## Cross-Cutting Themes

### 1. SemanticCompiler as mixed-role object

`SemanticCompiler` attempts to be simultaneously: a stateless plan factory, a lazy table analyzer, a UDF validator, and a mutable table registry. The `_tables` dict and `_udf_snapshot` field are the root of violations across P11 (CQS), P16 (functional core), and P17 (idempotency). The pipeline builders already work around this by constructing fresh compiler instances per invocation, but the underlying type continues to accrue state-related complexity. The root fix is to separate the stateless plan-building logic from the lazy-registration cache. This would reduce `SemanticCompiler`'s responsibility to one: transform table specifications into DataFusion DataFrame operations.

**Affected principles:** P11, P16, P17
**Root cause:** Convenience-driven accumulation of responsibilities into a single class over time.
**Suggested approach:** Extract `TableRegistry` (the `_tables` dict + `get_or_register`) into a separate object. Pass it to `SemanticCompiler` as an injected dependency. The compiler becomes a pure function container; the registry handles lazy analysis.

---

### 2. Confidence constant duplication

The confidence thresholds for join strategy inference appear in two modules that are independently maintained. The comment at `ir_pipeline.py:1083` is a code smell that acknowledges the problem. This will grow if new strategy types are added to `JoinStrategyType` — the `ir_pipeline.py` string dict must be manually extended in sync.

**Affected principles:** P7
**Root cause:** `ir_pipeline.py` performs a lightweight schema-based inference using only field names (not a live `SessionContext`), so it cannot import the full `inference.py` result types directly without a heavier dependency. The solution is to export the constants from `strategies.py` (which has no problematic dependencies) rather than duplicating them.

---

### 3. Silent inference failure

The `except (AttributeError, KeyError, TypeError, ValueError)` catch in `infer_semantics()` is too broad and produces no telemetry. Combined with the fact that `InferredViewProperties.inferred_join_strategy` being `None` is treated as "no strategy inferred" (not "inference failed"), a programming error in `_infer_view_properties` will degrade join quality silently in production.

**Affected principles:** P8, P24 (Observability — not in scope for this review but noted)
**Root cause:** The original intent was graceful degradation for schema-miss cases, but the exception catch was over-generalized.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 | Move confidence constants to `strategies.py`; import in both consumers | small | Eliminates silent drift between inference tuning and fingerprinting |
| 2 | P15 | Remove duplicated `hasattr(schema, "names")` inline in `union_with_discriminator` | small | Reduces code duplication, improves maintainability |
| 3 | P8 | Narrow exception catch in `infer_semantics()` to `KeyError`; add `logger.warning` | small | Converts silent bugs into observable events |
| 4 | P17 | Add early-exit guard to `register()` to prevent silent overwrite | small | Makes re-registration explicit and safe |
| 5 | P12 | Accept optional `spec_registry` callable in `SemanticCompiler.__init__` | small | Eliminates late import, makes dependency injectable |

---

## Recommended Action Sequence

1. **[P7 — small]** Extract confidence constants to `semantics/joins/strategies.py` as `STRATEGY_CONFIDENCE_MAP: dict[JoinStrategyType, float]`. Update `inference.py` and `ir_pipeline.py` to import from there. Remove the acknowledging comment in `ir_pipeline.py`.

2. **[P8 — small]** Narrow the exception catch in `infer_semantics()` (`ir_pipeline.py:1300`) to `KeyError` only. Re-raise `TypeError` and `AttributeError` after emitting a `logger.warning(...)` with `exc_info=True`. This is a prerequisite for detecting bugs in `_infer_view_properties` early.

3. **[P15 — small]** In `SemanticCompiler.union_with_discriminator()` (`compiler.py:1107-1110`), replace the inline `hasattr(schema, "names")` guard with `self._schema_names(df)`.

4. **[P17 — small]** Add `if name in self._tables: return self._tables[name]` as the first two lines of `SemanticCompiler.register()`. Document that `reregister()` is needed for intentional replacement.

5. **[P12 — small]** Add `spec_registry: Callable[[str], SemanticTableSpec | None] | None = None` to `SemanticCompiler.__init__`. Use it in `_spec_for_table` instead of the late import. Update call sites in `pipeline_builders.py` to pass `spec_for_table` explicitly.

6. **[P11 + P16 — medium]** Extract `TableRegistry` from `SemanticCompiler`. The registry owns `_tables` and `get_or_register`. `SemanticCompiler` receives a `TableRegistry` at construction and calls `registry.get(name)`. This makes the compiler stateless and the registry an explicit, injectable collaborator. Address `_udf_snapshot` lifetime in the same pass.

---

## Rust Migration Candidates

### High priority: Byte-span canonicalization

`src/semantics/span_normalize.py:normalize_byte_span_df()` and `normalization_helpers.py:line_index_join()` perform the hottest per-row operation in the pipeline: converting line/column offsets to byte offsets via a join against `file_line_index_v1` and a Rust UDF (`col_to_byte`). The outer join orchestration is currently Python + DataFusion DataFrames, but it always emits the same logical plan shape and the join keys are fully static. This is a strong candidate for:

- A Rust `TableProvider` that pre-joins the line index inline, eliminating the two-phase join (start/end) from Python.
- A Rust `ScalarUDF` or physical plan extension that takes `(start_line, start_col, end_line, end_col)` columns and emits `(bstart, bend)` in a single scan pass using the line index as an internal lookup table.

**Expected LOC reduction:** ~120 Python lines (`span_normalize.py` + relevant `normalization_helpers.py` sections).
**Expected throughput gain:** The Python join construction is called once per view per pipeline run, but the plan it creates executes over all rows. Moving the join to a Rust UDF eliminates the line-index join entirely from the DataFusion plan.

### Medium priority: `stable_id` expression construction

`src/semantics/compiler.py:239-266` (`_stable_id_expr`) and its callers in `normalize_from_spec()` are pure expression builders. The logic that concatenates path/bstart/bend into a stable ID using `concat_ws` + `udf_expr("stable_id", ...)` could be expressed as a single Rust multi-argument UDF `stable_id_from_span(prefix, path, bstart, bend)`, eliminating the Python-side `concat_ws` plan node and reducing plan complexity.

**Expected LOC reduction:** ~20 Python lines per normalization spec.
**Expected throughput gain:** Minor per-row, meaningful at plan-compilation time (fewer DataFusion IR nodes).

### Low priority: View graph ordering in `ir_optimize.py`

The `order_join_groups()` and `prune_ir()` functions in `ir_optimize.py` operate on small Python dataclasses (`SemanticIR`, `SemanticIRView`, `SemanticIRJoinGroup`). These run once at pipeline compile time, not in the hot data path. Rust migration here would yield negligible performance benefit and significant FFI complexity cost. Not recommended.

---

## DF52 Migration Impact

### No breaking changes detected in current usage

The reviewed files use `df.join()`, `df.select()`, `df.with_column()`, `df.filter()`, `df.union()`, `df.drop()`, `df.distinct()`, `ctx.table()`, and `df.schema()`. None of these appear in the DF52 breaking-changes list. The `CoalesceBatchesExec` removal (DF52-H) is transparent to Python consumers. The `DFSchema` API change returning `&FieldRef` (DF52-J) affects Rust embedders, not Python bindings.

### DF52 opportunities

**1. Dynamic filtering for span joins (DF52-B)**

`join_by_span_overlap()` and `join_by_span_contains()` in `join_helpers.py` perform a path equi-join followed by a filter on span overlap. With DF52's expanded hash-join dynamic filtering (build-side hash set → IN predicate pushdown to scan), the path equi-join could automatically prune the scan side when the set of matching `file_id` values is small. This requires no code change — it is an automatic optimizer benefit once DF52 is adopted, provided `file_id` columns are in the join keys (they are, via `FILE_IDENTITY` compatibility group logic).

**2. `df.cache()` for high-fan-out views**

`ir_pipeline.py:_cache_policy_for_position()` detects `"high_fan_out"` views and maps them to `"delta_staging"` cache policy. This requires a Delta write/read round-trip. DF52 (and prior versions) expose `DataFrame.cache()` which materializes results in-memory within a `SessionContext`. For views with high fan-out but small output size (e.g., normalized entity tables that feed multiple relationship views), `df.cache()` would be faster than Delta staging and would not require storage I/O. The existing `CachePolicy` enum (`"none"`, `"delta_staging"`, `"delta_output"`) could gain a `"memory"` variant that calls `df.cache()` instead of writing Delta.

**LOC change:** Additive — a new branch in `pipeline_builders.py`'s cache-policy resolution.
**DF52 obsolescence:** The current Delta-based staging is not obsoleted; `df.cache()` would be an additional option for smaller views.

**3. `TableSchema` for schema handling (DF52-L)**

The `hasattr(schema, "names")` guards in `compiler.py` (`_schema_names` at line 279 and inline at line 1107) are a defensive workaround for schema API variation across DataFusion versions. DF52's `TableSchema` consolidation provides a stable schema abstraction. Once on DF52, the `hasattr` guard could be replaced with a direct `.names` access, as `TableSchema` guarantees the attribute.

---

## Planning-Object Consolidation

### Assessment

The semantic pipeline does **not** exhibit the primary anti-patterns the audit targets:

- **`ctx.sql()` vs. DataFrame API**: All DataFrame construction uses the DataFrame chain API (`df.select()`, `df.join()`, `df.with_column()`), not raw SQL strings. No bespoke SQL construction for internal plumbing was found.
- **Custom view graph compilation**: The IR pipeline (`compile_semantics` → `infer_semantics` → `optimize_semantics` → `emit_semantics`) builds a `SemanticIR` data structure and then separately instantiates DataFusion views through `ctx.register_view()`. This is the intended pattern. It does not attempt to replicate DataFusion's optimizer.
- **Manual join orchestration replacing DataFrame join**: `join_helpers.py` and `normalization_helpers.py` use `df.join()` with explicit `left_on`/`right_on` keys. The span-overlap pattern (path equi-join + `span_overlaps` filter) is semantically different from a simple DataFusion join and cannot be expressed as a single `df.join()` call, so the two-step approach is correct.
- **Bespoke plan caching replacing `df.cache()`**: The `pipeline_cache.py` `"delta_staging"` mechanism uses Delta Lake writes as an explicit caching strategy. This is intentional architectural choice (persistence across process restarts, partitioned by `file_id`). `df.cache()` is a complementary optimization for in-memory cases, not a replacement.
- **18 hardcoded join keys**: The memory note references "18 hardcoded join keys." These are **not present** in the reviewed `compiler.py` or `normalization_helpers.py`. Join keys are inferred dynamically from schema `CompatibilityGroup` membership in `_resolve_join_keys()` (`compiler.py:314-390`) and `_infer_join_keys_from_schemas()` (`ir_pipeline.py:1018-1051`). The `join_keys` fields in `normalize_registry.py` and `dataset_rows.py` are catalog metadata (for DML merge operations), not join key specifications for DataFusion query planning. The join inference system is functioning as designed.

**Conclusion:** No planning-object consolidation opportunities of the type targeted by the audit (bespoke replacements of DF built-ins) were found in the semantic pipeline.
