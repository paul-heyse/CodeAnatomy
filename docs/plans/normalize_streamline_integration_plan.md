# Normalize Streamline Integration Plan (Ibis/DataFusion/SQLGlot)

This plan consolidates normalize execution with the existing DataFusion, Ibis, and SQLGlot
capabilities, aligning diagnostics and span logic to a single canonical approach.

Date: January 24, 2026

Goals
- Use the canonical Ibis backend configuration (no legacy bypass).
- Align SQLGlot diagnostics and plan hashing with the datafusion_compile policy.
- Unify span logic with DataFusion UDF normalization semantics (single path, no fallback).
- Reduce duplication by centralizing plan resolution and API entrypoints.

----------------------------------------------------------------------
Scope 1: Normalize runtime uses canonical Ibis backend configuration
----------------------------------------------------------------------
Status: Implemented (validation pending)
Why
- Today normalize builds the backend directly via ibis.datafusion.connect, bypassing
  ibis_engine backend configuration (object stores, ibis.options).
- This can yield subtle behavior drift from the rest of the pipeline.

Representative pattern
```python
from ibis_engine.execution_factory import ibis_backend_from_ctx, ibis_execution_from_ctx

backend = ibis_backend_from_ctx(ctx)
execution = ibis_execution_from_ctx(ctx, backend=backend)
```

Target files
- src/normalize/runtime.py
- src/normalize/ibis_api.py
- src/normalize/output_writes.py (validation only; should already use runtime backend)
- src/ibis_engine/execution_factory.py (no behavior change; used by normalize)

Implementation checklist
- [x] In build_normalize_runtime, replace ibis.datafusion.connect with ibis_backend_from_ctx.
- [x] Ensure register_datafusion_dialect is still called once per runtime build.
- [x] In _materialize_table_expr, use runtime.execution_ctx instead of reconstructing a new context.
- [ ] Verify normalize runtime picks up object stores and ibis options from ExecutionContext.
- [ ] Run lint/type checks.

Decommission candidates
- src/normalize/runtime.py: direct ibis.datafusion.connect usage.

----------------------------------------------------------------------
Scope 2: SQLGlot diagnostics align with datafusion_compile policy
----------------------------------------------------------------------
Status: Partially implemented (helpers added, runner integration pending)
Why
- Normalize diagnostics currently use resolve_sqlglot_policy() default, which can drift from
  DataFusion compile behavior and plan hashes.
- We want a single policy source: datafusion_compile.

Representative pattern
```python
policy = resolve_sqlglot_policy(name="datafusion_compile")
snapshot = sqlglot_policy_snapshot_for(policy)
schema_map, schema_map_hash = build_schema_map(runtime)

diagnostics = sqlglot_diagnostics(
    plan.expr,
    backend=compiler_backend,
    options=SqlGlotDiagnosticsOptions(
        schema_map=schema_map,
        policy=policy,
    ),
)
plan_hash = plan_fingerprint(
    diagnostics.optimized,
    dialect=policy.write_dialect,
    policy_hash=snapshot.policy_hash,
    schema_map_hash=schema_map_hash,
)
```

Target files
- src/normalize/runner.py
- src/sqlglot_tools/bridge.py (shared helper or new helper module)
- src/datafusion_engine/bridge.py (optional: central diagnostics helper)
- src/sqlglot_tools/optimizer.py (policy snapshot usage, no behavior change)

Implementation checklist
- [x] Create a shared helper to compile SQLGlot diagnostics with the datafusion_compile policy.
- [x] Use SchemaIntrospector schema_map and schema_map_fingerprint consistently with DataFusion.
- [ ] Replace normalize-specific SQLGlot policy resolution with datafusion_compile policy.
- [ ] Update normalize plan artifact payloads to match DataFusion naming/fields (if needed).
- [ ] Remove local SQLGlot helper functions in runner once centralized helper is in place.
- [ ] Add tests that assert normalize plan hashes match DataFusion for the same Ibis expression.

Decommission candidates
- src/normalize/runner.py:
  - _sqlglot_context
  - _sqlglot_schema_map
  - _sqlglot_lineage_payload
  - _sqlglot_ast_payload
  - _record_sqlglot_plan (replace with shared helper wrapper)

----------------------------------------------------------------------
Scope 3: Unify span logic with DataFusion UDF normalization semantics
----------------------------------------------------------------------
Status: Implemented
Why
- Span logic exists in both normalize.ibis_plan_builders and normalize.ibis_spans,
  and DataFusion UDFs implement their own col-unit normalization.
- We want a single canonical logic path for edge cases, without fallbacks.

Representative pattern
```python
# Shared canonical logic (module: normalize/span_logic.py or datafusion_engine/span_logic.py)
def normalize_col_unit_expr(col_unit: Value, *, encoding: Value | None) -> Value:
    # Mirrors DataFusion UDF _normalize_col_unit semantics.
    ...

def span_struct_expr(inputs: SpanStructInputs) -> Value:
    # Single shared implementation used by all Ibis span builders.
    ...

# Usage
col_unit = normalize_col_unit_expr(col_unit_raw, encoding=position_encoding)
byte_start = col_to_byte(line_text, col_offset, col_unit)
```

Target files
- src/normalize/ibis_spans.py
- src/normalize/ibis_plan_builders.py
- src/normalize/ibis_exprs.py
- src/datafusion_engine/udf_registry.py
- src/normalize/text_index.py (constants, if relocated)
- New shared module: src/normalize/span_logic.py or src/datafusion_engine/span_logic.py

Implementation checklist
- [x] Extract canonical col-unit normalization semantics into a shared module.
- [x] Ensure shared logic matches DataFusion UDF behavior (_normalize_col_unit).
- [x] Reuse a single span struct builder across ibis_spans and ibis_plan_builders.
- [x] Remove duplicate span/offset helpers in both modules.
- [x] Align position encoding constants in one module (single source of truth).
- [x] Add targeted tests for span edge cases (byte/utf variants, end_exclusive, line_base).

Decommission candidates
- src/normalize/ibis_spans.py:
  - SpanStructInputs
  - _span_struct_expr
  - _line_base_value
  - _zero_based_line
  - _end_exclusive_value
  - _normalize_end_col
  - _col_unit_value
  - _col_unit_from_encoding
  - _line_offset_expr
- src/normalize/ibis_plan_builders.py:
  - SpanStructInputs
  - _span_struct_expr
  - _line_base_value
  - _zero_based_line
  - _end_exclusive_value
  - _normalize_end_col
  - _col_unit_value
  - _col_unit_from_encoding
  - _line_offset_expr
- src/normalize/ibis_exprs.py: position_encoding_norm_expr (if moved to shared module)

----------------------------------------------------------------------
Scope 4: Consolidate Ibis plan resolution in normalize runner
----------------------------------------------------------------------
Status: Not started (runner integration pending)
Why
- _resolve_rule_plan_ibis re-implements IbisPlanCatalog.resolve_plan behavior.
- Consolidating reduces drift and leverages existing catalog caching.

Representative pattern
```python
plan = catalog.resolve_plan(input_name, ctx=ctx, label=input_name)
if plan is None:
    return None
return plan
```

Target files
- src/normalize/runner.py
- src/ibis_engine/catalog.py (optional: add helper for rule inputs)

Implementation checklist
- [ ] Replace source resolution logic with IbisPlanCatalog.resolve_plan.
- [ ] Keep rule.ibis_builder handling, but use catalog for the source case.
- [ ] Ensure ViewReference and DatasetSource paths are handled by catalog.
- [ ] Remove any redundant materialization logic now covered by catalog.
- [ ] Add a small unit test verifying behavior for ViewReference and DatasetSource.

Decommission candidates
- src/normalize/runner.py: _resolve_rule_plan_ibis (replace with catalog call)

----------------------------------------------------------------------
Scope 5: Consolidate normalize Ibis API entrypoints
----------------------------------------------------------------------
Status: Implemented (tests pending)
Why
- build_* functions in normalize.ibis_api repeat the same pattern.
- A shared builder reduces drift and ensures plan portability + finalize logic is consistent.

Representative pattern
```python
def build_normalize_output(
    output: str,
    inputs: Mapping[str, NormalizeSource],
    *,
    ctx: ExecutionContext | None,
    runtime: NormalizeRuntime | None,
    profile: str = "default",
) -> TableLike:
    normalize_runtime = _require_runtime(runtime)
    exec_ctx = ensure_execution_context(ctx, profile=profile)
    catalog = _catalog_from_tables(normalize_runtime.ibis_backend, tables=dict(inputs))
    builder = resolve_plan_builder_ibis(output)
    plan = builder(catalog, exec_ctx, normalize_runtime.ibis_backend)
    return _finalize_plan(plan, output=output, ctx=exec_ctx, runtime=normalize_runtime)
```

Target files
- src/normalize/ibis_api.py
- src/normalize/task_catalog.py (alignment check)
- src/normalize/ibis_bridge.py (no behavior change; used by generic builder)

Implementation checklist
- [x] Implement a generic build_normalize_output helper in normalize.ibis_api.
- [x] Convert existing build_* functions to call the generic helper with a mapping of inputs.
- [x] Use resolve_plan_builder_ibis so portability fallback is guaranteed.
- [x] Keep public function signatures for backward compatibility.
- [ ] Add small regression tests for one or two outputs (cfg_blocks, diagnostics).

Decommission candidates
- None required; specific build_* functions can stay as thin wrappers.

----------------------------------------------------------------------
Decommission and Deletion Summary (Consolidated)
----------------------------------------------------------------------
Functions to remove (after new shared helpers land)
- src/normalize/runner.py:
  - _sqlglot_context
  - _sqlglot_schema_map
  - _sqlglot_lineage_payload
  - _sqlglot_ast_payload
  - _record_sqlglot_plan (replace with shared helper wrapper)
  - _resolve_rule_plan_ibis
- src/normalize/ibis_spans.py:
  - SpanStructInputs
  - _span_struct_expr
  - _line_base_value
  - _zero_based_line
  - _end_exclusive_value
  - _normalize_end_col
  - _col_unit_value
  - _col_unit_from_encoding
  - _line_offset_expr
- src/normalize/ibis_plan_builders.py:
  - SpanStructInputs
  - _span_struct_expr
  - _line_base_value
  - _zero_based_line
  - _end_exclusive_value
  - _normalize_end_col
  - _col_unit_value
  - _col_unit_from_encoding
  - _line_offset_expr
- src/normalize/ibis_exprs.py:
  - position_encoding_norm_expr (if moved to shared module)

Files to delete
- Completed:
  - src/normalize/ibis_exprs.py
  - src/normalize/text_index.py
