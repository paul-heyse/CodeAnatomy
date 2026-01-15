Below is a comprehensive implementation plan for the remaining PR-05 and PR-06
scope items from `docs/plans/PR5through6.md`. Each scope item includes a
representative code pattern, a target file list, and an implementation
checklist. Per current direction, this plan avoids adding new tests or test
harnesses; validation is captured as runtime diagnostics or manual checks.

---

# PR-05 Remaining Work (Ibis-native normalization and CPG relationships)

## Scope item 5.1 - Span coordinate metadata + canonical normalization policy

**Goal**
Define explicit coordinate system metadata on raw span datasets (line base,
column unit, end exclusivity) and normalize all spans to a single canonical
policy before byte conversion.

**Code pattern**
```python
# registry/spec example: add coord metadata to raw span datasets
SpanCoordSpec = {
    "line_base": 0,          # 0-based line numbers
    "col_unit": "utf32",     # utf8 | utf16 | utf32 | byte
    "end_exclusive": True,
}

# Ibis normalization: canonicalize line/col conventions
line_base = ibis.literal(0)
start_line0 = ibis.ifelse(raw.line_base == 1, raw.start_line - 1, raw.start_line)
end_line0 = ibis.ifelse(raw.line_base == 1, raw.end_line - 1, raw.end_line)
end_exclusive = ibis.ifelse(raw.end_exclusive, raw.end_col, raw.end_col + 1)
```

**Target files**
- `src/extract/registry_templates.py`
- `src/normalize/registry_specs.py`
- `src/normalize/op_specs.py`
- `src/normalize/registry_fields.py`

**Implementation checklist**
- [x] Add coord metadata columns (line_base, col_unit, end_exclusive) to raw
      span datasets in extract templates.
- [x] Update normalize dataset specs to carry these metadata columns forward.
- [x] Canonicalize line base and end exclusivity in span normalization inputs.
- [x] Document the canonical policy alongside dataset contracts.

---

## Scope item 5.2 - Join-driven span normalization in Ibis (AST, bytecode, SCIP, diagnostics)

**Goal**
Replace Python loop-based span conversions with Ibis join-driven normalization
using the line index dataset as the only source of line offsets.

**Code pattern**
```python
line_idx = line_index.select(
    file_id=line_index.file_id,
    line_no=line_index.line_no,
    line_start_byte=line_index.line_start_byte,
    line_text=line_index.line_text,
)

start_idx = line_idx.view()
end_idx = line_idx.view()

joined = raw.join(
    start_idx,
    predicates=[raw.file_id == start_idx.file_id, raw.start_line0 == start_idx.line_no],
    how="left",
)
joined = joined.join(
    end_idx,
    predicates=[raw.file_id == end_idx.file_id, raw.end_line0 == end_idx.line_no],
    how="left",
)

bstart = start_idx.line_start_byte + col_to_byte(
    start_idx.line_text,
    raw.start_col,
    raw.col_unit,
)

bend = end_idx.line_start_byte + col_to_byte(
    end_idx.line_text,
    raw.end_col,
    raw.col_unit,
)
```

**Target files**
- `src/normalize/ibis_spans.py`
- `src/normalize/span_pipeline.py`
- `src/normalize/diagnostics_plans.py`
- `src/normalize/spans.py`
- `src/hamilton_pipeline/modules/normalization.py`

**Implementation checklist**
- [x] Add an Ibis-based span normalization path for AST, bytecode, and SCIP.
- [x] Convert diagnostics span conversion to Ibis join-driven logic.
- [x] Retire Python loop-based span conversion helpers or gate them behind
      legacy-only paths.
- [x] Ensure all normalized outputs align to schema contracts.

---

## Scope item 5.3 - Kernelized col_to_byte with unit-aware semantics

**Goal**
Centralize column-to-byte conversion as a kernel/UDF that understands
`col_unit` and avoids per-row Python logic.

**Code pattern**
```python
@ibis.udf.scalar.builtin(signature=((dt.string, dt.int64, dt.string), dt.int64))
def col_to_byte(line_text: Value, col: Value, unit: Value) -> Value:
    return line_text  # placeholder, real impl in DataFusion UDF / Arrow fallback

# DataFusion UDF registration pattern
@register_kernel("col_to_byte")
def _col_to_byte_kernel(line_text: pa.Array, col: pa.Array, unit: pa.Array) -> pa.Array:
    # vectorized conversion using pyarrow.compute
    ...
```

**Target files**
- `src/datafusion_engine/kernels.py`
- `src/ibis_engine/builtin_udfs.py`
- `src/ibis_engine/expr_compiler.py`
- `src/normalize/ibis_spans.py`

**Implementation checklist**
- [x] Extend `col_to_byte` signature to accept `col_unit`.
- [x] Register a DataFusion UDF implementation (vectorized Arrow arrays).
- [x] Preserve Arrow fallback behavior for non-DataFusion lanes.
- [x] Update all Ibis span normalization to call the centralized kernel.

---

## Scope item 5.4 - Stable ID generation as Ibis expressions everywhere

**Goal**
Ensure all stable IDs are computed inside Ibis plans (not in Python), and
emit natural keys in the output during the migration window.

**Code pattern**
```python
edge_id = stable_id_expr(
    "edge",
    edges.src_id,
    edges.dst_id,
    edges.edge_kind,
    null_sentinel="0",
)

output = edges.mutate(
    edge_id=edge_id,
    edge_src=edges.src_id,
    edge_dst=edges.dst_id,
)
```

**Target files**
- `src/ibis_engine/ids.py`
- `src/normalize/ibis_plan_builders.py`
- `src/normalize/ibis_spans.py`
- `src/normalize/spans.py`
- `src/cpg/emit_edges_ibis.py`
- `src/cpg/relationship_plans.py`

**Implementation checklist**
- [x] Replace any remaining Arrow/Python stable ID generation paths.
- [x] Emit natural keys alongside hashed IDs (debug-only) for normalize Ibis outputs.
- [x] Emit natural keys for CPG edges/relations in Ibis debug outputs.
- [x] Ensure all IDs use the same `stable_hash64`/`stable_hash128` implementation.

---

## Scope item 5.5 - Canonical sort kernel support in Ibis relationship plans

**Goal**
Implement `CanonicalSortKernelSpec` in Ibis to guarantee deterministic ordering
before dedupe and winner selection.

**Code pattern**
```python
def _apply_canonical_sort(table: Table, spec: CanonicalSortKernelSpec) -> Table:
    order_by = [
        table[key.column].asc() if key.order == "ascending" else table[key.column].desc()
        for key in spec.sort_keys
        if key.column in table.columns
    ]
    return table.order_by(order_by) if order_by else table
```

**Target files**
- `src/cpg/relationship_plans.py`
- `src/relspec/model.py`

**Implementation checklist**
- [x] Add `CanonicalSortKernelSpec` handling to the Ibis kernel spec pipeline.
- [x] Ensure dedupe uses canonical sort output, not implicit order.
- [x] Keep Arrow behavior consistent for legacy paths.

---

## Scope item 5.6 - Ibis-only normalize builders (retire Arrow plan builders)

**Goal**
Make Ibis the default normalize path in design phase and avoid dual implementations.

**Code pattern**
```python
if adapter_mode.use_ibis_bridge:
    plans = compile_normalize_plans_ibis(catalog, ctx=ctx, options=ibis_options)
else:
    plans = compile_normalize_plans(catalog, ctx=ctx)
```

**Target files**
- `src/normalize/runner.py`
- `src/normalize/ibis_plan_builders.py`
- `src/normalize/plan_builders.py`

**Implementation checklist**
- [x] Use Ibis builders as the default when adapter mode enables Ibis bridge.
- [x] Deprecate or gate Arrow builders for design-phase runs.
- [x] Ensure diagnostics normalization is Ibis-native end-to-end.

---

## Scope item 5.7 - Ibis relationship joins with interval alignment + coverage diagnostics

**Goal**
Add interval/overlap join helpers for relationship rules and emit ambiguity
coverage diagnostics.

**Code pattern**
```python
same_file = left.file_id == right.file_id
overlaps = (left.bstart < right.bend) & (left.bend > right.bstart)
joined = left.join(right, predicates=[same_file, overlaps], how="inner")

ranked = joined.mutate(score=score_expr(joined))
window = ibis.window(group_by=[ranked.src_id], order_by=[ranked.score.desc()])
winners = ranked.mutate(row_num=ibis.row_number().over(window)).filter(
    ranked.row_num == ibis.literal(1)
)
```

**Target files**
- `src/cpg/relationship_plans.py`
- `src/relspec/rules/handlers/cpg.py`
- `src/relspec/rules/diagnostics.py`

**Implementation checklist**
- [x] Implement interval overlap predicates for Ibis relationship joins.
- [x] Add ambiguity metrics (candidate counts, winner selection rates).
- [x] Ensure output schemas still align to CPG contracts.

---

## Scope item 5.8 - DataFusion runtime policy pack (cache, stats, planning knobs)

**Goal**
Centralize DataFusion configuration (caching, statistics, planning concurrency)
as a single policy surface applied at session creation and recorded in run
artifacts.

**Code pattern**
```python
DATAFUSION_POLICY = {
    "datafusion.runtime.list_files_cache_limit": "128MB",
    "datafusion.runtime.list_files_cache_ttl": "2m",
    "datafusion.runtime.metadata_cache_limit": "256MB",
    "datafusion.execution.collect_statistics": "true",
    "datafusion.execution.meta_fetch_concurrency": "8",
    "datafusion.execution.planning_concurrency": "8",
    "datafusion.execution.parquet.pushdown_filters": "true",
    "datafusion.execution.parquet.max_predicate_cache_size": "64MB",
    "datafusion.execution.parquet.enable_page_index": "true",
    "datafusion.execution.parquet.metadata_size_hint": "1048576",
}

config = SessionConfig()
for key, value in DATAFUSION_POLICY.items():
    config = config.set(key, value)
ctx = SessionContext(config=config)
```

**Target files**
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/compile_options.py`
- `src/hamilton_pipeline/modules/inputs.py`
- `src/obs/manifest.py`
- `src/obs/repro.py`

**Implementation checklist**
- [x] Define a canonical policy map (dev/default/prod tiers).
- [x] Apply policy at SessionContext construction time.
- [x] Record applied policy in run manifests and repro bundles.
- [x] Surface policy in diagnostics artifacts for debugging.

---

## Scope item 5.9 - Ibis DataFusion backend as primary execution lane

**Goal**
Route Ibis expressions directly to the DataFusion backend for execution while
retaining SQLGlot only for lineage/diagnostics and fallback translation.

**Code pattern**
```python
ibis_backend = datafusion_backend(ctx)
expr = compile_rule_to_ibis(...)
sqlglot_expr = ibis_to_sqlglot(expr, backend=ibis_backend, params=params)

try:
    result = expr.to_pyarrow_batches(params=params)
except TranslationError:
    df = df_from_sqlglot_or_sql(ctx, sqlglot_expr, options=options)
    result = df.collect()
```

**Target files**
- `src/ibis_engine/backend.py`
- `src/ibis_engine/query_compiler.py`
- `src/datafusion_engine/bridge.py`
- `src/sqlglot_tools/bridge.py`
- `src/sqlglot_tools/optimizer.py`

**Implementation checklist**
- [x] Use DataFusion backend as the primary executor for Ibis plans.
- [x] Preserve SQLGlot AST for lineage, diff, and diagnostics.
- [x] Keep SQLGlot->DataFusion translation as fallback only.

---

## Scope item 5.10 - Dataset registration upgrades (listing tables + object stores)

**Goal**
Register datasets with listing tables, partition columns, file sort order, and
object store bindings to improve scan planning and performance.

**Code pattern**
```python
ctx.register_object_store("s3://", s3, None)
ctx.register_listing_table(
    "events",
    "s3://bucket/path/",
    file_extension=".parquet",
    table_partition_cols=[("year", pa.int32()), ("month", pa.int32())],
    file_sort_order=[("event_time", "ASC")],
)
```

**Target files**
- `src/ibis_engine/registry.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/hamilton_pipeline/modules/inputs.py`

**Implementation checklist**
- [x] Add listing-table registration when datasets use directory prefixes.
- [x] Declare partition columns and file_sort_order where known.
- [x] Register object stores once per session and reuse across datasets.
- [x] Standardize default Parquet options (pruning, skip_metadata).

---

## Scope item 5.11 - Hot dataset caching policy (DataFrame.cache)

**Goal**
Materialize reuse-heavy normalized datasets once per run for high fanout rule
packs, with explicit cache scope and budget controls.

**Code pattern**
```python
df = ctx.table("type_nodes_v1").select("type_id", "type_repr")
cached = df.cache()
ctx.register_table("type_nodes_cached", cached)
```

**Target files**
- `src/datafusion_engine/bridge.py`
- `src/normalize/runner.py`
- `src/hamilton_pipeline/modules/normalization.py`
- `src/hamilton_pipeline/modules/cpg_build.py`

**Implementation checklist**
- [x] Identify high-reuse datasets (line index, types, SCIP).
- [x] Cache after projection/filters to reduce memory footprint.
- [x] Gate caching behind a runtime profile setting.
- [x] Record cached datasets in diagnostics artifacts.

---

## Scope item 5.12 - Observability artifacts (EXPLAIN, SQLGlot, plan fingerprints)

**Goal**
Emit run artifacts that capture explain plans, canonical SQL, and plan hashes,
plus applied DataFusion policy for debugging and regression analysis.

**Code pattern**
```python
sql = sqlglot_expr.sql(dialect="datafusion")
explain = ctx.sql(f"EXPLAIN ANALYZE {sql}").collect()
plan_fingerprint = hash_sqlglot(sqlglot_expr)
```

**Target files**
- `src/datafusion_engine/runtime.py`
- `src/sqlglot_tools/optimizer.py`
- `src/obs/repro.py`
- `src/obs/manifest.py`

**Implementation checklist**
- [x] Capture canonical SQLGlot SQL per compiled rule.
- [x] Store EXPLAIN ANALYZE output for key rule plans.
- [x] Compute stable plan fingerprints from canonical SQLGlot ASTs.
- [x] Include DataFusion policy + param signatures in artifacts.

---

## Scope item 5.13 - Domain functions via FunctionFactory / custom planning

**Goal**
Introduce a central function registry for domain primitives (stable hashes,
span alignment, scoring) with DataFusion FunctionFactory support where
available, and fall back to UDFs otherwise.

**Code pattern**
```rust
// rust/datafusion_ext/lib.rs (conceptual)
fn register_function_factory(ctx: &SessionContext) {
    ctx.state().register_function_factory(Arc::new(MyFunctionFactory::new()));
}
```

**Target files**
- `rust/datafusion_ext/lib.rs`
- `src/datafusion_engine/function_factory.py`
- `src/datafusion_engine/kernels.py`
- `src/ibis_engine/builtin_udfs.py`

**Implementation checklist**
- [x] Register stable hash + col_to_byte as global functions.
- [x] Use FunctionFactory where available; fallback to UDFs in Python-only mode.
- [x] Keep function volatility metadata explicit (stable vs immutable).

---

## Scope item 5.14 - Shared SessionContext ownership (single execution context)

**Goal**
Ensure Ibis and DataFusion share a single `SessionContext` so object stores,
catalogs, param tables, and views are registered once and visible across all
execution lanes.

**Code pattern**
```python
ctx = SessionContext(config=session_config)
ibis_backend = ibis.datafusion.connect(ctx)

# Use the same ctx for registrations and Ibis execution.
ctx.register_object_store("s3://", store, None)
ibis_backend.create_database("normalize", catalog="codeintel", force=True)
```

**Target files**
- `src/ibis_engine/backend.py`
- `src/datafusion_engine/runtime.py`
- `src/ibis_engine/registry.py`

**Implementation checklist**
- [x] Thread a shared SessionContext through Ibis backend construction.
- [x] Register object stores once per session.
- [x] Register catalogs once per session.
- [x] Avoid creating shadow contexts in adapter code paths.

---

## Scope item 5.15 - View-first execution (lazy rule graph)

**Goal**
Register rule outputs as views by default and only materialize to tables when
explicitly requested.

**Code pattern**
```python
expr = compile_rule_to_ibis(...)
backend.create_view("normalize.type_nodes_v1", expr, overwrite=True)

if materialize:
    backend.create_table("normalize.type_nodes_v1", expr, overwrite=True)
```

**Target files**
- `src/ibis_engine/registry.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/normalize/runner.py`
- `src/cpg/build_edges.py`

**Implementation checklist**
- [x] Default to view registration for rule outputs.
- [x] Add explicit materialization switches for heavy outputs.
- [x] Record view/table choice in diagnostics artifacts.

---

## Scope item 5.16 - Streaming outputs via Arrow C stream

**Goal**
Stream large results without full materialization by using DataFusion’s Arrow
C stream interface.

**Code pattern**
```python
df = ctx.sql(sql, param_values=bindings)
reader = pa.RecordBatchReader.from_stream(df)
return reader
```

**Target files**
- `src/ibis_engine/plan.py`
- `src/ibis_engine/runner.py`
- `src/datafusion_engine/bridge.py`

**Implementation checklist**
- [x] Implement `to_pyarrow_batches` via Arrow C stream.
- [x] Prefer streaming for large outputs in normalize/cpg emission.
- [x] Keep table materialization only for explicit materialize steps.

---

## Scope item 5.17 - Catalog/schema management + information_schema

**Goal**
Use explicit catalog/schema management and optional information_schema
introspection to validate registrations and param tables.

**Code pattern**
```python
backend.create_catalog("codeintel", force=True)
backend.create_database("params", catalog="codeintel", force=True)
ctx.sql("SET datafusion.catalog.information_schema=true").collect()

tables = backend.list_tables(database="params")
```

**Target files**
- `src/ibis_engine/registry.py`
- `src/datafusion_engine/runtime.py`
- `src/obs/manifest.py`

**Implementation checklist**
- [x] Create explicit catalogs and schemas for datasets/params.
- [x] Enable information_schema in diagnostic runs.
- [x] Record catalog/schema inventory in run manifests.

---

## Scope item 5.18 - Data source registration wrappers (Ibis + DataFusion)

**Goal**
Expose consistent read/register helpers for Parquet/CSV/JSON/Delta and
listing-table/dataset registration on the shared context.

**Code pattern**
```python
ibis_backend.read_parquet(path, table_name="events", parquet_pruning=True)
ctx.register_listing_table(
    "events_listing",
    path,
    file_extension=".parquet",
    table_partition_cols=[("year", pa.int32())],
)
```

**Target files**
- `src/ibis_engine/registry.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`

**Implementation checklist**
- [x] Implement Ibis read_* wrappers with explicit options.
- [x] Support listing-table and dataset registration for large prefixes.
- [x] Standardize file sort order and partition column declarations.

---

## Scope item 5.19 - Capability probing for fallback selection

**Goal**
Use backend capability probing to choose DataFusion execution, SQLGlot
fallback, or kernel lane.

**Code pattern**
```python
if backend.has_operation(op_type):
    return expr.to_pyarrow_batches(params=params)
if allow_fallback:
    return df_from_sqlglot_or_sql(ctx, sqlglot_expr, options=options)
return run_kernel_lane(expr, ctx=ctx)
```

**Target files**
- `src/ibis_engine/expr_compiler.py`
- `src/datafusion_engine/bridge.py`
- `src/normalize/runner.py`
- `src/cpg/build_edges.py`

**Implementation checklist**
- [x] Check `has_operation` for unsupported nodes before execution.
- [x] Route unsupported plans to SQLGlot fallback or kernel lane.
- [x] Emit diagnostics when a fallback path is taken.

---

# PR-06 Remaining Work (parameterization + param-table discipline)

## Scope item 6.1 - Scalar ParamOp plumbing (rel_ops -> Ibis params)

**Goal**
Ensure scalar parameters flow from rule `ParamOp` definitions into Ibis param
expressions and runtime bindings.

**Code pattern**
```python
registry = registry_from_ops(rule.rel_ops)
param = registry.register(ScalarParamSpec(name="min_score", dtype="int64"))
filtered = table.filter(table.score >= param)
```

**Target files**
- `src/relspec/rules/rel_ops.py`
- `src/ibis_engine/params_bridge.py`
- `src/relspec/rules/compiler.py`
- `src/hamilton_pipeline/modules/cpg_build.py`

**Implementation checklist**
- [x] Extract scalar ParamOp specs into the Ibis param registry.
- [x] Ensure rule compilation binds params at runtime (no literal thresholds).
- [x] Require stable param names for DataFusion binding.

---

## Scope item 6.2 - List params via param tables + join lowering

**Goal**
Replace any inline list filters with param-table joins and enforce the
no-inline-inlist gate.

**Code pattern**
```python
param_table = param_tables["file_allowlist"]
filtered = base.join(
    param_table,
    predicates=[base.file_id == param_table.file_id],
    how="semi",
)
```

**Target files**
- `src/ibis_engine/param_tables.py`
- `src/hamilton_pipeline/modules/params.py`
- `src/relspec/list_filter_gate.py`
- `src/relspec/rules/validation.py`

**Implementation checklist**
- [x] Ensure list params are always materialized as param tables.
- [x] Add lowering helpers from list ParamOp to join-based filters.
- [x] Enforce list-filter gate in rule validation diagnostics.

---

## Scope item 6.3 - Param table scope policy (per-run/per-session semantics)

**Goal**
Make param table registration respect scope policy and avoid name collisions in
long-lived sessions.

**Code pattern**
```python
if policy.scope == ParamTableScope.PER_SESSION:
    schema = f"{policy.schema}_{session_id}"
else:
    schema = policy.schema
```

**Target files**
- `src/ibis_engine/param_tables.py`
- `src/datafusion_engine/param_tables.py`
- `src/hamilton_pipeline/modules/params.py`

**Implementation checklist**
- [x] Add scope-aware schema naming for PER_SESSION.
- [x] Keep stable logical names via aliases when needed.
- [x] Ensure param tables are re-registered safely per run.

---

## Scope item 6.4 - Param dependency inference wiring + run-bundle diagnostics

**Goal**
Wire param dependency inference into rule compilation outputs and ensure the
active param set is captured in run bundles.

**Code pattern**
```python
deps = infer_param_deps(table_refs, policy=param_policy)
active = ActiveParamSet(frozenset(dep.logical_name for dep in deps))
```

**Target files**
- `src/relspec/param_deps.py`
- `src/relspec/rules/validation.py`
- `src/hamilton_pipeline/modules/params.py`
- `src/obs/repro.py`

**Implementation checklist**
- [x] Ensure dependency reports are emitted during rule diagnostics.
- [x] Use inferred deps to drive active param table registration.
- [x] Record active param tables and signatures in manifests/run bundles.

---

## Scope item 6.5 - Param signature caching + idempotent registration

**Goal**
Avoid re-registering identical param tables and record signatures for
reproducibility across runs.

**Code pattern**
```python
artifact = registry.register_values("file_allowlist", values)
cached = cache.get(artifact.logical_name)
if cached is not None and cached.signature == artifact.signature:
    return cached
cache[artifact.logical_name] = artifact
```

**Target files**
- `src/ibis_engine/param_tables.py`
- `src/datafusion_engine/param_tables.py`
- `src/hamilton_pipeline/modules/params.py`
- `src/obs/manifest.py`

**Implementation checklist**
- [x] Track param table signatures and reuse identical artifacts.
- [x] Skip backend re-registration when signatures match.
- [x] Persist signatures in manifests and repro bundles.

---

## Scope item 6.6 - Plan signature vs param signature separation

**Goal**
Keep plan signatures stable regardless of param values, and bind params only
at execution time while recording param signatures separately.

**Code pattern**
```python
plan_sig = hash_sqlglot(sqlglot_expr)
param_sig = param_signature(logical_name="file_allowlist", values=values)
bindings = registry.bindings(param_bundle.scalar)
```

**Target files**
- `src/sqlglot_tools/optimizer.py`
- `src/ibis_engine/params_bridge.py`
- `src/datafusion_engine/bridge.py`
- `src/obs/repro.py`
- `src/obs/manifest.py`

**Implementation checklist**
- [x] Compute plan signatures from canonical SQLGlot ASTs.
- [x] Record param signatures separately from plan fingerprints.
- [x] Bind params only at execution, not at compile time.

---

## Scope item 6.7 - DDL-based param table registration + information_schema

**Goal**
Use catalog/schema-aware registration and optional DDL pathways for stable
lineage and schema introspection when enabled.

**Code pattern**
```python
ctx.sql("SET datafusion.catalog.information_schema=true").collect()
backend.create_database(policy.schema, catalog=policy.catalog, force=True)
backend.create_table(table_name, artifact.table, database=policy.schema, overwrite=True)
```

**Target files**
- `src/ibis_engine/param_tables.py`
- `src/datafusion_engine/runtime.py`
- `src/ibis_engine/registry.py`

**Implementation checklist**
- [x] Enable information_schema in diagnostic runs where supported.
- [x] Ensure param tables register into explicit catalog/schema.
- [x] Maintain stable qualified names for SQLGlot lineage.

---

# Sequencing (recommended)

1) 5.1 -> 5.2 -> 5.3 (span policy, join-driven normalization, col_to_byte kernel)
2) 5.4 -> 5.5 (stable IDs, canonical sort in Ibis relationship plans)
3) 5.6 -> 5.7 -> 5.14 -> 5.15 -> 5.16 -> 5.17 -> 5.18 -> 5.19
   (Ibis-only normalize path, relationship joins, shared ctx, views, streaming,
   catalog management, registration wrappers, capability probing)
4) 6.1 -> 6.2 -> 6.3 -> 6.4 -> 6.5 -> 6.6 -> 6.7
   (param plumbing, list params, scope policy, deps, signatures, hashing, DDL)
