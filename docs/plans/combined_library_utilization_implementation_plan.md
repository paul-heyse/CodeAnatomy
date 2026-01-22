# Combined Library Utilization Implementation Plan

## Goals
- Maximize DataFusion-native execution (plans, streaming outputs, UDFs, and diagnostics).
- Reduce bespoke orchestration in favor of DataFusion/Ibis/SQLGlot/Delta primitives.
- Make schema, metadata, and plan artifacts first-class, reproducible contracts.
- Push incremental rebuilds and pruning into Delta-native and DataFusion-native surfaces.

## Preconditions / gates
- `scripts/bootstrap_codex.sh` then `uv sync`
- Lint and type gates before final merge:
  - `uv run ruff check --fix`
  - `uv run pyrefly check`
  - `uv run pyright --warnings --pythonversion=3.13`

## Scope 1: Extension parity and named-arg support (FunctionFactory + ExprPlanner + DeltaScanConfig)
Objective: align Python policies with Rust extension signatures and enable named arguments and
domain operators without bespoke fallbacks.

Representative code
```python
# src/datafusion_engine/runtime.py
profile = DataFusionRuntimeProfile(
    enable_expr_planners=True,
    expr_planner_names=("codeanatomy_domain",),
    enable_function_factory=True,
    function_factory_names=("rule_primitives",),
)
ctx = profile.session_context()
```

```rust
// rust/datafusion_ext/src/lib.rs
#[pyfunction]
fn delta_table_provider(
    py: Python<'_>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    file_column_name: Option<String>,
    enable_parquet_pushdown: Option<bool>,
    schema_force_view_types: Option<bool>,
    wrap_partition_values: Option<bool>,
    schema_ipc: Option<Vec<u8>>,
) -> PyResult<PyObject> {
    // build DeltaScanConfig with schema_force_view_types/wrap_partition_values
}
```

Target files
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/expr_planner.py`
- `src/datafusion_engine/function_factory.py`
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_bridge.py`
- `rust/datafusion_ext/src/lib.rs`

Implementation checklist
- [ ] Add missing Rust primitives (`prefixed_hash64`, `stable_id`) to `build_udf`.
- [ ] Implement `install_expr_planners` and register a minimal domain planner.
- [ ] Align `delta_table_provider` signature with Python call sites.
- [ ] Thread `wrap_partition_values` into `DeltaScanOptions` and scan config.
- [ ] Add a runtime profile validation that named-arg support matches extension capability.

## Scope 2: Observability and plan artifacts (metrics + tracing + run envelope)
Objective: capture structured, machine-readable execution metrics and correlation IDs
for every DataFusion run.

Representative code
```python
# src/obs/datafusion_runs.py
def start_run(*, label: str, sink: DiagnosticsSink | None) -> DataFusionRun:
    run_id = str(uuid.uuid4())
    sink.record_artifact(
        "datafusion_run_started_v1",
        {"run_id": run_id, "label": label},
    )
    return DataFusionRun(run_id=run_id, label=label)
```

```rust
// rust/datafusion_ext/src/tracing_ctx.rs
pub fn make_instrumented_state() -> SessionState {
    let options = InstrumentationOptions::builder()
        .record_metrics(true)
        .preview_limit(5)
        .build();
    let rule = instrument_with_info_spans!(options: options);
    SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rule(rule)
        .build()
}
```

Target files
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/bridge.py`
- `src/obs/diagnostics.py`
- `src/obs/diagnostics_tables.py`
- `src/obs/datafusion_runs.py` (new)
- `rust/datafusion_ext/src/tracing_ctx.rs` (new)

Implementation checklist
- [ ] Add a run envelope that records start/finish artifacts and timing.
- [ ] Capture EXPLAIN/EXPLAIN ANALYZE outputs as structured diagnostics.
- [ ] Wire datafusion-tracing spans to `DiagnosticsSink` (OTLP or JSON).
- [ ] Persist plan artifacts (logical, physical, Substrait bytes) with correlation IDs.

## Scope 3: Streaming-first materialization boundary (Arrow C Stream)
Objective: enforce streaming `RecordBatchReader` surfaces for large outputs to avoid
full materialization.

Representative code
```python
# src/datafusion_engine/materialize.py
def df_to_reader(df: DataFrame) -> pa.RecordBatchReader:
    return datafusion_to_reader(df)

def write_parquet_stream(
    reader: pa.RecordBatchReader,
    *,
    path: str,
    options: ParquetWriteOptions,
) -> None:
    write_dataset_parquet(reader, path, options=options)
```

Target files
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/arrowdsl/core/streaming.py`
- `src/arrowdsl/io/parquet.py`
- `src/engine/materialize.py`

Implementation checklist
- [ ] Prefer `__arrow_c_stream__` for DataFusion outputs by default.
- [ ] Gate `to_arrow_table()` behind explicit size checks or debug paths.
- [ ] Ensure writers accept `RecordBatchReader` everywhere a `Table` is accepted.
- [ ] Emit row-count and batch-size diagnostics for streamed outputs.

## Scope 4: Dual-lane compilation (Ibis -> Substrait preferred, SQL fallback)
Objective: reduce SQL string generation by compiling Ibis expressions directly to
Substrait bytes when supported.

Representative code
```python
# src/ibis_engine/substrait_bridge.py
def ibis_to_substrait_bytes(expr: IbisTable) -> bytes:
    compiler = SubstraitCompiler()
    plan = compiler.compile(expr)
    return plan.SerializeToString()

# src/datafusion_engine/bridge.py
def ibis_to_datafusion_dual_lane(...):
    try:
        plan_bytes = ibis_to_substrait_bytes(expr)
        return replay_substrait_bytes(ctx, plan_bytes)
    except Exception:
        return ibis_to_datafusion(expr, backend=backend, ctx=ctx, options=options)
```

Target files
- `src/ibis_engine/substrait_bridge.py` (new)
- `src/datafusion_engine/bridge.py`
- `src/ibis_engine/runner.py`
- `src/engine/plan_cache.py`

Implementation checklist
- [ ] Add Substrait compilation lane with explicit fallback to SQL.
- [ ] Store Substrait bytes in plan cache and diagnostics artifacts.
- [ ] Record per-expression support gaps to inform rule rewrites.
- [ ] Add a Substrait validation gate (optional) with pyarrow.substrait.

## Scope 5: SQLGlot compiler gate + serde artifacts + regression harness
Objective: enforce schema-aware compilation, persist AST artifacts, and add a
rewrite correctness harness.

Representative code
```python
# src/sqlglot_tools/optimizer.py
def preflight_sql(sql: str, *, schema: SchemaMapping | None) -> Expression:
    expr = parse_sql_strict(sql, dialect="datafusion")
    optimized = optimize(expr, schema=schema)
    annotate_types(optimized, schema=schema)
    return canonicalize(optimized)
```

```python
# tests/sqlglot/test_rewrite_regressions.py
def test_rewrite_semantics() -> None:
    tables = {"t": [{"a": "x", "b": 1}, {"a": "y", "b": 2}]}
    result = execute("SELECT a, SUM(b) AS s FROM t GROUP BY a", tables=tables)
    assert sorted(result.rows) == [("x", 1), ("y", 2)]
```

Target files
- `src/sqlglot_tools/optimizer.py`
- `src/ibis_engine/sql_bridge.py`
- `src/ibis_engine/plan_diff.py`
- `src/datafusion_engine/query_fragments.py`
- `tests/sqlglot/test_rewrite_regressions.py` (new)

Implementation checklist
- [ ] Make preflight (qualify + annotate + canonicalize) mandatory for SQL ingress.
- [ ] Persist SQLGlot AST payloads (serde dump) with policy hashes.
- [ ] Add rewrite regression tests using `sqlglot.executor.execute`.
- [ ] Emit structured diagnostics on qualification/type inference failures.

## Scope 6: Schema registry consolidation (DDL-first + info_schema + provider metadata)
Objective: make DDL and information_schema the primary schema contracts and reduce
built-in registry bespoke logic.

Representative code
```python
# src/datafusion_engine/schema_registry.py
def register_external_table(ctx: SessionContext, *, ddl: str) -> None:
    ctx.sql_with_options(ddl, sql_options_for_profile(None)).collect()
    record_table_definition_override(ctx, name=_table_name_from_ddl(ddl), ddl=ddl)
```

Target files
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/catalog_provider.py`
- `src/schema_spec/specs.py`

Implementation checklist
- [ ] Standardize `CREATE EXTERNAL TABLE` for registered datasets.
- [ ] Build schema maps from `information_schema` exclusively.
- [ ] Use `TableProvider` metadata hooks for defaults, constraints, and DDL provenance.
- [ ] Remove bespoke schema snapshots that duplicate information_schema.

## Scope 7: Delta CDF-driven incremental rebuilds
Objective: replace anti-join diffs with Delta Change Data Feed across incremental paths.

Representative code
```python
# src/incremental/delta_cdf.py
def read_cdf_since(*, table_path: str, start_version: int) -> pa.RecordBatchReader:
    table = DeltaTable(table_path)
    return table.load_cdf(starting_version=start_version)
```

Target files
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/incremental/deltas.py`
- `src/incremental/changes.py`
- `src/incremental/state_store.py`

Implementation checklist
- [ ] Store per-dataset CDF cursors in the incremental state store.
- [ ] Register CDF as a DataFusion table provider for SQL/DataFrame access.
- [ ] Define change-type filters (insert/update_postimage/delete) per pipeline.
- [ ] Add idempotency tests to ensure replays do not reapply changes.

## Scope 8: Delta scan customization + file pruning index
Objective: exploit Delta scan config knobs and external file indexes to minimize IO.

Representative code
```python
# src/storage/deltalake/file_index.py
def delta_file_index_table(dt: DeltaTable) -> pa.Table:
    adds = dt.get_add_actions(flatten=True)
    return adds.to_pyarrow_table()
```

```rust
// rust/datafusion_ext/src/delta_pruning.rs
pub fn delta_provider_with_files(
    table: DeltaTable,
    files: Vec<Add>,
) -> DeltaTableProvider {
    DeltaTableProvider::try_new(table.snapshot(), table.log_store(), scan_config)?
        .with_files(files)
}
```

Target files
- `src/storage/deltalake/file_index.py` (new)
- `src/datafusion_engine/registry_bridge.py`
- `rust/datafusion_ext/src/delta_pruning.rs` (new)
- `rust/datafusion_ext/src/lib.rs`

Implementation checklist
- [ ] Build a Delta file-index table from `get_add_actions`.
- [ ] Evaluate filters against the index and select candidate files.
- [ ] Use `DeltaTableProvider.with_files` to restrict scan inputs.
- [ ] Emit pruning diagnostics (candidate count vs total files).

## Scope 9: Cache introspection and runtime config artifacts
Objective: expose list/metadata/statistics cache state as queryable tables and
record cache settings in diagnostics.

Representative code
```python
# src/datafusion_engine/runtime.py
ctx.sql_with_options("SET datafusion.runtime.list_files_cache_ttl = '2m'", opts).collect()
ctx.sql_with_options("SELECT * FROM list_files_cache()", opts).collect()
```

Target files
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_introspection.py`
- `rust/datafusion_ext/src/lib.rs`

Implementation checklist
- [ ] Expose cache introspection table functions in Python SessionContext.
- [ ] Capture cache config and snapshot results in diagnostics tables.
- [ ] Add a contract test that caches are populated after a scan.

## Scope 10: UDF ladder and Ibis IR accelerators (replace bespoke compute)
Objective: prioritize built-ins, Rust UDFs, and Ibis IR primitives over Python or
custom Arrow kernels.

Representative code
```python
# src/ibis_engine/macros.py
def bind_columns(table: Table, *selectors: object) -> tuple[Value, ...]:
    return table.bind(*selectors)

# src/datafusion_engine/udf_registry.py
_ROW_INDEX_UDWF = udwf(RowIndexEvaluator, [pa.int64()], pa.int64(), "stable", "row_index")
```

Target files
- `src/datafusion_engine/udf_registry.py`
- `src/datafusion_engine/function_factory.py`
- `src/ibis_engine/macros.py`
- `src/ibis_engine/expr_compiler.py`
- `src/normalize/runner.py`

Implementation checklist
- [ ] Extend UDF coverage (UDAF/UDWF/UDTF) for remaining bespoke kernels.
- [ ] Prefer `datafusion.functions` over Python/Arrow compute where possible.
- [ ] Use Ibis selectors, deferred expressions, and `Table.bind` in rulepacks.
- [ ] Gate slow Python UDFs behind explicit performance policy checks.

## Legacy decommission targets (post-scope)
These are safe to delete once the listed scopes are implemented and all call sites
are migrated to the new paths.

### Scope 6 (Schema registry consolidation)
- `src/datafusion_engine/schema_introspection.py`: `_TABLE_DEFINITION_OVERRIDES`
- `src/datafusion_engine/schema_introspection.py`: `record_table_definition_override`
- `src/datafusion_engine/schema_introspection.py`: `table_definition_override`
- `src/datafusion_engine/registry_bridge.py`: the `record_table_definition_override(...)` call in
  external-table registration (replaced by TableProvider metadata + information_schema)

### Scope 7 (Delta CDF-only incremental diffs)
- `src/incremental/diff.py`: `diff_snapshots`
- `src/incremental/diff.py`: `_added_sql`
- `src/incremental/diff.py`: `_diff_sql`
- `src/incremental/diff.py`: `_session_context`
- `src/incremental/diff.py`: `_register_table`
- `src/incremental/diff.py`: `_deregister_table`
- `src/hamilton_pipeline/modules/incremental.py`: non-CDF branch that calls `diff_snapshots`

### Scope 3 + 7 (Streaming-first + CDF adoption)
- `src/datafusion_engine/runtime.py`: `read_delta_table_from_path`
- Call sites to migrate then delete (all currently materialize Arrow tables):
  - `src/incremental/scip_snapshot.py`
  - `src/incremental/snapshot.py`
  - `src/incremental/invalidations.py`
  - `src/incremental/fingerprint_changes.py`
  - `src/hamilton_pipeline/modules/incremental.py`
  - `src/hamilton_pipeline/modules/params.py`
  - `src/hamilton_pipeline/modules/outputs.py`
  - `src/hamilton_pipeline/arrow_adapters.py`

### Scope 3 + 10 (Streaming outputs + DataFusion-native compute)
- `src/hamilton_pipeline/arrow_adapters.py`: `ArrowDeltaLoader` / `ArrowDeltaSaver`
  (remove once Hamilton pipelines consume DataFusion/Ibis streaming outputs instead of `pyarrow.Table`)
