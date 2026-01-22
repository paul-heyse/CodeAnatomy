# Combined Library Utilization Gap Closure Plan

## Goals
- Close the remaining implementation gaps identified in the post-plan review.
- Align Python orchestration with Rust extension capabilities (named args, UDFs, Delta scan config).
- Make run envelopes, tracing, and plan artifacts first-class and consistently correlated.
- Activate dual-lane Substrait compilation at runtime and persist lanes in caches/diagnostics.
- Enforce SQL preflight and persist SQLGlot AST artifacts for reproducibility.
- Enable Delta file pruning with `with_files` and surface cache introspection tables.
- Retire remaining legacy/duplicate paths once replacements are live.

## Preconditions / gates
- `scripts/bootstrap_codex.sh` then `uv sync`
- Lint and type gates before final merge:
  - `uv run ruff check --fix`
  - `uv run pyrefly check`
  - `uv run pyright --warnings --pythonversion=3.13`

## Scope 1: Rust extension parity (FunctionFactory + ExprPlanner + DeltaScanConfig)
Objective: align Rust extension signatures and capabilities with Python expectations to make named
args and Delta scan configuration reliable.

Representative code
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
    let mut scan_builder = DeltaScanConfigBuilder::new();
    if let Some(value) = schema_force_view_types {
        scan_builder = scan_builder.with_schema_force_view_types(value);
    }
    if let Some(value) = wrap_partition_values {
        scan_builder = scan_builder.with_wrap_partition_values(value);
    }
    // ... build provider and return capsule
}

#[pyfunction]
fn install_expr_planners(ctx: PyRef<PySessionContext>, planners: Vec<String>) -> PyResult<()> {
    install_named_arg_planners(&ctx.ctx, planners)?;
    Ok(())
}
```

```rust
// rust/datafusion_ext/src/lib.rs
fn build_udf(primitive: &RulePrimitive, prefer_named: bool) -> Result<ScalarUDF> {
    let signature = primitive_signature(primitive, prefer_named)?;
    match primitive.name.as_str() {
        "prefixed_hash64" => Ok(ScalarUDF::from(Arc::new(PrefixedHash64Udf { signature }))),
        "stable_id" => Ok(ScalarUDF::from(Arc::new(StableIdUdf { signature }))),
        // existing primitives...
        name => Err(DataFusionError::Plan(format!("Unsupported rule primitive: {name}"))),
    }
}
```

Target files
- [ ] `rust/datafusion_ext/src/lib.rs`
- [ ] `src/datafusion_engine/registry_bridge.py`
- [ ] `src/datafusion_engine/expr_planner.py`
- [ ] `src/datafusion_engine/runtime.py`
- [ ] `src/datafusion_engine/function_factory.py`
- [ ] `test_scope2_implementation.py`

Implementation checklist
- [ ] Extend `delta_table_provider` signature and thread new options into `DeltaScanConfigBuilder`.
- [ ] Add missing rule primitives (`prefixed_hash64`, `stable_id`) to Rust UDF dispatch.
- [ ] Implement and export `install_expr_planners` from `datafusion_ext`.
- [ ] Add a runtime parity validation hook that records extension capability state.
- [ ] Add/extend tests to assert extension hooks are discoverable.

## Scope 2: Run envelope + tracing integration
Objective: create a run envelope with `run_id` propagation, and wire tracing to diagnostics sinks.

Representative code
```python
# src/datafusion_engine/runtime.py
with tracked_run(label=label, sink=self.diagnostics_sink) as run:
    options = replace(resolved, run_id=run.run_id)
    df = ibis_to_datafusion_dual_lane(expr, backend=backend, ctx=ctx, options=options)
```

```rust
// rust/datafusion_ext/src/tracing_ctx.rs
#[pyfunction]
fn install_tracing(ctx: PyRef<PySessionContext>) -> PyResult<()> {
    let state = make_instrumented_state();
    ctx.ctx.set_state(state);
    Ok(())
}
```

Target files
- [ ] `src/obs/datafusion_runs.py`
- [ ] `src/datafusion_engine/runtime.py`
- [ ] `src/datafusion_engine/bridge.py`
- [ ] `src/obs/diagnostics.py`
- [ ] `rust/datafusion_ext/src/lib.rs`
- [ ] `rust/datafusion_ext/src/tracing_ctx.rs` (new)

Implementation checklist
- [ ] Introduce a tracked run wrapper for compilation/execution entry points.
- [ ] Propagate `run_id` into `DataFusionCompileOptions` and plan artifacts.
- [ ] Expose a `datafusion_ext.install_tracing(ctx)` hook and call it from runtime.
- [ ] Record run start/finish artifacts plus tracing span summaries to diagnostics.
- [ ] Add tests verifying run IDs appear in plan artifact payloads.

## Scope 3: Dual-lane Substrait activation in the runner
Objective: actually use dual-lane compilation when `prefer_substrait=True` and persist lane info.

Representative code
```python
# src/ibis_engine/runner.py
if execution.datafusion is not None:
    result = ibis_to_datafusion_dual_lane(
        portable_plan.expr,
        backend=execution.backend,
        ctx=execution.ctx,
        options=options,
    )
    _maybe_store_substrait_lane(result, options=options)
    return result.df
```

```python
# src/datafusion_engine/bridge.py
def _maybe_store_substrait_lane(result: DualLaneCompilationResult, *, options: DataFusionCompileOptions) -> None:
    if result.substrait_bytes is None or options.plan_cache is None:
        return
    key = _plan_cache_key(compile_sqlglot_expr(...), options=options)
    options.plan_cache.put(
        PlanCacheEntry(
            plan_hash=key.plan_hash,
            profile_hash=key.profile_hash,
            plan_bytes=result.substrait_bytes,
            compilation_lane=result.lane,
        )
    )
```

Target files
- [ ] `src/ibis_engine/runner.py`
- [ ] `src/datafusion_engine/bridge.py`
- [ ] `src/engine/plan_cache.py`
- [ ] `test_dual_lane_integration.py`

Implementation checklist
- [ ] Route DataFusion-backed execution through `ibis_to_datafusion_dual_lane`.
- [ ] Persist Substrait bytes and lane metadata into `PlanCacheEntry`.
- [ ] Record fallback reasons when Substrait lane fails.
- [ ] Extend integration tests to assert runtime selection of Substrait lane.

## Scope 4: SQLGlot preflight enforcement + AST artifacts
Objective: enforce schema-aware preflight across SQL ingress and persist AST artifacts.

Representative code
```python
# src/datafusion_engine/bridge.py
result = preflight_sql(sql, schema=options.schema_map, dialect=options.dialect, strict=True, policy=policy)
if options.enforce_preflight and result.errors:
    raise ValueError(f"Preflight validation failed: {'; '.join(result.errors)}")
if options.sql_ingest_hook is not None:
    options.sql_ingest_hook(emit_preflight_diagnostics(result))

ast_artifact = serialize_ast_artifact(ast_to_artifact(expr, sql=sql, policy=policy))
plan_artifacts = replace(plan_artifacts, sqlglot_ast=ast_artifact)
```

Target files
- [ ] `src/datafusion_engine/bridge.py`
- [ ] `src/datafusion_engine/compile_options.py`
- [ ] `src/sqlglot_tools/optimizer.py`
- [ ] `tests/sqlglot/test_rewrite_regressions.py`
- [ ] `test_preflight.py`

Implementation checklist
- [ ] Make preflight enforcement opt-out (policy defaults to `True` in runtime profiles).
- [ ] Emit structured preflight diagnostics to the diagnostics sink.
- [ ] Extend `DataFusionPlanArtifacts` to carry serialized SQLGlot AST payloads.
- [ ] Add tests validating AST artifact persistence and preflight gating.

## Scope 5: Delta file pruning with `with_files`
Objective: use Delta add-action file indexes to restrict scans using `with_files`.

Representative code
```python
# src/datafusion_engine/registry_bridge.py
index_table = build_delta_file_index(dt)
candidates = select_candidate_files(index_table, filter_expr=filters)
provider = delta_table_provider_with_files(
    table_uri=str(table_path),
    files=candidates,
    delta_scan=delta_scan,
)
ctx.register_table(table_name, provider)
```

```rust
// rust/datafusion_ext/src/delta_pruning.rs
pub fn delta_provider_with_files(
    snapshot: Snapshot,
    log_store: Arc<dyn LogStore>,
    scan_config: DeltaScanConfig,
    files: Vec<String>,
) -> Result<DeltaTableProvider> {
    DeltaTableProvider::try_new(snapshot, log_store, scan_config)?
        .with_files(files)
        .map_err(DataFusionError::from)
}
```

Target files
- [ ] `src/storage/deltalake/file_index.py`
- [ ] `src/storage/deltalake/file_pruning.py`
- [ ] `src/datafusion_engine/registry_bridge.py`
- [ ] `rust/datafusion_ext/src/delta_pruning.rs` (new)
- [ ] `rust/datafusion_ext/src/lib.rs`

Implementation checklist
- [ ] Implement a Rust `delta_table_provider_with_files` and export it via `datafusion_ext`.
- [ ] Replace `_delta_table_provider_with_files` placeholder with native call.
- [ ] Add candidate file selection and pruning diagnostics.
- [ ] Add regression tests for file count reductions and correctness.

## Scope 6: Cache introspection table functions
Objective: expose cache introspection as queryable tables and persist cache settings.

Representative code
```python
# src/datafusion_engine/cache_introspection.py
def register_cache_introspection_functions(ctx: SessionContext) -> None:
    module = importlib.import_module("datafusion_ext")
    register = getattr(module, "register_cache_tables")
    register(ctx)

# Usage
ctx.sql("SELECT * FROM list_files_cache()").collect()
```

Target files
- [ ] `src/datafusion_engine/cache_introspection.py`
- [ ] `src/datafusion_engine/runtime.py`
- [ ] `rust/datafusion_ext/src/lib.rs`
- [ ] `rust/datafusion_ext/src/cache_tables.rs` (new)

Implementation checklist
- [ ] Implement cache UDTFs (list_files, metadata, predicate) in Rust extension.
- [ ] Wire `register_cache_introspection_functions` into runtime init.
- [ ] Record cache config snapshots to diagnostics artifacts.
- [ ] Add contract tests verifying cache tables return rows after scans.

## Scope 7: Legacy decommission and cleanup
Objective: remove legacy helpers once new paths are live.

Representative code
```python
# src/hamilton_pipeline/modules/incremental.py
reader = read_delta_as_reader(delta_path)
write_incremental_diff_stream(reader, state_store=incremental_state_store)
```

Target files
- [ ] `src/incremental/diff.py` (remove `_diff_snapshots`, `_added_sql`, `_diff_sql`, helpers)
- [ ] `src/hamilton_pipeline/modules/incremental.py` (remove fallback path and legacy calls)
- [ ] `src/hamilton_pipeline/arrow_adapters.py` (remove `ArrowDeltaLoader`/`ArrowDeltaSaver`)
- [ ] `src/hamilton_pipeline/modules/outputs.py`
- [ ] `src/hamilton_pipeline/modules/params.py`

Implementation checklist
- [ ] Remove deprecated diff helpers and update call sites to CDF-only paths.
- [ ] Replace Arrow adapter usages with streaming/DataFusion-native outputs.
- [ ] Delete legacy adapters after integration tests pass.
