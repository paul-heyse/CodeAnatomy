# Combined Advanced Library Utilization Implementation Plan

## Goals
- Maximize built-in DataFusion, Ibis, SQLGlot, and Delta Lake surfaces to reduce bespoke logic.
- Improve robustness and dynamic adaptation through information_schema-driven metadata.
- Unify relational execution and storage integration across Delta and DataFusion.
- Strengthen error handling, idempotent writes, and lineage observability.

## Source plans
- `docs/plans/combined_library_utilization_gap_closure_plan.md`
- `docs/plans/datafusion_advanced_leverage_plan.md`

## Scope 1: Delta SQL DDL registration via DeltaTableFactory
Objective: register Delta tables using DataFusion DDL and delta-rs TableProviderFactory so SQL
becomes the canonical registration path (no bespoke DeltaTable wiring).

Representative code
```rust
// rust/datafusion_ext/src/lib.rs
#[pyfunction]
fn install_delta_table_factory(ctx: PyRef<PySessionContext>, alias: String) -> PyResult<()> {
    let mut state = ctx.ctx.state_ref().write();
    let factories = state.table_factories_mut();
    factories.insert(alias, Arc::new(DeltaTableFactory {}));
    Ok(())
}
```

```python
# src/datafusion_engine/runtime.py
ctx.sql(
    """
    CREATE EXTERNAL TABLE repo_delta
    STORED AS DELTATABLE
    LOCATION 's3://bucket/repo'
    OPTIONS (aws_region='us-east-1')
    """
).collect()
```

Target files
- [x] `rust/datafusion_ext/src/lib.rs`
- [x] `src/datafusion_engine/runtime.py`
- [x] `src/schema_spec/specs.py`
- [x] `src/datafusion_engine/registry_bridge.py`

Implementation checklist
- [x] Expose `install_delta_table_factory` from the Rust extension and call it during runtime init.
- [x] Allow Delta dataset specs to emit `STORED AS DELTATABLE` DDL with `OPTIONS(...)`.
- [x] Route Delta registrations through DDL paths where `delta_scan` does not require `with_files`.
- [x] Record DDL registration details in diagnostics artifacts.

Legacy decommission
- [x] `src/datafusion_engine/registry_bridge.py` `_register_delta` default path.
- [x] Direct `DeltaTable` registration for non-pruned Delta reads.

## Scope 2: Dynamic builtin function catalog (information_schema-driven)
Objective: replace static builtin function maps with runtime introspection of DataFusion
`information_schema` to stay aligned with engine capabilities.

Representative code
```python
# src/datafusion_engine/udf_catalog.py
catalog = FunctionCatalog.from_information_schema(
    routines=introspector.routines_snapshot(),
    parameters=introspector.parameters_snapshot(),
    parameters_available=True,
)
is_builtin = func_id.lower() in catalog.function_names
```

Target files
- [x] `src/datafusion_engine/udf_catalog.py`
- [x] `src/datafusion_engine/builtin_registry.py` (removed)
- [x] `src/datafusion_engine/schema_registry.py`
- [x] `src/datafusion_engine/schema_introspection.py`

Implementation checklist
- [x] Add a runtime function catalog cache built from information_schema routines/parameters.
- [x] Replace static builtin lookup with catalog-backed lookup and signature checks.
- [x] Record function catalog snapshots in diagnostics for visibility.

Legacy decommission
- [x] `src/datafusion_engine/builtin_registry.py` and its static registry tables.
- [x] Hard-coded builtin signatures in UDF tier selection logic.

## Scope 3: Unbounded external tables for streaming sources
Objective: enforce DataFusion streaming semantics via `CREATE UNBOUNDED EXTERNAL TABLE`
and remove bespoke streaming flags.

Representative code
```python
# src/schema_spec/specs.py
ddl = f"""
CREATE UNBOUNDED EXTERNAL TABLE {table_name} (
  {column_defs}
)
STORED AS PARQUET
LOCATION '{location}'
"""
```

Target files
- [x] `src/schema_spec/specs.py`
- [x] `src/schema_spec/system.py`
- [x] `src/datafusion_engine/registry_bridge.py`

Implementation checklist
- [x] Map streaming dataset specs to `unbounded=True` and emit UNBOUNDED DDL.
- [x] Ensure registry bridge uses DDL for streaming registrations.
- [x] Record streaming registration metadata in diagnostics artifacts.

Legacy decommission
- [x] Ad-hoc streaming registration flags in registry code paths.
- [x] Manual streaming table registration helpers outside DDL.

## Scope 4: DataFusion INSERT paths for Delta writes
Objective: rely on DataFusion DML against DeltaTableProvider for append/overwrite instead of
custom write loops where possible.

Representative code
```python
# src/datafusion_engine/bridge.py
sql = f"INSERT INTO {table_name} {select_sql}"
ctx.sql_with_options(sql, sql_options).collect()
```

Target files
- [x] `src/datafusion_engine/bridge.py`
- [x] `src/ibis_engine/io_bridge.py`
- [x] `src/storage/deltalake/delta.py`

Implementation checklist
- [x] Add DataFusion INSERT write path for Delta datasets (append/overwrite).
- [ ] Prefer INSERT path for DataFusion-backed writes; fallback to `write_deltalake` only when
      INSERT is unsupported.
- [ ] Record write mode and table provider capabilities in diagnostics.

Legacy decommission
- [x] Delta write flows that always materialize Arrow and call `write_deltalake`.

## Scope 5: SQLGlot AST execution path via Ibis DataFusion backend
Objective: execute SQLGlot ASTs directly (no string round-trips) and preserve AST fidelity.

Representative code
```python
# src/ibis_engine/sql_bridge.py
expr = ibis_to_sqlglot(ibis_expr, backend=backend, params=params)
backend.raw_sql(expr)
```

Target files
- [x] `src/ibis_engine/sql_bridge.py`
- [x] `src/datafusion_engine/bridge.py`
- [x] `src/sqlglot_tools/bridge.py`

Implementation checklist
- [x] Add an AST-aware execution path that passes SQLGlot expressions to Ibis backend raw_sql.
- [x] Preserve read/write dialect metadata in plan artifacts for traceability.
- [x] Make AST execution the default for DataFusion-backed SQL ingress.

Legacy decommission
- [x] SQL string generation paths that immediately re-parse into SQLGlot.
- [x] Redundant SQL serialization for DataFusion execution.

## Scope 6: Ibis-native Delta read/write integration
Objective: use Ibis `read_delta` and `to_delta` for Delta IO in Python paths.

Representative code
```python
# src/ibis_engine/sources.py
table = backend.read_delta(path, table_name=name, storage_options=storage)
backend.to_delta(expr, path, mode=mode, overwrite_schema=overwrite_schema)
```

Target files
- [x] `src/ibis_engine/sources.py`
- [x] `src/ibis_engine/io_bridge.py`
- [x] `src/storage/deltalake/delta.py`

Implementation checklist
- [x] Route Delta reads through Ibis backend `read_delta`.
- [x] Route Delta writes through Ibis backend `to_delta` where possible.
- [x] Consolidate Delta IO configuration in Ibis source options.

Legacy decommission
- [x] Custom Delta read/write helpers that bypass Ibis backend surfaces.

## Scope 7: SQLGlot lineage artifacts and canonicalization ingress
Objective: capture lineage metadata from SQLGlot and enforce dialect-aware transpilation.

Representative code
```python
# src/sqlglot_tools/optimizer.py
diagnostics = {
    "tables": referenced_tables(expr),
    "columns": referenced_columns(expr),
    "canonical_fingerprint": canonical_ast_fingerprint(expr),
}
```

Target files
- [x] `src/sqlglot_tools/lineage.py`
- [x] `src/sqlglot_tools/optimizer.py`
- [x] `src/datafusion_engine/bridge.py`
- [x] `src/obs/diagnostics_tables.py`

Implementation checklist
- [x] Add lineage payloads (tables, columns, scopes) to plan artifacts.
- [x] Enforce `sqlglot.transpile(read=..., write=...)` for external SQL ingress.
- [x] Record dialect settings and canonical fingerprints in diagnostics artifacts.

Legacy decommission
- [x] Bespoke lineage extraction paths outside SQLGlot.

## Scope 8: Native Delta CDF TableProvider integration
Objective: use delta-rs CDF TableProvider instead of materializing Arrow tables.

Representative code
```rust
// rust/datafusion_ext/src/lib.rs
#[pyfunction]
fn delta_cdf_table_provider(
    py: Python<'_>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    options: DeltaCdfOptions,
) -> PyResult<PyObject> {
    let provider = DeltaCdfTableProvider::try_new(options.to_builder(table_uri, storage_options)?)
        .map_err(|err| PyRuntimeError::new_err(format!("{err}")))?;
    Ok(FFI_TableProvider::new(Arc::new(provider), true, None).into_py(py))
}
```

Target files
- [x] `rust/datafusion_ext/src/lib.rs`
- [x] `src/datafusion_engine/registry_bridge.py`
- [x] `src/datafusion_engine/delta_cdf_provider.py` (removed)

Implementation checklist
- [x] Expose a Rust-backed CDF provider factory from the extension module.
- [x] Register CDF tables via provider instead of Arrow batch materialization.
- [x] Record CDF provider usage and options in diagnostics artifacts.

Legacy decommission
- [x] `src/datafusion_engine/delta_cdf_provider.py` (materialized CDF path).

## Scope 9: DeltaScanConfig derived from session settings
Objective: use `DeltaScanConfig::new_from_session` to inherit DataFusion settings automatically.

Representative code
```rust
// rust/datafusion_ext/src/lib.rs
let mut scan_config = DeltaScanConfig::new_from_session(&ctx.ctx)
    .map_err(|err| PyRuntimeError::new_err(format!("{err}")))?;
scan_config = scan_config.with_schema(schema);
```

Target files
- [x] `rust/datafusion_ext/src/lib.rs`
- [x] `src/datafusion_engine/registry_bridge.py`

Implementation checklist
- [x] Build scan config from DataFusion session defaults before applying overrides.
- [x] Remove duplicated scan settings in Python when they are available via session config.
- [x] Record effective scan config in Delta diagnostics artifacts.

Legacy decommission
- [x] Redundant per-call scan configuration logic where session defaults suffice.

## Scope 10: Idempotent Delta writes via CommitProperties
Objective: attach deterministic app_id and version (run_id) to Delta commits for idempotency.

Representative code
```python
# src/storage/deltalake/delta.py
props = CommitProperties(app_id=run_id, version=commit_version)
write_deltalake(path, table, commit_properties=props, mode=mode)
```

Target files
- [x] `src/storage/deltalake/delta.py`
- [x] `src/datafusion_engine/runtime.py`
- [x] `src/obs/datafusion_runs.py`

Implementation checklist
- [x] Thread run_id into Delta write paths and commit properties.
- [x] Use a stable version sequence for idempotent retries.
- [x] Record commit properties in diagnostics artifacts.

Legacy decommission
- [x] Write paths that do not tag commits with app_id/version.

## Global legacy decommission list (after full plan completion)
- [x] `src/datafusion_engine/builtin_registry.py`
- [x] `src/datafusion_engine/delta_cdf_provider.py`
- [x] `src/datafusion_engine/registry_bridge.py` `_register_delta` default registration path
- [x] Delta write flows that always materialize Arrow and call `write_deltalake`
- [x] SQL string re-serialization paths that discard SQLGlot ASTs
