# Ibis + DataFusion Modernization Plan

This plan implements the Ibis/DataFusion modernization recommendations and the
legacy/compatibility deprecations identified in the `src/ibis_engine` review.
It focuses on deeper DataFusion integration, SQLGlot policy hardening, and
removing obsolete paths while preserving the current execution model.

## Goals
- Replace legacy/compat helpers with first-class DataFusion/Ibis interfaces.
- Make parameterization, prepared statements, and projection pushdown the
  default execution behavior.
- Expand the Substrait lane so SQL generation becomes a fallback path.
- Treat SQL ingestion as a formally supported path with deterministic artifacts.
- Enforce schema contracts using DataFusion scan and schema capabilities.
- Tighten UDF catalogs against runtime metadata.
- Wire object-store + listing-table options into the Ibis adapter layer.

---

## Scope 1: Legacy cleanups and deprecations

**Objective:** remove obsolete helpers and align callers on canonical interfaces.

**Representative code snippet**
```python
# src/ibis_engine/registry.py
def datafusion_context(backend: object) -> SessionContext:
    """Return a DataFusion SessionContext from an Ibis backend."""
    for attr in ("con", "_context", "_ctx", "ctx", "session_context"):
        ctx = getattr(backend, attr, None)
        if isinstance(ctx, SessionContext):
            return ctx
    msg = "Ibis backend does not expose a DataFusion SessionContext."
    raise ValueError(msg)

# Remove deprecated alias _datafusion_context and update imports to use
# datafusion_context in all call sites.
```

**Target files**
- `src/ibis_engine/registry.py` (remove `_datafusion_context`)
- `src/ibis_engine/sources.py` (remove Delta convenience wrappers)
- `src/ibis_engine/backend.py` (optionally drop legacy module fallback)
- Any call sites referencing removed helpers

**Implementation checklist**
- [ ] Remove `_datafusion_context` and update imports to `datafusion_context`.
- [ ] Remove `read_delta_ibis`, `write_delta_ibis`, `plan_to_delta_ibis` and
      update call sites to use DataFusion registry and Delta writers.
- [ ] Decide on compatibility policy for `ibis.backends.datafusion` import
      fallback and remove if no longer needed.
- [ ] Update docs/architecture references if they mention removed helpers.

---

## Scope 2: Parameterization + prepared statements as the default lane

**Objective:** eliminate large literal expansions and stabilize plan caches.

**Representative code snippet**
```python
# src/datafusion_engine/bridge.py
from datafusion_engine.bridge import prepare_statement, execute_prepared_statement
from ibis_engine.params_bridge import datafusion_param_bindings

def execute_parametrized_sql(
    ctx: SessionContext,
    *,
    sql: str,
    params: Mapping[str, object],
    param_types: Sequence[str],
) -> DataFrame:
    bindings = datafusion_param_bindings(params)
    spec = prepare_statement(
        ctx,
        sql=sql,
        options=PreparedStatementOptions(param_types=param_types),
    )
    return execute_prepared_statement(ctx, spec.name, bindings)
```

**Target files**
- `src/ibis_engine/params_bridge.py`
- `src/ibis_engine/query_compiler.py`
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/runtime.py` (prepared statement policy)

**Implementation checklist**
- [ ] Extend parameter metadata to include SQL param types for prepared plans.
- [ ] Route large `file_id` filters through param tables or prepared statements
      instead of literal OR chains.
- [ ] Add `PreparedStatementOptions` usage in execution policy.
- [ ] Record parameter signatures in diagnostics for reproducibility.

---

## Scope 3: Dynamic projection pushdown from SQLGlot lineage

**Objective:** minimize scanned columns by computing required input columns per
query and passing them into DataFusion scan options.

**Representative code snippet**
```python
# src/ibis_engine/lineage.py
required = required_columns_by_table(expr, backend=backend, schema_map=schema_map)

# src/datafusion_engine/registry_bridge.py
def _apply_dynamic_projection(scan: DataFusionScanOptions, *, required: Sequence[str]) -> DataFusionScanOptions:
    return replace(scan, projection_exprs=tuple(required))
```

**Target files**
- `src/ibis_engine/lineage.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/compile_options.py`
- `src/datafusion_engine/df_builder.py`

**Implementation checklist**
- [ ] Compute required columns per table via SQLGlot lineage.
- [ ] Inject `projection_exprs` into `DataFusionScanOptions` per request.
- [ ] Persist lineage-derived projections as diagnostics artifacts.
- [ ] Add tests that confirm reduced column scans via `EXPLAIN`.

---

## Scope 4: Substrait lane expansion (SQL fallback only)

**Objective:** treat Substrait as the primary IR lane and SQL as fallback.

**Representative code snippet**
```python
# src/ibis_engine/substrait_bridge.py
def _try_ibis_substrait(expr: IbisTable, *, record_gaps: bool, diagnostics_sink: DiagnosticsCollector | None):
    if ibis_substrait_available():
        plan = ibis_substrait_compile(expr)
        return SubstraitCompilationResult(plan_bytes=plan, success=True, errors=())
    return _substrait_failure(expr=expr, error_msg="ibis-substrait unavailable", ...)
```

**Target files**
- `src/ibis_engine/substrait_bridge.py`
- `src/datafusion_engine/bridge.py`
- `src/ibis_engine/runner.py`

**Implementation checklist**
- [ ] Add `ibis-substrait` path before `pyarrow.substrait` fallback.
- [ ] Replay Substrait via `datafusion.substrait.Consumer` in the bridge.
- [ ] Record Substrait plan bytes + validation artifacts.
- [ ] Add feature flag to force SQL fallback for debugging.

---

## Scope 5: SQL ingestion as a first-class path

**Objective:** support SQL and SQLGlot AST ingestion with deterministic artifacts.

**Representative code snippet**
```python
# src/ibis_engine/sql_bridge.py
def parse_sql_expr(
    expr: Expression,
    *,
    catalog: Mapping[str, _SchemaProtocol],
    schema: _SchemaProtocol,
    policy: SqlGlotPolicy,
) -> Table:
    normalized = normalize_expr(expr, options=NormalizeExprOptions(schema=_schema_map_from_catalog(catalog), policy=policy))
    sql = sqlglot_sql(normalized, policy=policy)
    table = ibis.parse_sql(sql, catalog, dialect=policy.write_dialect)
    validate_expr_schema(table, expected=schema.to_pyarrow())
    return table
```

**Target files**
- `src/ibis_engine/sql_bridge.py`
- `src/sqlglot_tools/optimizer.py`
- `src/ibis_engine/compiler_checkpoint.py`

**Implementation checklist**
- [ ] Add AST ingestion function alongside string ingestion.
- [ ] Persist SQLGlot serde payloads + policy snapshots in diagnostics.
- [ ] Enforce `validate_qualify_columns` and identifier normalization.
- [ ] Add golden tests for SQL->Ibis round-trips.

---

## Scope 6: Schema contract hardening via DataFusion scan options

**Objective:** turn dataset contracts into explicit scan and ordering policies.

**Representative code snippet**
```python
# schema_spec/system.py (usage)
DatasetSpec(
    name="analytics.cpg_edges",
    datafusion_scan=DataFusionScanOptions(
        file_sort_order=("repo", "commit", "edge_id"),
        unbounded=False,
        listing_table_factory_infer_partitions=True,
        schema_force_view_types=True,
    ),
)
```

**Target files**
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/ibis_engine/registry.py`

**Implementation checklist**
- [ ] Encode ordering guarantees into `file_sort_order` contracts.
- [ ] Support `unbounded=True` for streaming datasets.
- [ ] Wire schema adapter factory hooks when present.
- [ ] Ensure scan options are reflected in DDL and registry-based registration.

---

## Scope 7: UDF catalog tightening and runtime validation

**Objective:** ensure builtin and custom UDFs align with runtime capabilities.

**Representative code snippet**
```python
# src/datafusion_engine/udf_catalog.py
def load_builtin_udfs(ctx: SessionContext) -> Mapping[str, UdfCatalogEntry]:
    rows = ctx.sql("SHOW FUNCTIONS").collect()
    return _udf_entries_from_rows(rows)

def validate_ibis_udfs(catalog: UdfCatalog, ibis_specs: Sequence[IbisUdfSpec]) -> None:
    for spec in ibis_specs:
        catalog.require(spec.engine_name)
```

**Target files**
- `src/datafusion_engine/udf_catalog.py`
- `src/datafusion_engine/udf_registry.py`
- `src/ibis_engine/builtin_udfs.py`
- `src/engine/function_registry.py`

**Implementation checklist**
- [ ] Load builtin function metadata via `SHOW FUNCTIONS`.
- [ ] Validate Ibis builtin UDF specs against runtime catalog.
- [ ] Emit diagnostics for missing or mismatched functions.
- [ ] Add strict/permissive policy gates for UDF usage.

---

## Scope 8: Object store + listing-table integration in the Ibis adapter

**Objective:** expose DataFusion object store registration and listing-table
options through the Ibis backend configuration.

**Representative code snippet**
```python
# src/ibis_engine/backend.py
def build_backend(cfg: IbisBackendConfig) -> ibis.backends.BaseBackend:
    profile = cfg.datafusion_profile or DataFusionRuntimeProfile()
    ctx = profile.session_context()
    for store in cfg.object_stores:
        ctx.register_object_store(store.scheme, store.handle, store.host)
    return ibis_datafusion.connect(ctx)
```

**Target files**
- `src/ibis_engine/config.py`
- `src/ibis_engine/backend.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/ibis_engine/registry.py`

**Implementation checklist**
- [ ] Add object-store definitions to `IbisBackendConfig`.
- [ ] Register object stores before `ibis.datafusion.connect(ctx)`.
- [ ] Expose listing-table registration options from dataset specs.
- [ ] Add diagnostics for object store and listing-table registration.

---

## Delivery sequencing
1) Scope 1 (cleanups) to remove deprecated surfaces.
2) Scope 2 (parameterization) to stabilize execution.
3) Scope 3 (projection pushdown) for performance gains.
4) Scope 4 (Substrait lane) to reduce SQL surface.
5) Scope 5 (SQL ingestion) for agent-safe SQL workflows.
6) Scope 6 (schema contracts) for correctness guarantees.
7) Scope 7 (UDF catalog) for runtime safety.
8) Scope 8 (object store + listing tables) for IO scalability.

---

## Test and validation checklist (global)
- [ ] `uv run ruff format` + `uv run ruff check --fix`
- [ ] `uv run pyrefly check`
- [ ] `uv run pyright --warnings --pythonversion=3.13.11`
- [ ] Add unit tests for prepared statements + param bindings.
- [ ] Add integration tests for projection pushdown and Substrait replay.
- [ ] Add golden tests for SQL ingestion round-trips and lineage artifacts.
