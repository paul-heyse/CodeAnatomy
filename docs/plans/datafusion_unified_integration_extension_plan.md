# DataFusion Unified Integration Extension Plan

## Goals
- Eliminate remaining raw SQL and fallback execution paths so Ibis always executes on
  DataFusion-native plans.
- Standardize Delta registration on DataFusion TableProvider surfaces only.
- Move all DDL generation to SQLGlot ASTs and normalize through the policy pipeline.
- Make `information_schema` the single source of truth for schema maps, qualification,
  and SQLGlot policy enforcement.
- Harden SQL ingestion with strict qualification + normalization before Ibis parsing.
- Replace implicit Ibis fallbacks with explicit allowlists and built-in UDF wiring.

## Preconditions / gates
- DataFusion Python >= 43 (TableProvider registration and info_schema).
- deltalake >= 0.22 (DeltaTable provider support).
- ibis-framework[datafusion] (DataFusion backend, `create_table/create_view` APIs).
- SQLGlot pinned to `sqlglot_tools.optimizer` policy version.

## Scope 1: Remove raw SQL execution paths (Ibis always DF-native)
Objective: eliminate `raw_sql` execution and force DataFusion-native plans for
materialize and streaming.

Code patterns
```python
def stream_plan_df(
    plan: IbisPlan,
    *,
    execution: DataFusionExecutionOptions,
    params: Mapping[Value, object] | None,
) -> RecordBatchReaderLike:
    options = _resolve_options(
        execution.options,
        execution=execution,
        params=params,
    )
    df = ibis_plan_to_datafusion(
        plan,
        backend=execution.backend,
        ctx=execution.ctx,
        options=options,
    )
    return datafusion_to_reader(df)
```

Target files
- `src/ibis_engine/runner.py`
- `src/ibis_engine/execution.py`
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/compile_options.py`

Implementation checklist
- Remove `_raw_sql_plan` usage and `force_sql` branches in plan execution.
- Delete `execute_raw_sql` usage paths and enforce DataFusion DataFrame translation.
- Keep parameter bindings supported via Ibis `params=` and DataFusion plan execution.
- Ensure plan artifact capture continues via `DataFusionCompileOptions`.

## Scope 2: Delta TableProvider-only registration (no Arrow dataset fallback)
Objective: use DataFusion TableProvider surfaces for Delta reads and eliminate
Arrow dataset fallback paths.

Code patterns
```python
from datafusion import SessionContext
from deltalake import DeltaTable

def register_delta_provider(
    ctx: SessionContext, *, name: str, path: str, storage: Mapping[str, str] | None
) -> None:
    table = DeltaTable(path, storage_options=dict(storage or {}))
    ctx.register_table(name, table)
```

Target files
- `src/storage/dataset_sources.py`
- `src/storage/deltalake/delta.py`
- `src/ibis_engine/sources.py`
- `src/ibis_engine/io_bridge.py`
- `src/datafusion_engine/registry_bridge.py`

Implementation checklist
- Remove `DeltaTable.to_pyarrow_dataset` paths in dataset sources.
- Register Delta tables directly via DataFusion TableProvider (or Ibis `read_delta`).
- Thread storage options and scan policies into TableProvider registration.
- Update callers to use DataFusion-registered tables for reads and CDF access.

## Scope 3: SQLGlot AST-based DDL for tables and views
Objective: generate deterministic DDL using SQLGlot ASTs and normalize with the
SQLGlot policy pipeline.

Code patterns
```python
from sqlglot import exp

def create_external_table_stmt(
    *,
    name: str,
    column_defs: list[exp.ColumnDef],
    location: str,
    file_format: str,
) -> exp.Create:
    return exp.Create(
        this=exp.Schema(
            this=exp.Table(this=exp.Identifier(this=name)),
            expressions=column_defs,
        ),
        kind="EXTERNAL",
        properties=exp.Properties(
            expressions=[
                exp.LocationProperty(this=exp.Literal.string(location)),
                exp.FileFormat(this=exp.Identifier(this=file_format)),
            ],
        ),
    )
```

Target files
- `src/schema_spec/specs.py`
- `src/schema_spec/view_specs.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/sqlglot_tools/optimizer.py`

Implementation checklist
- Replace string-built DDL with SQLGlot ASTs.
- Include `NOT NULL`, `DEFAULT`, and `PRIMARY KEY` constraints in column defs.
- Emit `PARTITIONED BY`, `WITH ORDER`, and `UNBOUNDED` where configured.
- Normalize all DDL through `normalize_ddl_sql` before registration.

## Scope 4: information_schema-derived schema maps for SQLGlot qualification
Objective: use `information_schema` to build catalog-aware schema maps for
qualification, type inference, and canonicalization.

Code patterns
```python
def schema_map_from_info_schema(rows: Sequence[Mapping[str, object]]) -> dict[str, object]:
    mapping: dict[str, dict[str, dict[str, dict[str, str]]]] = {}
    for row in rows:
        catalog = str(row.get("table_catalog") or "datafusion")
        schema = str(row.get("table_schema") or "public")
        table = str(row.get("table_name") or "")
        column = str(row.get("column_name") or "")
        dtype = str(row.get("data_type") or "unknown")
        mapping.setdefault(catalog, {}).setdefault(schema, {}).setdefault(table, {})[column] = dtype
    return mapping
```

Target files
- `src/datafusion_engine/schema_introspection.py`
- `src/sqlglot_tools/optimizer.py`
- `src/datafusion_engine/query_fragments.py`

Implementation checklist
- Emit catalog + schema + table + column mapping from `information_schema.columns`.
- Pass the mapping into SQLGlot qualification and type annotation phases.
- Enforce `qualify_outputs` and `validate_qualify_columns` in normalization.
- Persist schema-map fingerprints in diagnostics/manifest artifacts.

## Scope 5: SQL ingestion policy lane (strict parse + normalize before Ibis)
Objective: normalize SQL inputs through SQLGlot policy before Ibis parsing and
capture artifacts for reproducibility.

Code patterns
```python
from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    default_sqlglot_policy,
    normalize_expr,
    parse_sql_strict,
)

def normalize_ingest_sql(sql: str, *, schema_map: Mapping[str, object]) -> Expression:
    policy = default_sqlglot_policy()
    expr = parse_sql_strict(sql, dialect=policy.read_dialect)
    return normalize_expr(
        expr,
        options=NormalizeExprOptions(schema=schema_map, policy=policy, sql=sql),
    )
```

Target files
- `src/ibis_engine/sql_bridge.py`
- `src/sqlglot_tools/optimizer.py`
- `src/obs/manifest.py`

Implementation checklist
- Normalize ingest SQL via SQLGlot policy before `ibis.parse_sql`.
- Validate qualification errors early and surface them in artifacts.
- Persist policy snapshots + AST payloads for SQL ingestion.
- Reject unsupported constructs via `unsupported_level=RAISE`.

## Scope 6: Ibis expression registry hardening (explicit allowlist)
Objective: remove implicit fallbacks to `ibis.*` and only allow explicitly
registered functions and built-in UDF mappings.

Code patterns
```python
@dataclass(frozen=True)
class IbisExprRegistry:
    functions: Mapping[str, IbisExprFn]

    def resolve(self, name: str) -> IbisExprFn:
        fn = self.functions.get(name)
        if fn is None:
            msg = f"Unsupported Ibis function: {name!r}."
            raise KeyError(msg)
        return fn
```

Target files
- `src/ibis_engine/expr_compiler.py`
- `src/ibis_engine/builtin_udfs.py`
- `src/ibis_engine/registry.py`

Implementation checklist
- Remove `getattr(ibis, name)` fallback resolution.
- Register builtin DataFusion UDFs via `@ibis.udf.scalar.builtin`.
- Require `backend.has_operation` checks for rulepack-only ops.
- Document allowed function inventory in diagnostics.

## Scope 7: Diagnostics + artifacts alignment (policy snapshots, info_schema)
Objective: record schema, policy, and AST fingerprints for deterministic
incremental rebuilds and debugging.

Code patterns
```python
def _runtime_notes(ctx: SessionContext) -> dict[str, object]:
    return {"datafusion_version": ctx.sql("SELECT version()").collect()[0][0]}
```

Target files
- `src/obs/manifest.py`
- `src/obs/diagnostics_tables.py`
- `src/datafusion_engine/schema_introspection.py`

Implementation checklist
- Persist SQLGlot policy snapshots and AST hashes per rule output.
- Persist `information_schema` snapshots for drift detection.
- Add diagnostics for schema-map and DDL fingerprints.

## Legacy decommission list (post-migration)
Remove these once the above scopes are complete and tests pass.

- Raw SQL execution and bridge fallbacks:
  - `src/ibis_engine/runner.py` (`_raw_sql_plan`, force-sql branches)
  - `src/ibis_engine/sql_bridge.py` (`execute_raw_sql`)
  - `src/datafusion_engine/compile_options.py` (`force_sql` flag)
- Delta Arrow dataset fallbacks:
  - `src/storage/dataset_sources.py` (`_delta_dataset_from_path`, `_delta_dataset_from_files`)
  - Any `DeltaTable.to_pyarrow_dataset` usage
- Implicit Ibis function fallback resolution:
  - `src/ibis_engine/expr_compiler.py` (`getattr(ibis, ...)` fallback)

## Quality gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13.11`
- `uv run pytest -q`
