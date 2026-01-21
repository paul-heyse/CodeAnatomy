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
- Expand DataFusion UDF families (UDAF/UDWF/UDTF) and built-ins to replace bespoke
  Arrow/Python compute.
- Adopt advanced Ibis IR patterns (`unpack`, selectors, `to_parquet_dir`,
  `compiler.to_sqlglot`) to reduce bespoke transforms.
- Persist SQLGlot AST payloads, diff scripts, and policy snapshots for deterministic
  caching and debug reproducibility.
- Standardize outputs using DataFusion `COPY`/`INSERT` and provider-native writes.

## Preconditions / gates
- DataFusion Python >= 43 (TableProvider registration and info_schema).
- deltalake >= 0.22 (DeltaTable provider support).
- ibis-framework[datafusion] (DataFusion backend, `create_table/create_view` APIs).
- SQLGlot pinned to `sqlglot_tools.optimizer` policy version.

## Scope 1: Remove raw SQL execution paths (Ibis always DF-native)
Objective: eliminate `raw_sql` execution and force DataFusion-native plans for
materialize and streaming.

Status: Completed.

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
- [x] Remove `_raw_sql_plan` usage and `force_sql` branches in plan execution.
- [x] Delete `execute_raw_sql` usage paths and enforce DataFusion DataFrame translation.
- [x] Keep parameter bindings supported via Ibis `params=` and DataFusion plan execution.
- [x] Ensure plan artifact capture continues via `DataFusionCompileOptions`.

## Scope 2: Delta TableProvider-only registration (no Arrow dataset fallback)
Objective: use DataFusion TableProvider surfaces for Delta reads and eliminate
Arrow dataset fallback paths.

Status: Completed.

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
- `src/datafusion_engine/catalog_provider.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/incremental/publish.py`
- `src/incremental/relspec_update.py`

Implementation checklist
- [x] Remove `DeltaTable.to_pyarrow_dataset` paths in dataset sources.
- [x] Register Delta tables directly via DataFusion TableProvider (or Ibis `read_delta`).
- [x] Thread storage options and scan policies into TableProvider registration.
- [x] Replace Arrow-based Delta reads (`read_table_delta`, `read_delta_cdf`) with
  DataFusion/Ibis and update callers.
- [x] Update remaining callers to use DataFusion-registered tables for reads and CDF access.

## Scope 3: SQLGlot AST-based DDL for tables and views
Objective: generate deterministic DDL using SQLGlot ASTs and normalize with the
SQLGlot policy pipeline.

Status: Partial (AST in place for CREATE TABLE/VIEW; external DDL still string-built).

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
- [x] Replace string-built DDL with SQLGlot ASTs for CREATE TABLE/VIEW.
- [x] Include `PRIMARY KEY` constraints in column defs.
- [ ] Include `NOT NULL` and `DEFAULT` constraints in column defs where configured.
- [ ] Emit `PARTITIONED BY`, `WITH ORDER`, and `UNBOUNDED` via AST properties.
- [x] Normalize all DDL through `normalize_ddl_sql` before registration.

## Scope 4: information_schema-derived schema maps for SQLGlot qualification
Objective: use `information_schema` to build catalog-aware schema maps for
qualification, type inference, and canonicalization.

Status: Partial (schema map + fingerprints recorded; diagnostics tables pending).

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
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_registry.py`

Implementation checklist
- [x] Emit catalog + schema + table + column mapping from `information_schema.columns`.
- [x] Pass the mapping into SQLGlot qualification and type annotation phases.
- [x] Enforce `qualify_outputs` and `validate_qualify_columns` in normalization.
- [x] Persist schema-map fingerprints in manifest artifacts.
- [ ] Emit schema-map fingerprints in diagnostics tables for drift detection.

## Scope 5: SQL ingestion policy lane (strict parse + normalize before Ibis)
Objective: normalize SQL inputs through SQLGlot policy before Ibis parsing and
capture artifacts for reproducibility.

Status: Completed.

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
- [x] Normalize ingest SQL via SQLGlot policy before `ibis.parse_sql`.
- [x] Validate qualification errors early and surface them in artifacts.
- [x] Persist policy hash + normalized SQL in ingestion artifacts.
- [x] Persist AST payloads (serde) for SQL ingestion.
- [x] Require explicit schema + catalog mappings for all ingest SQL.
- [x] Reject unsupported constructs via `unsupported_level=RAISE`.

## Scope 6: Ibis expression registry hardening (explicit allowlist)
Objective: remove implicit fallbacks to `ibis.*` and only allow explicitly
registered functions and built-in UDF mappings.

Status: Partial (fallback removed; allowlist wiring + diagnostics pending).

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
- [x] Remove `getattr(ibis, name)` fallback resolution.
- [x] Register builtin DataFusion UDFs via `@ibis.udf.scalar.builtin`.
- [ ] Require `backend.has_operation` checks for rulepack-only ops.
- [ ] Document allowed function inventory in diagnostics.
- [ ] Gate rulepack functions against `information_schema.routines` + `parameters`.

## Scope 7: Diagnostics + artifacts alignment (policy snapshots, info_schema)
Objective: record schema, policy, and AST fingerprints for deterministic
incremental rebuilds and debugging.

Status: Partial (schema/policy snapshots recorded; diagnostics drift pending).

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
- [x] Expand `information_schema` introspection helpers and function catalogs.
- [x] Persist SQLGlot policy snapshots in runtime + ingest artifacts.
- [x] Persist `information_schema` snapshots (schema map + function catalog) in manifest artifacts.
- [ ] Persist per-rule AST hashes alongside diff scripts.
- [ ] Add diagnostics for schema-map and DDL fingerprints.

## Scope 8: DataFusion UDF expansion (UDAF/UDWF/UDTF) + built-in substitution
Objective: replace bespoke Arrow/Python aggregations and nested builders with
DataFusion UDFs or built-in SQL functions.

Status: Completed.

Code patterns
```python
from datafusion import Accumulator, udaf, col
import pyarrow as pa
import pyarrow.compute as pc

class ArrayAggAcc(Accumulator):
    def __init__(self) -> None:
        self._values: list[pa.Array] = []

    def update(self, values: pa.Array) -> None:
        self._values.append(values)

    def merge(self, states: list[pa.Array]) -> None:
        self._values.extend(states)

    def state(self) -> list[pa.Scalar]:
        return [pa.scalar(self._values)]

    def evaluate(self) -> pa.Scalar:
        return pa.scalar(self._values)

array_agg_udaf = udaf(
    ArrayAggAcc,
    input_types=[pa.string()],
    return_type=pa.list_(pa.string()),
    state_type=[pa.list_(pa.string())],
    volatility="stable",
    name="array_agg_udaf",
)
ctx.register_udaf(array_agg_udaf)
```

```python
from datafusion import udwf, WindowEvaluator
import pyarrow as pa

class RunningCount(WindowEvaluator):
    def evaluate_all(self, values: list[pa.Array], num_rows: int) -> pa.Array:
        return pa.array(list(range(1, num_rows + 1)))

row_index_udwf = udwf(RunningCount, [pa.int64()], pa.int64(), "stable", "row_index")
ctx.register_udwf(row_index_udwf)
```

```python
sql = """
SELECT
  array_distinct(array_agg(col)) AS uniq_vals,
  map_entries(attrs) AS attrs_kv,
  union_tag(node_variant) AS variant_tag
FROM input
GROUP BY key
"""
```

Target files
- `src/arrowdsl/schema/nested_builders.py`
- `src/arrowdsl/spec/expr_ir.py`
- `src/arrowdsl/schema/encoding_policy.py`
- `src/arrowdsl/finalize/finalize.py`
- `src/arrowdsl/core/ids.py`
- `src/hamilton_pipeline/modules/normalization.py`
- `src/incremental/props_update.py`
- `src/ibis_engine/builtin_udfs.py`
- `src/datafusion_engine/runtime.py`

Implementation checklist
- [x] Replace bespoke list/struct accumulator helpers with UDAFs or built-ins.
- [x] Add UDWFs for row indexing where explicit ordering is required.
- [x] Add UDTFs for literal-driven table expansions when needed.
- [x] Register new UDFs in SessionContext bootstraps and diagnostics inventories.
- [x] Replace remaining `pc.*` compute in hot paths with DataFusion SQL/built-ins.

## Scope 9: Ibis advanced IR patterns (unpack/selectors/to_parquet_dir)
Objective: reduce bespoke transforms by adopting Ibis high-level operators.

Status: Partial (AST payloads + diff scripts captured; schema mapping gates pending).

Code patterns
```python
import ibis
from ibis import selectors as s

expr = (
    table
    .unpack("struct_col")
    .mutate(**s.across(s.numeric(), lambda c: c.fill_null(0), names="fill_{name}"))
)
```

```python
table.to_parquet_dir(output_dir, params=params, partition_by=["repo"])
```

```python
sqlglot_expr = backend.compiler.to_sqlglot(expr)
```

Target files
- `src/normalize/ibis_plan_builders.py`
- `src/normalize/ibis_spans.py`
- `src/relspec/compiler.py`
- `src/cpg/relationship_plans.py`
- `src/ibis_engine/io_bridge.py`
- `src/ibis_engine/plan_diff.py`
- `src/ibis_engine/sql_bridge.py`
- `src/relspec/rules/validation.py`

Implementation checklist
- [ ] Replace struct field projections with `.unpack(...)`.
- [ ] Use selector-driven transforms for wide schema updates.
- [ ] Standardize dataset writes via `to_parquet_dir` where appropriate.
- [ ] Capture SQLGlot ASTs via `backend.compiler.to_sqlglot`.
- [ ] Prefer `.pipe(...)`/`.substitute(...)` for composable rule templates.

## Scope 10: SQLGlot AST serialization + semantic diff artifacts
Objective: persist canonical AST payloads, diff scripts, and schema-aware
fingerprints for deterministic caching and replay.

Status: Remaining.

Code patterns
```python
from sqlglot.serde import dump
from sqlglot.diff import diff

payload = dump(expr)
changes = [change for change in diff(before, after)]
```

Target files
- `src/sqlglot_tools/optimizer.py`
- `src/sqlglot_tools/bridge.py`
- `src/ibis_engine/compiler_checkpoint.py`
- `src/ibis_engine/plan_diff.py`
- `src/engine/plan_cache.py`
- `src/obs/manifest.py`
- `src/obs/diagnostics_tables.py`

Implementation checklist
- [x] Persist AST payloads via `sqlglot.serde.dump` in diagnostics/manifest artifacts.
- [x] Store semantic diff scripts between canonicalization phases.
- [ ] Use SQLGlot schema objects (`MappingSchema`) for stable identifier normalization.
- [ ] Gate incremental rebuilds on semantic diffs + plan hashes.

## Scope 11: Output materialization via DataFusion COPY/INSERT
Objective: standardize output writes on DataFusion-native DML/DDL surfaces and
provider-native writes.

Status: Partial (catalog snapshots captured; allowlist enforcement pending).

Code patterns
```python
ctx.sql(
    "COPY (SELECT * FROM results) TO '/path/out' "
    "STORED AS PARQUET OPTIONS (compression 'zstd')"
).collect()
```

```python
ctx.sql("INSERT INTO target SELECT * FROM staging").collect()
```

Target files
- `src/datafusion_engine/bridge.py`
- `src/engine/materialize.py`
- `src/storage/deltalake/delta.py`
- `src/obs/parquet_writers.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/ibis_engine/io_bridge.py`

Implementation checklist
- [ ] Route non-Delta dataset writes through `COPY` for Parquet/CSV/JSON outputs.
- [ ] Use `INSERT` for TableProvider-backed sinks when supported.
- [ ] Use delta-rs only for merge/update operations.
- [ ] Record write SQL + options in diagnostics.

## Scope 12: Function catalog inventory + allowlist enforcement
Objective: align rulepack function usage with DataFusion’s runtime catalog
(`SHOW FUNCTIONS` + `information_schema` routines/parameters).

Status: Remaining.

Code patterns
```python
table = ctx.sql("SHOW FUNCTIONS").to_arrow_table()
entries = table.to_pylist()
```

Target files
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/runtime.py`
- `src/ibis_engine/expr_compiler.py`
- `src/engine/function_registry.py`
- `src/obs/diagnostics_tables.py`
- `src/obs/manifest.py`

Implementation checklist
- [x] Persist function catalog snapshots from information_schema routines/parameters.
- [ ] Join routines + parameters for full signature validation.
- [ ] Gate rulepack functions against the runtime allowlist.
- [ ] Emit allowlist inventories as diagnostics artifacts.

## Legacy decommission list (post-migration)
Remove these once the above scopes are complete and tests pass.

- [x] Raw SQL execution and bridge fallbacks:
  - `src/ibis_engine/runner.py` (`_raw_sql_plan`, force-sql branches)
  - `src/ibis_engine/sql_bridge.py` (`execute_raw_sql`)
  - `src/datafusion_engine/compile_options.py` (`force_sql` flag)
- [x] Delta Arrow dataset fallbacks:
  - `src/storage/dataset_sources.py` (`_delta_dataset_from_path`, `_delta_dataset_from_files`)
  - Any `DeltaTable.to_pyarrow_dataset` usage
- [x] Implicit Ibis function fallback resolution:
  - `src/ibis_engine/expr_compiler.py` (`getattr(ibis, ...)` fallback)
- [x] Arrow-based Delta snapshot readers:
  - `src/storage/deltalake/delta.py` (`read_table_delta`, `read_delta_cdf`)
  - `src/storage/io.py` (re-exports)
  - `src/storage/__init__.py` (re-exports)
  - `src/incremental/scip_snapshot.py`
  - `src/incremental/snapshot.py`
  - `src/incremental/invalidations.py`
  - `src/incremental/fingerprint_changes.py`
- [ ] Bespoke nested builders superseded by DataFusion functions/UDFs:
  - `src/arrowdsl/schema/nested_builders.py` (list/struct accumulators) ✅
  - `src/arrowdsl/spec/codec.py` (nested encode/decode helpers)
  - `src/arrowdsl/schema/schema.py` (custom nested projection helpers)
- [x] Bespoke Arrow compute paths replaced by DataFusion built-ins:
  - `src/arrowdsl/core/ids.py` (binary join + hash assembly)
  - `src/arrowdsl/finalize/finalize.py` (row hashing/filters)
  - `src/hamilton_pipeline/modules/normalization.py` (mask/merge logic)
- [ ] Tests/docs that assume fallback execution:
  - `tests/integration/test_datafusion_fallbacks.py`

## Quality gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13.11`
- `uv run pytest -q`
