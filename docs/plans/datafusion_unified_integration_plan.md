# DataFusion Unified Integration Implementation Plan

## Goals
- Consolidate schema/catalog behavior around DataFusion built-ins and `information_schema`.
- Make Ibis the single relational IR, with DataFusion as the only execution substrate.
- Treat SQLGlot as the canonical compiler IR with deterministic, schema-aware rewrites.
- Standardize Delta Lake integration through DataFusion TableProvider and Delta scan policies.
- Remove fallback/shim paths so the system is fully aligned with the target architecture.

## Preconditions / gates
- DataFusion Python >= 43 (foreign table providers, info_schema, DDL surfaces).
- deltalake >= 0.22 (TableProvider support).
- ibis-framework with the DataFusion extra (`ibis-framework[datafusion]`).
- SQLGlot version pinned to match `src/sqlglot_tools/optimizer.py` policies.

## Status legend
- [x] Complete
- [~] Partially complete
- [ ] Remaining

## Execution phases (ordered)
Phase 1: Catalog + compiler policy foundation (Scopes 1, 4, 8, 7).
Phase 2: Ibis IR + function registry hardening (Scopes 3, 9).
Phase 3: Delta provider unification + DDL registration + write alignment (Scope 5).
Phase 4: Nested schema shaping (Scope 6).
Phase 5: Legacy cleanup + tests (Legacy decommission list).

## Scope 1: DataFusion catalog + information_schema as source of truth
Objective: enable DataFusion auto-loading and treat information_schema as the canonical
introspection layer for schema, columns, and routines.

Status: [x] Complete.

Code patterns
```python
from datafusion import SessionConfig, SessionContext

def _apply_catalog_config(config: SessionConfig, *, root: str, fmt: str) -> SessionConfig:
    return (
        config.set("datafusion.catalog.information_schema", "true")
        .set("datafusion.catalog.location", root)
        .set("datafusion.catalog.format", fmt)
    )

def _columns(ctx: SessionContext, *, catalog: str, schema: str) -> list[dict[str, object]]:
    sql = """
    select table_name, column_name, data_type, is_nullable, column_default
    from information_schema.columns
    where table_catalog = '{catalog}' and table_schema = '{schema}'
    """
    return [row.as_dict() for row in ctx.sql(sql.format(catalog=catalog, schema=schema)).collect()]

def _df_settings(ctx: SessionContext) -> list[dict[str, str]]:
    table = ctx.sql("SELECT name, value FROM information_schema.df_settings").to_arrow_table()
    return [{str(name): str(value)} for name, value in zip(table["name"], table["value"], strict=False)]

def _routines(ctx: SessionContext) -> list[dict[str, object]]:
    table = ctx.sql("SELECT * FROM information_schema.routines").to_arrow_table()
    return table.to_pylist()

def _parameters(ctx: SessionContext) -> list[dict[str, object]]:
    table = ctx.sql("SELECT * FROM information_schema.parameters").to_arrow_table()
    return table.to_pylist()
```

Target files
- `src/datafusion_engine/runtime.py`
- `src/engine/runtime_profile.py`
- `src/engine/session_factory.py`
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/schema_registry.py`
- `src/obs/diagnostics_tables.py`
- `src/obs/manifest.py`

Implementation checklist
- [x] Add runtime profile settings for catalog autoload and information_schema.
- [x] Centralize info_schema query helpers and remove bespoke introspection SQL.
- [x] Persist info_schema snapshots to diagnostics for schema drift visibility.
- [x] Validate expected schema against info_schema for all registered inputs.
- [x] Capture info_schema settings snapshots (`df_settings`) and DataFusion version.
- [x] Expose routines/parameters inventory for function allowlisting and UDF validation.
- [x] Emit schema_map fingerprints derived from info_schema for compiler gating.

## Scope 2: Schema DDL via SQLGlot with full DataFusion DDL surface
Objective: generate deterministic DDL (SQLGlot AST) for tables and views with
partitioning, ordering, constraints, and defaults.

Status: [x] Complete.

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

```python
def delta_external_table_ddl(*, name: str, location: str, options: Mapping[str, str]) -> str:
    options_clause = ", ".join(f"{key}={value!r}" for key, value in options.items())
    ddl = f"CREATE EXTERNAL TABLE {name} STORED AS DELTATABLE LOCATION {location!r}"
    if options_clause:
        ddl = f"{ddl} OPTIONS ({options_clause})"
    return normalize_ddl_sql(ddl)
```

Target files
- `src/schema_spec/specs.py`
- `src/schema_spec/relationship_specs.py`
- `src/schema_spec/view_specs.py`
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/query_fragments.py`

Implementation checklist
- [x] Emit SQLGlot column defs with `NOT NULL`, `DEFAULT`, and `PRIMARY KEY` constraints.
- [x] Add `PARTITIONED BY`, `WITH ORDER`, and `UNBOUNDED` surfaces where appropriate.
- [x] Normalize DDL through `sqlglot_tools.optimizer` before registration.
- [x] Store DDL fingerprints in diagnostics/manifest outputs.
- [x] Thread storage `OPTIONS(...)` into external table DDL for object store auth.
- [x] Add Delta DDL registration via `STORED AS DELTATABLE` when provider factory is available.

## Scope 3: Ibis substrate consolidation (SessionContext-first)
Objective: make Ibis IR the only relational builder and DataFusion the only executor,
with no SQL fallback paths.

Status: [x] Complete.

Code patterns
```python
import ibis
from datafusion import SessionContext

def build_backend(ctx: SessionContext) -> ibis.backends.datafusion.Backend:
    return ibis.datafusion.connect(ctx)

def run_expr(expr: ibis.expr.types.Table, *, params: dict[object, object]) -> None:
    reader = expr.to_pyarrow_batches(params=params, chunk_size=250_000)
    # Stream to Parquet or Delta via Arrow dataset writer.
```

```python
from ibis import _, selectors as s

expr = (
    table
    .mutate(
        **s.across(s.numeric(), lambda col: col.fill_null(0), names="fill_{name}")
    )
    .filter(_.path.contains("src/"))
)
```

Target files
- `src/ibis_engine/backend.py`
- `src/ibis_engine/runner.py`
- `src/ibis_engine/registry.py`
- `src/ibis_engine/plan.py`
- `src/ibis_engine/io_bridge.py`
- `src/ibis_engine/params_bridge.py`
- `src/ibis_engine/query_compiler.py`
- `src/ibis_engine/scan_io.py`
- `src/normalize/ibis_plan_builders.py`
- `src/normalize/runner.py`
- `src/relspec/graph.py`
- `src/relspec/cpg/build_edges.py`
- `src/relspec/cpg/build_nodes.py`
- `src/relspec/cpg/build_props.py`

Implementation checklist
- [x] Route all Ibis backends through a single SessionContext provider.
- [x] Replace named memtable usage with `create_table`/`create_view`.
- [x] Standardize `ibis.param` + `params=` for dynamic rulepack runs.
- [x] Use `Table.cache()` for shared intermediates with explicit lifecycle control.
- [x] Remove SQLGlot/Ibis fallback execution branches.
- [x] Adopt deferred (`ibis._`) and selector-based rule templates for wide schemas.
- [x] Gate rulepack operations with `backend.has_operation` checks.
- [x] Replace any remaining raw SQL usage with `Backend.sql` + explicit schemas.

## Scope 4: SQLGlot compiler policy hardening
Objective: enforce schema-aware qualification, stable AST fingerprints, and
deterministic SQL shape for caching and diffing.

Status: [x] Complete.

Code patterns
```python
from dataclasses import replace

from sqlglot_tools.optimizer import NormalizeExprOptions, default_sqlglot_policy, normalize_expr

policy = replace(
    default_sqlglot_policy(),
    read_dialect="datafusion",
    write_dialect="datafusion",
)
normalized = normalize_expr(
    expr,
    options=NormalizeExprOptions(schema=schema_map, policy=policy, sql=raw_sql),
)
```

```python
from sqlglot.diff import diff
from sqlglot.optimizer import annotate_types, canonicalize, normalize_identifiers
from sqlglot.optimizer.normalize import normalization_distance, normalize as normalize_predicates
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates
from sqlglot.optimizer.pushdown_projections import pushdown_projections

qualified = normalize_identifiers(normalized, dialect=policy.write_dialect)
typed = annotate_types(qualified, schema=schema_map, dialect=policy.write_dialect)
canonical = canonicalize(typed, dialect=policy.write_dialect)
if normalization_distance(canonical) <= 64:
    canonical = normalize_predicates(canonical, dnf=False, max_distance=64)
canonical = pushdown_predicates(canonical)
canonical = pushdown_projections(canonical)
script = diff(normalized, canonical)
```

Target files
- `src/sqlglot_tools/optimizer.py`
- `src/sqlglot_tools/bridge.py`
- `src/sqlglot_tools/lineage.py`
- `src/ibis_engine/compiler_checkpoint.py`
- `src/ibis_engine/plan_diff.py`
- `src/ibis_engine/lineage.py`
- `src/engine/plan_cache.py`
- `src/obs/manifest.py`

Implementation checklist
- [x] Build schema maps from info_schema for qualification and type inference.
- [x] Enforce `qualify_outputs` and `validate_qualify_columns` for all compiled SQL.
- [x] Persist canonical AST fingerprints and policy snapshots per rule output.
- [x] Gate incremental rebuilds on semantic diffs and plan hashes.
- [x] Normalize identifiers with stored originals for deterministic cache keys.
- [x] Run type annotation + canonicalization before plan hashing.
- [x] Apply CNF predicate normalization with distance guardrails.
- [x] Push down projections/predicates and record required-column artifacts.

## Scope 5: Delta Lake + DataFusion provider unification
Objective: register Delta tables as DataFusion providers (no Arrow dataset fallback)
and standardize read/write policy alignment.

Status: [x] Complete.

Code patterns
```python
from datafusion import SessionContext
from deltalake import DeltaTable

def register_delta(ctx: SessionContext, *, name: str, path: str, opts: dict[str, str]) -> None:
    ctx.register_table(name, DeltaTable(path, storage_options=opts))
```

```python
from datafusion import SessionConfig, SessionContext

from schema_spec.system import DeltaScanOptions

config = SessionConfig().set("datafusion.sql_parser.enable_ident_normalization", "false")
ctx = SessionContext(config)
scan = DeltaScanOptions(file_column_name="_file", enable_parquet_pushdown=True)
```

Target files
- `src/storage/deltalake/config.py`
- `src/storage/deltalake/delta.py`
- `src/storage/dataset_sources.py`
- `src/ibis_engine/sources.py`
- `src/ibis_engine/io_bridge.py`
- `src/engine/materialize.py`
- `src/engine/runtime_profile.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/incremental/changes.py`
- `src/incremental/deltas.py`
- `src/incremental/snapshot.py`
- `src/schema_spec/system.py`

Implementation checklist
- [x] Require TableProvider registration for Delta (no dataset fallback).
- [x] Thread storage options and scan policies through registry/location config.
- [x] Use DataFusion `INSERT`/`COPY` for append/overwrite and delta-rs for merge/update.
- [x] Add CDF as a first-class table provider for incremental pipelines.
- [x] Map Delta scan knobs (file column, schema override, view types) into registration options.
- [x] Enforce case-sensitive identifier toggle in runtime profile with diagnostics snapshots.
- [x] Use DataFusion plan outputs + `write_deltalake` for provider writes when insert is unsupported.

## Scope 6: Nested schema shaping via DataFusion functions
Objective: rely on DataFusion built-ins for struct/map/union shaping and Arrow
metadata inspection so nested outputs are schema-correct without custom logic.

Status: [x] Complete.

Code patterns
```sql
select
  named_struct('name', name, 'loc', struct(line, col)) as sym,
  map('kind', kind, 'module', module) as attrs
from symbols
```

```python
from datafusion import functions as F

expr = df.select(
    F.named_struct("name", F.col("name"), "loc", F.struct(F.col("line"), F.col("col"))).alias("sym")
)
```

```sql
select
  arrow_typeof(node) as node_type,
  arrow_metadata(node) as node_meta,
  union_tag(node_variant) as variant_tag
from cpg_nodes
```

Target files
- `src/arrowdsl/schema/schema.py`
- `src/arrowdsl/spec/expr_ir.py`
- `src/arrowdsl/spec/codec.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/extract_templates.py`
- `src/cpg/prop_transforms.py`

Implementation checklist
- [x] Replace bespoke nested-type projection logic with DataFusion functions.
- [x] Validate nested types via `arrow_typeof` and `arrow_metadata` in diagnostics.
- [x] Standardize struct/map expansion with `unnest` and `get_field`.
- [x] Use `map_entries`/`map_extract` to normalize attribute bags for analytics.
- [x] Handle union types with `union_tag`/`union_extract` when present.

## Scope 7: Observability + error handling alignment
Objective: remove fallback diagnostics and replace with strict, DataFusion-native
error surfaces plus canonical diagnostics artifacts.

Status: [x] Complete.

Code patterns
```python
def _runtime_notes(ctx: SessionContext) -> dict[str, object]:
    return {"datafusion_version": ctx.sql("select version()").collect()[0][0]}
```

Target files
- `src/obs/manifest.py`
- `src/obs/diagnostics_tables.py`
- `src/obs/diagnostics.py`
- `src/datafusion_engine/runtime.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/obs/repro.py`

Implementation checklist
- [x] Replace fallback diagnostics with info_schema + plan hash diagnostics.
- [x] Persist SQLGlot policy snapshots and DDL fingerprints in manifests.
- [x] Make fallback disallowed by default in runtime profiles.
- [x] Persist schema_map fingerprints and SQLGlot diff scripts in artifacts.
- [x] Capture function inventory snapshots derived from information_schema.

## Scope 8: SQL ingestion policy lane (strict parse + normalize before Ibis)
Objective: normalize SQL inputs through SQLGlot policy before Ibis parsing and
capture artifacts for reproducibility.

Status: [x] Complete.

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
- `src/sqlglot_tools/bridge.py`
- `src/obs/manifest.py`
- `src/obs/repro.py`

Implementation checklist
- [x] Normalize ingest SQL via SQLGlot policy before `ibis.parse_sql`.
- [x] Enforce `unsupported_level=RAISE` and strict parse errors for ingestion.
- [x] Persist ingestion AST payloads, policy snapshots, and parse failures.
- [x] Require explicit schema + catalog mappings for all ingest SQL.

## Scope 9: Function registry allowlist + UDF inventory
Objective: remove implicit function fallbacks and gate supported functions against
DataFusion/Ibis inventory.

Status: [x] Complete.

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
- `src/engine/function_registry.py`
- `src/datafusion_engine/schema_introspection.py`
- `src/obs/diagnostics_tables.py`

Implementation checklist
- [x] Remove `getattr(ibis, ...)` fallback resolution.
- [x] Register builtin DataFusion UDFs via `@ibis.udf.scalar.builtin`.
- [x] Validate rulepack function usage against info_schema routines/parameters.
- [x] Publish allowlist inventories as diagnostics artifacts.

## Legacy decommission list (post-migration)
Remove these once the above scopes are complete and tests pass.

- [x] DataFusion SQL fallback paths and diagnostics:
  - [x] `src/datafusion_engine/bridge.py` (`SqlFallbackContext`, fallback hooks, diagnostics emitters)
  - [x] `src/datafusion_engine/compile_options.py` (fallback policy structures)
  - [x] `src/obs/diagnostics_tables.py` (`datafusion_fallbacks_table`)
  - [x] `src/hamilton_pipeline/modules/outputs.py` (fallback notes export)
- [x] Ibis fallback execution and SQLGlot bridge fallbacks:
  - [x] `src/ibis_engine/runner.py` (fallback branches)
  - [x] `src/ibis_engine/execution.py` (`allow_fallback` plumbing)
  - [x] `src/relspec/graph.py` (fallback plan paths)
  - [x] `src/hamilton_pipeline/modules/inputs.py` (fallback policy toggles)
- [x] Dataset-provider fallbacks for Delta registration:
  - [x] `src/datafusion_engine/registry_loader.py`
    (`_register_with_cache_fallback`, `_should_fallback_to_dataset`)
  - [x] `src/datafusion_engine/registry_bridge.py` (dataset provider fallback paths)
- [x] Direct MemTable registration helpers (prefer `create_table` / `register_dataset_df`):
  - [x] `src/datafusion_engine/bridge.py`
    (`register_memtable*`, `slice_memtable_batches`)
- [x] Named memtable usage for stable datasets (replace with `create_table` / `create_view`):
  - [x] `src/normalize/ibis_api.py`
  - [x] `src/normalize/ibis_plan_builders.py`
  - [x] `src/relspec/graph.py`
  - [x] `src/relspec/cpg/build_edges.py`
  - [x] `src/relspec/cpg/build_nodes.py`
  - [x] `src/relspec/cpg/build_props.py`
- [x] Implicit Ibis function fallback resolution:
  - [x] `src/ibis_engine/expr_compiler.py` (`getattr(ibis, ...)` fallback)
- [x] Delta CDF Arrow materialization fallbacks:
  - [x] `src/datafusion_engine/registry_bridge.py` (fallback to `read_delta_cdf`)
  - [x] `src/storage/deltalake/delta.py` (`read_delta_cdf`)
  - [x] `src/storage/io.py` (re-export)
  - [x] `src/storage/__init__.py` (re-export)
- [x] Direct delta-rs reads used for incremental snapshots:
  - [x] `src/incremental/scip_snapshot.py`
  - [x] `src/incremental/snapshot.py`
  - [x] `src/incremental/invalidations.py`
- [x] Bespoke nested-shaping helpers superseded by DataFusion functions:
  - [x] `src/arrowdsl/schema/schema.py`
  - [x] `src/arrowdsl/spec/codec.py`
  - [x] `src/datafusion_engine/query_fragments.py`
- [x] Tests/docs that assume fallback execution:
  - [x] `tests/integration/test_datafusion_fallbacks.py`

## Quality gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13`
