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

## Scope 1: DataFusion catalog + information_schema as source of truth
Objective: enable DataFusion auto-loading and treat information_schema as the canonical
introspection layer for schema, columns, and routines.

Status: [~] Mostly complete (info_schema helper consolidation remains).

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
```

Target files
- `src/datafusion_engine/runtime.py`
- `src/engine/runtime_profile.py`
- `src/engine/session_factory.py`
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/schema_registry.py`
- `src/obs/diagnostics_tables.py`

Implementation checklist
- [x] Add runtime profile settings for catalog autoload and information_schema.
- [~] Centralize info_schema query helpers and remove bespoke introspection SQL.
- [x] Persist info_schema snapshots to diagnostics for schema drift visibility.
- [~] Validate expected schema against info_schema for all registered inputs.

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

Target files
- `src/schema_spec/specs.py`
- `src/schema_spec/relationship_specs.py`
- `src/schema_spec/view_specs.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/query_fragments.py`

Implementation checklist
- [x] Emit SQLGlot column defs with `NOT NULL`, `DEFAULT`, and `PRIMARY KEY` constraints.
- [x] Add `PARTITIONED BY`, `WITH ORDER`, and `UNBOUNDED` surfaces where appropriate.
- [x] Normalize DDL through `sqlglot_tools.optimizer` before registration.
- [x] Store DDL fingerprints in diagnostics/manifest outputs.

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

Target files
- `src/ibis_engine/backend.py`
- `src/ibis_engine/runner.py`
- `src/ibis_engine/registry.py`
- `src/ibis_engine/plan.py`
- `src/ibis_engine/io_bridge.py`
- `src/normalize/ibis_plan_builders.py`
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

## Scope 4: SQLGlot compiler policy hardening
Objective: enforce schema-aware qualification, stable AST fingerprints, and
deterministic SQL shape for caching and diffing.

Status: [~] Partially complete (schema-map propagation and plan-hash gating remain).

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
- [~] Build schema maps from info_schema for qualification and type inference.
- [x] Enforce `qualify_outputs` and `validate_qualify_columns` for all compiled SQL.
- [x] Persist canonical AST fingerprints and policy snapshots per rule output.
- [~] Gate incremental rebuilds on semantic diffs and plan hashes.

## Scope 5: Delta Lake + DataFusion provider unification
Objective: register Delta tables as DataFusion providers (no Arrow dataset fallback)
and standardize read/write policy alignment.

Status: [~] Partially complete (provider-only reads and write-path alignment remain).

Code patterns
```python
from datafusion import SessionContext
from deltalake import DeltaTable

def register_delta(ctx: SessionContext, *, name: str, path: str, opts: dict[str, str]) -> None:
    ctx.register_table(name, DeltaTable(path, storage_options=opts))
```

Target files
- `src/storage/deltalake/config.py`
- `src/storage/deltalake/delta.py`
- `src/ibis_engine/sources.py`
- `src/ibis_engine/io_bridge.py`
- `src/engine/materialize.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/incremental/changes.py`
- `src/incremental/deltas.py`
- `src/incremental/snapshot.py`

Implementation checklist
- [~] Require TableProvider registration for Delta (no dataset fallback).
- [~] Thread storage options and scan policies through registry/location config.
- [ ] Use DataFusion `INSERT`/`COPY` for append/overwrite and delta-rs for merge/update.
- [~] Add CDF as a first-class table provider for incremental pipelines.

## Scope 6: Nested schema shaping via DataFusion functions
Objective: rely on DataFusion built-ins for struct/map/union shaping and Arrow
metadata inspection so nested outputs are schema-correct without custom logic.

Status: [ ] Remaining.

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

Target files
- `src/arrowdsl/schema/schema.py`
- `src/arrowdsl/spec/expr_ir.py`
- `src/arrowdsl/spec/codec.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/extract_templates.py`
- `src/cpg/prop_transforms.py`

Implementation checklist
- [ ] Replace bespoke nested-type projection logic with DataFusion functions.
- [ ] Validate nested types via `arrow_typeof` and `arrow_metadata` in diagnostics.
- [ ] Standardize struct/map expansion with `unnest` and `get_field`.

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

Implementation checklist
- [x] Replace fallback diagnostics with info_schema + plan hash diagnostics.
- [x] Persist SQLGlot policy snapshots and DDL fingerprints in manifests.
- [x] Make fallback disallowed by default in runtime profiles.

## Legacy decommission list (post-migration)
Remove these once the above scopes are complete and tests pass.

- [x] DataFusion SQL fallback paths and diagnostics:
  - [x] `src/datafusion_engine/bridge.py` (`SqlFallbackContext`, fallback hooks, diagnostics emitters)
  - [x] `src/datafusion_engine/compile_options.py` (fallback policy structures)
  - [x] `src/obs/diagnostics_tables.py` (`datafusion_fallbacks_table`)
  - [x] `src/hamilton_pipeline/modules/outputs.py` (fallback notes export)
- [~] Ibis fallback execution and SQLGlot bridge fallbacks:
  - [x] `src/ibis_engine/runner.py` (fallback branches)
  - [x] `src/ibis_engine/execution.py` (`allow_fallback` plumbing)
  - [~] `src/relspec/graph.py` (fallback plan paths)
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
- [ ] Tests/docs that assume fallback execution:
  - [ ] `tests/integration/test_datafusion_fallbacks.py`

## Quality gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13`
