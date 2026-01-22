# DataFusion Advanced Leverage Implementation Plan

## Goals
- Extend the unified integration plans with advanced DataFusion-native surfaces.
- Remove bespoke schema/constraint logic in favor of information_schema.
- Make DataFusion DataFrame/Expr the only query surface (no SQL fragments).
- Make streaming and plan artifacts first-class outputs.
- Consolidate output writes under DataFusion COPY/INSERT and writer options.
- Eliminate param-table registry plumbing via UDTF/unnest/values patterns.

## Relationship to existing plans
- Builds on `docs/plans/datafusion_unified_integration_plan.md`.
- Builds on `docs/plans/datafusion_unified_integration_extension_plan.md`.

## Preconditions / gates
- DataFusion Python >= 51 for analyze-level explain output and unparser stability.
- DataFusion Python >= 43 for information_schema and TableProvider registration.
- ibis-framework[datafusion] for backend bridging and selector APIs.
- SQLGlot pinned to match `src/sqlglot_tools/optimizer.py` policy.

## Status legend
- [x] Complete
- [~] Partially complete
- [ ] Remaining

## Execution phases (ordered)
Phase 1: Contract + safety foundation (Scopes 1, 10).
Phase 2: Plan-first architecture (Scopes 2, 3).
Phase 3: Streaming outputs + runtime knobs (Scopes 4, 8, 9).
Phase 4: Plan artifacts + portability (Scopes 6, 7).
Phase 5: Param-table replacement + cleanup (Scope 5 + legacy list).

## Scope 1: information_schema constraints as the canonical contract
Objective: replace bespoke relationship and constraint validation with information_schema
tables for PK/FK/unique checks and runtime drift detection.

Status: [x] Complete.

Code patterns
```python
from datafusion import SessionContext

def constraint_rows(ctx: SessionContext, *, catalog: str, schema: str) -> list[dict[str, object]]:
    sql = """
    SELECT
      tc.table_catalog,
      tc.table_schema,
      tc.table_name,
      tc.constraint_name,
      tc.constraint_type,
      kcu.column_name
    FROM information_schema.table_constraints tc
    LEFT JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
    WHERE tc.table_catalog = '{catalog}' AND tc.table_schema = '{schema}'
    ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
    """
    return [row.as_dict() for row in ctx.sql(sql.format(catalog=catalog, schema=schema)).collect()]
```

Target files
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/schema_registry.py`
- `src/schema_spec/relationship_specs.py`
- `src/arrowdsl/schema/validation.py`
- `src/obs/diagnostics_tables.py`

Implementation checklist
- [x] Add constraint inventory query helpers for PK/unique/FK.
- [x] Validate key fields against information_schema constraints during registration.
- [x] Include constraint rows in schema snapshot artifacts.
- [x] Replace bespoke constraint validation with information_schema-based checks.
- [x] Emit constraint drift diagnostics when registered schema deviates.
- [x] Gate relationship specs against constraints (no duplicate logic in Python).

## Scope 2: Lazy views and Table wrappers as the default registry type
Objective: keep DataFusion plans lazy; register views instead of materialized Arrow tables
whenever possible, and use `datafusion.catalog.Table` as the universal registry handle.

Status: [x] Complete.

Code patterns
```python
from datafusion import SessionContext
from datafusion.catalog import Table

def register_lazy_view(ctx: SessionContext, *, name: str, df) -> None:
    view = df.into_view(temporary=True)
    ctx.register_table(name, view)

def table_handle(obj: object) -> Table:
    return Table(obj)
```

Target files
- `src/datafusion_engine/catalog_provider.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/ibis_engine/registry.py`

Implementation checklist
- [x] Convert registry registration to `register_view`/`into_view` for plan outputs.
- [x] Register fragment + nested schema views via DataFrame builders (`into_view`).
- [x] Register symtable-derived CPG views via DataFrame builders (no SQL DDL).
- [x] Stop calling `to_arrow_table` for registry flows outside diagnostics.
- [x] Store `Table` wrappers in catalog providers instead of raw datasets.

## Scope 3: Expr-first templates (replace SQL fragment strings)
Objective: replace raw SQL fragments with DataFusion Expr and DataFrame APIs so
fragment construction is schema-aware and compiler-validated.

Status: [x] Complete.

Code patterns
```python
from datafusion import functions as f

def filter_high_confidence(df):
    expr = df.parse_sql_expr("confidence >= 0.95")
    return df.filter(expr).with_column("rank", f.row_number().alias("rank"))
```

Target files
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/df_builder.py`
- `src/datafusion_engine/extract_templates.py`
- `src/datafusion_engine/kernels.py`

Implementation checklist
- [x] Normalize fragment SQL into SQLGlot expressions and build DataFrames directly.
- [x] Replace SQL fragment generators with Expr/DataFrame transformations.
- [ ] Route any remaining raw SQL through `parse_sql_expr`.
- [x] Delete fragment-to-string paths once all consumers use Expr templates.
- [x] Register fragment ViewSpecs via DataFrame builders (SQLGlot → DataFrame).
- [x] Add CTE + UNNEST handling in the DataFrame translator to support fragment builders.
- [x] Replace symtable SQL view generation with DataFrame builders.

## Scope 4: Streaming-first outputs (Arrow C Stream and partitioned streams)
Objective: standardize output surfaces on streaming RecordBatch readers and Arrow
C Stream exports; minimize full materialization in hot paths.

Status: [x] Complete.

Code patterns
```python
from datafusion_engine.bridge import datafusion_to_reader

def stream_results(df, *, ordering):
    reader = datafusion_to_reader(df, ordering=ordering)
    for batch in reader:
        yield batch
```

Target files
- `src/datafusion_engine/bridge.py`
- `src/ibis_engine/io_bridge.py`
- `src/engine/materialize.py`
- `src/arrowdsl/core/interop.py`

Implementation checklist
- [x] Provide streaming reader helpers (datafusion_to_reader, df_to_reader) and prefer_reader outputs.
- [ ] Treat `execute_stream_partitioned` as the default for large outputs.
- [ ] Avoid `to_arrow_table` in non-diagnostic code paths.
- [ ] Route large output writes from readers rather than tables.

## Scope 5: Param-table replacement with UDTF/unnest/VALUES
Objective: remove custom param-table registry plumbing and use DataFusion-native
table functions or unnest-based parameter expansion.

Status: [~] Partially complete.

Code patterns
```python
from datafusion import udtf
from datafusion.catalog import Table
import json
import pyarrow as pa
import pyarrow.dataset as ds

@udtf("param_table")
def param_table(values_json: str) -> Table:
    values = json.loads(values_json)
    table = pa.table({"value": values})
    return Table(ds.dataset(table))
```

Target files
- `src/ibis_engine/param_tables.py`
- `src/datafusion_engine/param_tables.py`
- `src/ibis_engine/params_bridge.py`
- `src/ibis_engine/registry.py`
- `src/datafusion_engine/runtime.py`

Implementation checklist
- [x] Replace param-table registry with VALUES/memtable expansions.
- [x] Remove custom catalog/schema prefixes for params.
- [x] Ensure parameter usage is validated by information_schema routines.

## Scope 6: Plan artifacts as first-class outputs
Objective: persist logical, optimized, physical, and explain artifacts for every
plan build to enable reproducibility and performance diffs.

Status: [x] Complete.

Code patterns
```python
from datafusion import DataFrame

def plan_artifacts(df: DataFrame) -> dict[str, str]:
    return {
        "logical": str(df.logical_plan()),
        "optimized": str(df.optimized_logical_plan()),
        "physical": str(df.execution_plan()),
    }
```

Target files
- `src/obs/manifest.py`
- `src/obs/repro.py`
- `src/ibis_engine/compiler_checkpoint.py`
- `src/engine/plan_cache.py`
- `src/datafusion_engine/runtime.py`

Implementation checklist
- [x] Capture logical/optimized/physical plan text for each rule output.
- [x] Enable `EXPLAIN ANALYZE` with profile-configured analyze level.
- [x] Persist plan artifacts alongside schema and runtime snapshots.

## Scope 7: Substrait + unparser artifacts for portability
Objective: store Substrait bytes and unparsed SQL for deterministic diffing and
cross-engine replay.

Status: [~] Partially complete.

Code patterns
```python
from datafusion.substrait import Consumer, Serde
from datafusion.unparser import Dialect, Unparser

substrait = Serde.serialize_bytes(sql, ctx)
plan = Serde.deserialize_bytes(substrait)
logical = Consumer.from_substrait_plan(ctx, plan)
sql_text = str(Unparser(Dialect.default()).plan_to_sql(logical))
```

Target files
- `src/datafusion_engine/bridge.py`
- `src/ibis_engine/plan_diff.py`
- `src/engine/plan_cache.py`
- `src/obs/manifest.py`
- `src/obs/repro.py`

Implementation checklist
- [x] Store Substrait bytes for each plan build and attach to artifacts.
- [x] Store unparsed SQL for human-readable review diffs.
- [ ] Gate cache keys on Substrait fingerprints + runtime profile.

## Scope 8: Output parallelism + parquet writer knobs in runtime profiles
Objective: surface write-time performance knobs in runtime profiles and apply
them consistently for COPY/INSERT/Parquet writes.

Status: [~] Partially complete.

Code patterns
```python
config = (
    config.set("datafusion.execution.minimum_parallel_output_files", "8")
    .set("datafusion.execution.soft_max_rows_per_output_file", "5000000")
    .set("datafusion.execution.maximum_parallel_row_group_writers", "4")
)
```

Target files
- `src/datafusion_engine/runtime.py`
- `src/engine/runtime_profile.py`
- `src/datafusion_engine/bridge.py`
- `src/engine/materialize.py`

Implementation checklist
- [ ] Add output parallelism keys to runtime profile serialization.
- [x] Apply parquet writer options via `write_parquet_with_options`.
- [x] Record write options in diagnostics artifacts.

## Scope 9: Join/partition shaping and optimizer alignment
Objective: make partitioning explicit for join-heavy kernels and expose knobs
for dynamic filter pushdown and repartition behavior.

Status: [~] Partially complete.

Code patterns
```python
df = df.repartition_by_hash("repo", num=execution.target_partitions)
joined = df.join(right, on="repo", how="inner")
```

Target files
- `src/datafusion_engine/kernels.py`
- `src/datafusion_engine/runtime.py`
- `src/engine/runtime_profile.py`

Implementation checklist
- [ ] Add repartition shaping where joins are hot and stable keys exist.
- [ ] Expose round-robin repartition and perfect-hash join knobs in profiles.
- [x] Record optimizer settings in diagnostics snapshots.

## Scope 10: SQL safety gating for all execution surfaces
Objective: enforce `SQLOptions` for all SQL entry points and remove implicit
unsafe execution paths.

Status: [x] Complete.

Code patterns
```python
from datafusion import SQLOptions

opts = SQLOptions().with_allow_ddl(False).with_allow_dml(False)
df = ctx.sql_with_options(sql, opts, param_values=params)
```

Target files
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/schema_introspection.py`

Implementation checklist
- [x] Apply `sql_with_options` for runtime diagnostics and plan artifact queries.
- [x] Enforce `sql_with_options` in schema introspection helpers.
- [x] Use SQL policy helpers derived from the runtime profile.
- [x] Expand `sql_with_options` usage beyond runtime (schema registry, validation, registry bridge).
- [x] Replace `ctx.sql` with `ctx.sql_with_options` everywhere.
- [x] Centralize SQL option policy in runtime profile configuration.
- [x] Reject unsafe SQL categories for untrusted inputs (policy violations raise).

## Legacy decommission list (post-migration)
Remove these once all scopes are complete and tests pass.

- SQL fragment modules superseded by Expr templates:
  - `src/datafusion_engine/query_fragments.py`
- Bespoke parquet/Arrow output writers superseded by DataFusion writes:
  - `src/arrowdsl/io/parquet.py`
  - `src/obs/parquet_writers.py`
- Param-table registry plumbing superseded by UDTF/unnest:
  - `src/ibis_engine/param_tables.py`
  - `src/datafusion_engine/param_tables.py`
- Ordering metadata glue superseded by `WITH ORDER` and file_sort_order:
  - `src/arrowdsl/core/ordering_policy.py`
  - Ordering helpers in `src/ibis_engine/scan_io.py`

## Quality gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13.11`
- `uv run pytest -q`
