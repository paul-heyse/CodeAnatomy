# Extract DataFusion Integration Modernization Plan

## Goals
- Make `src/extract` SessionContext-first and DataFusion-native (streaming-first, plan-aware).
- Replace bespoke row/table plumbing with DataFusion/Ibis built-ins.
- Canonicalize SQL paths with SQLGlot and drive query planning through DataFusion/SQLGlot/Ibis.
- Centralize schema introspection/validation on DataFusion `information_schema` + Arrow-aware functions.
- Adopt Delta Lake TableProvider paths for extract inputs/outputs where applicable.

## Inputs
- `docs/python_library_reference/datafusion.md`
- `docs/python_library_reference/datafusion_schema.md`
- `docs/python_library_reference/datafusion_builtin_and_udf.md`
- `docs/python_library_reference/datafusion_advanced_integration.md`
- `docs/python_library_reference/ibis_advanced_integration.md`
- `docs/python_library_reference/sqlglot_advanced_integration.md`
- `docs/python_library_reference/deltalake_datafusion_integration.md`

## Preconditions / gates
- `scripts/bootstrap_codex.sh` then `uv sync`
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13`

---

## Scope 1: SessionContext-first extraction facade
Status: Completed
Objective: provide a single, shared SessionContext + Ibis backend per extraction run and remove ad hoc session creation in extractors.

Representative code
```python
# src/extract/session.py
@dataclass(frozen=True)
class ExtractSession:
    exec_ctx: ExecutionContext
    df_profile: DataFusionRuntimeProfile
    df_ctx: SessionContext
    ibis_backend: BaseBackend


def build_extract_session(ctx: ExecutionContext) -> ExtractSession:
    df_profile = ctx.runtime.datafusion or DataFusionRuntimeProfile()
    df_ctx = df_profile.session_context()
    backend = build_backend(IbisBackendConfig(datafusion_profile=df_profile))
    return ExtractSession(
        exec_ctx=ctx,
        df_profile=df_profile,
        df_ctx=df_ctx,
        ibis_backend=backend,
    )
```

Target files
- [x] `src/extract/session.py` (new)
- [x] `src/extract/helpers.py`
- [x] `src/extract/ast_extract.py`
- [x] `src/extract/cst_extract.py`
- [x] `src/extract/tree_sitter_extract.py`
- [x] `src/extract/bytecode_extract.py`
- [x] `src/extract/symtable_extract.py`
- [x] `src/extract/scip_extract.py`
- [x] `src/extract/runtime_inspect_extract.py`

Implementation checklist
- [x] Add `ExtractSession` + builder bound to `ExecutionContext`.
- [x] Replace `execution_context_factory` calls in extractors with `ExtractSession`.
- [x] Thread `ExtractSession` through extract entrypoints and helpers.
- [x] Ensure DataFusion registry/UDC installs happen once per session.

Legacy decommission
- [x] Per-extractor `SessionContext()` construction.
- [x] Ad hoc backend instantiation in `extract/helpers.py`.

---

## Scope 2: SQLGlot/Ibis worklists (remove string SQL templates)
Status: Completed
Objective: generate worklists via SQLGlot AST or Ibis joins; use DataFusion `information_schema` for existence checks.

Representative code
```python
# src/extract/worklists.py
from sqlglot import exp


def worklist_expr(repo: str, output: str) -> exp.Select:
    repo_tbl = exp.to_table(repo)
    out_tbl = exp.to_table(output)
    join_on = exp.EQ(
        this=exp.column("file_id", table=repo_tbl.alias_or_name),
        expression=exp.column("file_id", table=out_tbl.alias_or_name),
    )
    return (
        exp.select("*")
        .from_(repo_tbl)
        .join(out_tbl, on=join_on, join_type="left")
        .where(exp.or_(
            exp.Is(this=exp.column("file_id", table=out_tbl.alias_or_name), expression=exp.null()),
            exp.IsNot(
                this=exp.column("file_sha256", table=out_tbl.alias_or_name),
                expression=exp.column("file_sha256", table=repo_tbl.alias_or_name),
            ),
        ))
    )
```

Target files
- [x] `src/extract/worklists.py`
- [x] `src/datafusion_engine/df_builder.py`
- [x] `src/sqlglot_tools/optimizer.py`

Implementation checklist
- [x] Replace `_WORKLIST_SQL` templates with SQLGlot expressions.
- [x] Use `df_builder.df_from_sqlglot` to produce DataFusion DataFrames.
- [x] Use `information_schema.tables` to detect output table presence.
- [x] Standardize dialect/qualification policy before generating SQL.

Legacy decommission
- [x] `_WORKLIST_SQL` string templates in `src/extract/worklists.py`.
- [x] `_sql_identifier` quoting helper.

---

## Scope 3: Streaming ingest + Ibis `create_table`/`create_view`
Status: Completed
Objective: remove row-list materialization by registering Arrow streams/record batches directly and building Ibis views over DataFusion tables.

Representative code
```python
# src/extract/helpers.py

def ibis_plan_from_batches(
    name: str,
    reader: pa.RecordBatchReader,
    *,
    session: ExtractSession,
) -> IbisPlan:
    datafusion_from_arrow(session.df_ctx, name=name, value=reader)
    table = session.ibis_backend.table(name)
    return IbisPlan(expr=table, ordering=Ordering.unordered())
```

Target files
- [x] `src/extract/helpers.py`
- [x] `src/datafusion_engine/bridge.py`
- [x] `src/ibis_engine/registry.py`

Implementation checklist
- [x] Replace `ibis_plan_from_rows` and `ibis_plan_from_row_batches` with streaming reader variants.
- [x] Use Ibis `create_table`/`create_view` instead of memtable naming.
- [x] Register Arrow streams via `datafusion_from_arrow` (record batch / Arrow C stream).
- [x] Keep schema alignment via `dataset_schema` and `align_table` only when needed.

Legacy decommission
- [x] `ibis_plan_from_rows` and `ibis_plan_from_row_batches` (row list → table path).
- [x] `pa.Table.from_pylist` in extract entrypoints.

---

## Scope 4: Streaming output materialization via DataFusion
Status: Completed
Objective: use DataFusion streaming outputs and writer options for all extract datasets.

Representative code
```python
# src/engine/materialize_extract_outputs.py
options = DataFrameWriteOptions(
    insert_operation="overwrite",
    partition_by=tuple(location.partition_cols),
    sort_by=tuple(location.sort_cols),
)
parquet_opts = ParquetWriterOptions(compression="zstd(3)")

plan_df = plan.to_dataframe(session.df_ctx)
plan_df.write_parquet_with_options(
    path=str(location.path),
    options=parquet_opts,
    write_options=options,
)
```

Target files
- [x] `src/engine/materialize_pipeline.py`
- [x] `src/extract/helpers.py`
- [x] `src/extract/schema_ops.py`

Implementation checklist
- [x] Prefer `execute_stream`/`__arrow_c_stream__` for non-trivial outputs.
- [x] Use DataFusion `write_parquet_with_options` where SessionContext exists.
- [x] Preserve partitioning/sorting via `DataFrameWriteOptions`.
- [x] Only fall back to `pyarrow.dataset.write_dataset` when DataFusion is unavailable.

Legacy decommission
- [x] Direct `pa.concat_tables` output assembly.
- [x] Per-extractor bespoke write helpers.

---

## Scope 5: Schema introspection + validation via DataFusion
Status: Completed
Objective: treat DataFusion `information_schema` and Arrow-aware SQL functions as the authoritative schema validation layer.

Representative code
```python
# src/extract/schema_ops.py
sql = """
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = ?
ORDER BY ordinal_position
"""
columns = session.df_ctx.sql(sql, params=[name]).collect()
```

```sql
-- DataFusion SQL surface
SELECT
  arrow_typeof(col) AS dtype,
  arrow_metadata(col) AS meta
FROM my_extract_view
```

Target files
- [x] `src/extract/schema_ops.py`
- [x] `src/datafusion_engine/schema_registry.py`
- [x] `src/datafusion_engine/schema_introspection.py`

Implementation checklist
- [x] Replace schema validation lookups with `information_schema.columns`.
- [x] Use `arrow_typeof`/`arrow_cast` for runtime alignment checks.
- [x] Persist schema snapshots in diagnostics tables for extract runs.

Legacy decommission
- [x] Python-only schema drift checks that duplicate `information_schema`.

---

## Scope 6: DataFusion built-ins + UDF consolidation
Status: Completed
Objective: move bespoke per-extractor compute into DataFusion built-ins/UDFs and register once per SessionContext.

Representative code
```python
# src/datafusion_engine/udf_registry.py
@udf([pa.string()], pa.string(), "stable", name="normalize_path")
def normalize_path_udf(arr: pa.Array) -> pa.Array:
    return pa.array([str(value).replace("\\\\", "/") for value in arr])
```

Target files
- [x] `src/datafusion_engine/udf_registry.py`
- [x] `src/datafusion_engine/udf_catalog.py`
- [x] `src/extract/helpers.py`

Implementation checklist
- [x] Inventory bespoke PyArrow compute in extractors.
- [x] Replace with DataFusion built-ins or UDFs.
- [x] Register UDFs via `DataFusionRuntimeProfile._install_udfs`.

Legacy decommission
- [x] Ad hoc PyArrow compute pipelines inside extractors.

---

## Scope 7: Delta Lake TableProvider integration for extract inputs/outputs
Status: Completed
Objective: register Delta tables directly and avoid Arrow dataset fallback; adopt CDF where available.

Representative code
```python
# src/datafusion_engine/registry_bridge.py
from deltalake import DeltaTable


def register_delta_table(ctx: SessionContext, name: str, location: str) -> None:
    table = DeltaTable(location)
    ctx.register_table(name, table)
```

Target files
- [x] `src/datafusion_engine/registry_bridge.py`
- [x] `src/datafusion_engine/registry_loader.py`
- [x] `src/extract/helpers.py`

Implementation checklist
- [x] Register Delta-backed datasets as `TableProvider`.
- [x] Add CDF provider integration where change feeds are configured.
- [x] Update dataset locations to include Delta metadata (version/timestamp).

Legacy decommission
- [x] `register_dataset` Arrow Dataset fallback for Delta.
- [x] Custom file-scanning for Delta inputs.

---

## Scope 8: Plan artifacts + diagnostics
Status: Completed
Objective: persist logical/physical/Substrait artifacts for extract runs and attach them to diagnostics.

Representative code
```python
# src/obs/diagnostics_tables.py
plan = df.execution_plan().display_indent()
substrait = datafusion.substrait.Serde().serialize_plan(df)
collector.add_plan_artifact(dataset=name, plan=plan, substrait=substrait)
```

Target files
- [x] `src/obs/diagnostics_tables.py`
- [x] `src/datafusion_engine/runtime.py`
- [x] `src/extract/helpers.py`

Implementation checklist
- [x] Capture logical + physical plan text for each extract.
- [x] Store Substrait bytes when available.
- [x] Link artifacts to settings hash and runtime profile hash.

Legacy decommission
- [x] Ad hoc logging of SQL strings without plan artifacts.

---

## Module retirements (post-migration)
- [x] `src/extract/worklists.py` string-SQL path (`_WORKLIST_SQL`, `_sql_identifier`).
- [x] `src/extract/helpers.py` row-list ingestion utilities (`ibis_plan_from_rows`, `ibis_plan_from_row_batches`).
- [x] Per-extractor bespoke write helpers that bypass DataFusion (`write_*_outputs` patterns).
- [x] Any Delta input paths that register Arrow Datasets instead of `DeltaTable` providers.

## Global implementation checklist
- [x] Introduce `ExtractSession` and thread through all extract entrypoints.
- [x] Replace SQL strings with SQLGlot/Ibis AST paths for worklists.
- [x] Register Arrow streams directly in DataFusion; remove row-list materialization.
- [x] Standardize streaming output writes through DataFusion.
- [x] Centralize schema validation in `information_schema` + Arrow SQL functions.
- [x] Replace bespoke compute with DataFusion built-ins/UDFs.
- [x] Register Delta providers + optional CDF.
- [x] Persist plan artifacts for diagnostics.
