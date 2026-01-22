# Engine Overhaul + Advanced Integration Plan

## Goals
- Make `src/engine` the single composition root for DataFusion, Ibis, SQLGlot, and Delta.
- Default all execution to DataFusion-native paths (DDL registration, COPY/INSERT, Delta providers).
- Treat SQLGlot AST as the canonical SQL contract, with dialect-aware normalization.
- Unify write policies and runtime governance (memory, spill, parallelism, write options).
- Decommission legacy write paths, static registries, and string-only SQL round-trips.

## Status summary (updated)
- Scopes 1–9 are implemented.

## Inputs / references
- `docs/palns/combined_advanced_library_utilization_plan.md`
- `docs/plans/combined_extraction_source_improvements_plan.md`
- `docs/python_library_reference/datafusion.md`
- `docs/python_library_reference/datafusion_schema.md`
- `docs/python_library_reference/ibis_advanced_integration.md`
- `docs/python_library_reference/sqlglot_advanced_integration.md`
- `docs/python_library_reference/deltalake.md`
- `docs/python_library_reference/deltalake_datafusion_integration.md`

## Scope 1: Engine composition root (runtime + catalog + compiler)
Objective: make `engine` the authoritative composition layer for runtime settings, catalog
registration, SQL compilation, and diagnostics wiring.

Representative code
```python
from dataclasses import dataclass

from arrowdsl.core.execution_context import ExecutionContext
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from sqlglot_tools.optimizer import SqlGlotPolicy


@dataclass(frozen=True)
class EngineRuntime:
    """Bundle runtime settings for DataFusion, Ibis, and SQLGlot."""

    datafusion: DataFusionRuntimeProfile
    sqlglot_policy: SqlGlotPolicy
    ibis_config: IbisBackendConfig


def build_engine_runtime(ctx: ExecutionContext) -> EngineRuntime:
    df_profile = ctx.runtime.datafusion or DataFusionRuntimeProfile()
    ibis_config = IbisBackendConfig(
        datafusion_profile=df_profile,
        fuse_selects=ctx.runtime.ibis_fuse_selects,
        default_limit=ctx.runtime.ibis_default_limit,
        default_dialect=ctx.runtime.ibis_default_dialect,
        interactive=ctx.runtime.ibis_interactive,
    )
    return EngineRuntime(
        datafusion=df_profile,
        sqlglot_policy=df_profile.sqlglot_policy(),
        ibis_config=ibis_config,
    )
```

Target files
- `src/engine/runtime.py`
- `src/engine/runtime_profile.py`
- `src/engine/session_factory.py`
- `src/engine/session.py`
- `src/engine/__init__.py`
- `src/datafusion_engine/runtime.py`

Implementation checklist
- [x] Introduce an `EngineRuntime` bundle to own DataFusion/Ibis/SQLGlot policy wiring.
- [x] Make `build_engine_session()` consume `EngineRuntime` and persist its snapshots.
- [x] Centralize runtime hashing to include SQLGlot + function catalog + write policy.
- [x] Ensure diagnostics record engine runtime + compiler snapshots once per session.

Module retirements
- [x] Replace ad-hoc runtime wiring scattered in `engine/materialize*` with the new bundle.

---

## Scope 2: Catalog + registration standardization (DDL-first)
Objective: register datasets exclusively via DataFusion DDL (DeltaTableFactory and
UNBOUNDED external tables) and make object-store registration a first-class feature.

Representative code
```python
def register_dataset(ctx: SessionContext, *, name: str, spec: DatasetSpec) -> None:
    ddl = spec.external_table_ddl()
    ctx.sql(ddl).collect()
    ctx.sql(f"ANALYZE TABLE {name}").collect()
```

Target files
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/schema_spec/specs.py`
- `src/engine/session_factory.py`

Implementation checklist
- [x] Require Delta DDL registration via `STORED AS DELTATABLE` in all engine paths.
- [x] Register object stores in runtime init (S3/GCS/Azure/HTTP) before DDL.
- [x] Default streaming sources to `CREATE UNBOUNDED EXTERNAL TABLE`.
- [x] Record DDL statements + table provider metadata in diagnostics.

Module retirements
- [x] Remove direct non-DDL dataset registrations from engine surfaces.

---

## Scope 3: SQLGlot AST execution as the default ingress
Objective: make SQLGlot AST canonicalization the standard and execute via Ibis
DataFusion backend using `raw_sql(Expression)` rather than string round-trips.

Representative code
```python
from sqlglot_tools.optimizer import normalize_expr, parse_sql_strict
from sqlglot_tools.bridge import ibis_to_sqlglot

expr = parse_sql_strict(sql, dialect=policy.read_dialect, error_level=policy.error_level)
normalized = normalize_expr(expr, schema_map=policy.schema_map, policy=policy)
backend.raw_sql(normalized)

ibis_expr = ibis_to_sqlglot(ibis_plan, backend=backend, params=params)
backend.raw_sql(ibis_expr)
```

Target files
- `src/ibis_engine/sql_bridge.py`
- `src/ibis_engine/execution.py`
- `src/datafusion_engine/bridge.py`
- `src/sqlglot_tools/optimizer.py`

Implementation checklist
- [x] Make AST execution the default for DataFusion SQL ingress.
- [x] Enforce dialect-aware `read` + `write` policies on all SQL parsing.
- [x] Persist AST + normalized SQL in diagnostics bundles.
- [x] Add strict error-level policies for unsupported syntax.

Module retirements
- [x] Remove SQL string serialization paths that immediately re-parse into SQLGlot.

---

## Scope 4: Function catalog unification (information_schema + UDF tiers)
Objective: build the function registry from DataFusion `information_schema` and merge
Ibis + PyArrow function tiers into a single lane-aware catalog.

Representative code
```python
catalog = FunctionCatalog.from_information_schema(
    routines=introspector.routines_snapshot(),
    parameters=introspector.parameters_snapshot(),
    parameters_available=True,
)
registry = build_function_registry(
    datafusion_function_catalog=catalog.routines,
    ibis_specs=ibis_udf_specs(),
)
```

Target files
- `src/datafusion_engine/udf_catalog.py`
- `src/datafusion_engine/runtime.py`
- `src/engine/function_registry.py`
- `src/engine/pyarrow_registry.py` (removed)

Implementation checklist
- [x] Build function registries from info_schema snapshots at session build time.
- [x] Merge UDF tiers (`builtin`, `pyarrow`, `pandas`, `python`) with lane precedence.
- [x] Persist function catalog payloads for reproducibility.

Module retirements
- [x] Retire `src/engine/pyarrow_registry.py` after info_schema catalog is canonical.

---

## Scope 5: Unified materialization and write policy pipeline
Objective: consolidate all plan output writes into a single pipeline with DataFusion
`COPY/INSERT` as the default and full `DataFrameWriteOptions` support.

Representative code
```python
from datafusion import DataFrameWriteOptions, ParquetWriterOptions

write_opts = DataFrameWriteOptions(
    partition_by=["repo"],
    sort_by=["path"],
    single_file_output=False,
)
pq_opts = ParquetWriterOptions(
    compression="zstd(5)",
    max_row_group_size=1_000_000,
    statistics_enabled="page",
)
df.write_parquet_with_options(target_path, pq_opts, write_opts)
```

Target files
- `src/engine/materialize_pipeline.py`
- `src/engine/materialize.py` (removed)
- `src/engine/materialize_extract_outputs.py` (removed)
- `src/ibis_engine/io_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/schema_spec/policies.py`

Implementation checklist
- [x] Merge plan materialization + extract output writes into one writer pipeline.
- [x] Expose `DataFrameWriteOptions` + `ParquetWriterOptions` via `DataFusionWritePolicy`.
- [x] Default to `COPY` / `INSERT INTO` for parquet/csv/json outputs.
- [x] Route large outputs through streaming readers (avoid `to_pyarrow_table()`).

Module retirements
- [x] Remove `write_ast_outputs`-style one-off writer functions after the unified pipeline.
- [x] Retire `src/engine/materialize_extract_outputs.py` after consolidation.

---

## Scope 6: Delta integration (read/write, idempotency, CDF, maintenance)
Objective: standardize Delta IO through Ibis + DataFusion providers, wire idempotent
commit properties, and expose CDF + vacuum as engine-level helpers.

Representative code
```python
from storage.deltalake.delta import IdempotentWriteOptions

options = IbisDeltaWriteOptions(
    mode="append",
    storage_options=location.storage_options,
    idempotent=IdempotentWriteOptions(app_id=run_id, version=commit_version),
)
write_ibis_dataset_delta(reader, str(location.path), options=options)
```

Target files
- `src/ibis_engine/sources.py`
- `src/ibis_engine/io_bridge.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/storage/deltalake/delta.py`
- `src/obs/diagnostics_tables.py`

Implementation checklist
- [x] Route all Delta reads through `IbisBackend.read_delta`.
- [x] Default Delta writes to `IbisBackend.to_delta` with schema policies.
- [x] Thread `run_id` and commit version into `CommitProperties`.
- [x] Expose Delta CDF reads via provider-backed registration.
- [x] Add engine helpers for vacuum + history snapshots.

Module retirements
- [x] Retire direct `write_deltalake` calls outside the IO bridge.

---

## Scope 7: Runtime governance and advanced tuning
Objective: expose DataFusion tuning knobs (parallelism, spill, parquet pushdown,
dynamic filters, metadata cache) via `RuntimeProfileSpec`.

Representative code
```python
config = (
    SessionConfig()
    .with_target_partitions(16)
    .with_repartition_joins(True)
    .with_repartition_aggregations(True)
    .with_repartition_windows(True)
    .with_repartition_file_scans(True)
)
runtime = (
    RuntimeEnvBuilder()
    .with_disk_manager_specified("/mnt/df-spill")
    .with_fair_spill_pool(8 * 1024**3)
)
ctx = SessionContext(config, runtime)
```

Target files
- `src/datafusion_engine/runtime.py`
- `src/engine/runtime_profile.py`
- `src/engine/session_factory.py`

Implementation checklist
- [x] Add runtime profile fields for memory pool, spill paths, and cache limits.
- [x] Enable parquet pushdown + dynamic filter pushdown where supported.
- [x] Add per-session object store buffer sizing for remote writes.
- [x] Persist runtime settings in diagnostics artifacts for reproducibility.

Module retirements
- [ ] None (capability expansion).

---

## Scope 8: Observability and artifact completeness
Objective: ensure every compile/execute/write path records SQLGlot policies, runtime
settings, function catalogs, and write metadata.

Representative code
```python
diagnostics.record_artifact(
    "engine_runtime_v1",
    {
        "datafusion": runtime_profile.snapshot(),
        "sqlglot_policy": sqlglot_policy_snapshot(policy).payload(),
        "function_catalog": function_catalog.payload(),
    },
)
```

Target files
- `src/obs/diagnostics_tables.py`
- `src/datafusion_engine/runtime.py`
- `src/engine/runtime_profile.py`
- `src/engine/session_factory.py`

Implementation checklist
- [x] Emit a single engine runtime artifact per session.
- [x] Record SQLGlot AST + normalized SQL for all SQL ingress paths.
- [x] Record write policy metadata for all outputs (Delta + parquet/csv/json).

Module retirements
- [x] Remove duplicate per-module diagnostics that replicate the same payloads.

---

## Scope 9: Decommission and delete legacy modules
Objective: remove obsolete modules and functions after migration to the new architecture.

Modules to decommission
- `src/engine/pyarrow_registry.py` (replaced by info_schema-driven function catalog).
- `src/engine/materialize_extract_outputs.py` (merged into unified writer pipeline).
- `src/engine/materialize.py` (replace with consolidated materialization module).

Functions and paths to decommission
- SQL string re-parse paths in `datafusion_engine/bridge.py` once AST execution is default.
- Direct `write_deltalake` usage outside `ibis_engine/io_bridge.py`.
- Any per-dataset “one-off” output writers superseded by unified pipeline.

Implementation checklist
- [x] Migrate callers to new materialization surface before deleting modules.
- [x] Remove compatibility shims to fully migrate to the target architecture.
- [x] Remove imports/references and update `__all__` exports.

---

## Global legacy decommission list (after full plan completion)
- [x] Static builtin function tables (ensure only info_schema-derived catalogs remain).
- [x] SQL string generation paths that immediately re-parse into SQLGlot.
- [x] Arrow materialization paths used only for Delta writes.
- [x] Per-dataset extract output writers that bypass the unified write policy.

## Acceptance criteria
- [x] Engine builds a single session with DataFusion + Ibis + SQLGlot policy snapshots.
- [x] All dataset registrations are DDL-first (DeltaTableFactory and UNBOUNDED where applicable).
- [x] SQL ingress records normalized AST + dialect policy and executes AST by default.
- [x] Delta writes are idempotent (CommitProperties `app_id` + `version`).
- [x] Write policy supports DataFusion `DataFrameWriteOptions` and Parquet writer tuning.
- [x] Legacy modules are removed with no runtime regressions.
