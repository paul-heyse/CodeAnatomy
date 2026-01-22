# Normalize DataFusion + Delta Overhaul Plan

This plan rebuilds `src/normalize` around a DataFusion-owned execution
substrate, Delta-backed outputs by default, and nested span objects. It
integrates Ibis and SQLGlot capabilities, removes fallback paths, and ships
the advanced features we have documented but not yet deployed.

## Goals
- Delta-backed normalize outputs by default with no Parquet or Arrow fallback.
- Nested span structs and maps as the canonical schema for span-bearing data.
- DataFusion-first execution with SQLGlot canonicalization and lineage.
- Idempotent Delta writes with CDF-ready outputs and complete diagnostics.
- Contract-first DDL registration for both input and output datasets.

## Preconditions / gates
- `scripts/bootstrap_codex.sh` then `uv sync`
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13`

---

## Scope 1: Normalize runtime substrate (DataFusion-owned SessionContext)

**Status:** in progress (runtime substrate landed)

**Objective:** centralize normalize execution around a DataFusion
SessionContext and Ibis DataFusion backend, with a single contract for SQL
policy, diagnostics, and execution labels.

**Representative code snippet**
```python
# src/normalize/runtime.py
from dataclasses import dataclass

import ibis
from datafusion import SessionContext
from ibis.backends import BaseBackend

from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.sql_options import sql_options_for_profile
from sqlglot_tools.optimizer import register_datafusion_dialect


@dataclass(frozen=True)
class NormalizeRuntime:
    ctx: SessionContext
    ibis_backend: BaseBackend
    sql_policy: object
    diagnostics: object | None


def build_normalize_runtime(profile: DataFusionRuntimeProfile) -> NormalizeRuntime:
    ctx = profile.session_context()
    register_datafusion_dialect()
    backend = ibis.datafusion.connect(ctx=ctx)
    return NormalizeRuntime(
        ctx=ctx,
        ibis_backend=backend,
        sql_policy=sql_options_for_profile(profile),
        diagnostics=profile.diagnostics_sink,
    )
```

**Target files**
- `src/normalize/runtime.py` (new)
- `src/normalize/runner.py`
- `src/ibis_engine/execution.py`
- `src/datafusion_engine/runtime.py`

**Implementation checklist**
- [x] Add NormalizeRuntime and wire it into normalize execution entrypoints.
- [x] Require DataFusion-backed Ibis execution for all normalize runs.
- [x] Propagate execution labels into normalize execution.
- [ ] Thread SQL policy/options into normalize plan compilation and execution paths.

---

## Scope 2: DDL-first registration for normalize inputs and outputs

**Status:** in progress (outputs registered via DDL; inputs pending)

**Objective:** register all normalize inputs and outputs through DDL, using
DeltaTableFactory for Delta-backed tables and DataFusion schema contracts.

**Representative code snippet**
```python
# src/normalize/registry_bridge.py
def register_normalize_table(
    ctx: SessionContext,
    *,
    name: str,
    column_defs: str,
    location: str,
    options_sql: str,
) -> None:
    ddl = f"""
    CREATE EXTERNAL TABLE {name} (
        {column_defs}
    )
    STORED AS DELTATABLE
    LOCATION '{location}'
    OPTIONS ({options_sql})
    """
    ctx.sql(ddl).collect()
```

**Target files**
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/ibis_engine/registry.py`
- `src/normalize/registry_runtime.py`
- `src/normalize/catalog.py`

**Implementation checklist**
- [x] Add normalize output DDL registration helper and wire into output materialization.
- [ ] Route all normalize dataset registrations through DDL (inputs still use non-DDL paths).
- [ ] Use DeltaTableFactory for all Delta-backed tables.
- [ ] Emit DDL provenance metadata into diagnostics artifacts.
- [ ] Remove non-DDL registration paths for normalize datasets.

---

## Scope 3: Delta-backed output writes (INSERT/COPY only, idempotent)

**Status:** in progress (Delta outputs wired; INSERT/COPY path pending)

**Objective:** write normalize outputs to Delta tables using DataFusion
INSERT/COPY (commit properties optional for normalize), with no fallback write paths.

**Representative code snippet**
```python
# src/normalize/output_writes.py
from deltalake import CommitProperties
from deltalake._internal import Transaction

from datafusion_engine.bridge import DeltaInsertOptions, datafusion_insert_from_dataframe


def write_normalize_output_delta(
    ctx: SessionContext,
    *,
    table_name: str,
    dataframe: DataFrame,
    run_id: str,
    commit_version: int,
) -> None:
    props = CommitProperties(
        app_transactions=[Transaction(app_id=run_id, version=commit_version)],
    )
    datafusion_insert_from_dataframe(
        ctx,
        dataframe=dataframe,
        table_name=table_name,
        options=DeltaInsertOptions(commit_properties=props),
    )
```
Note: normalize outputs do not require commit properties; keep commit reservation for
diagnostics, and only apply commit properties if the DataFusion INSERT path supports them.

**Target files**
- `src/normalize/output_writes.py` (new)
- `src/ibis_engine/io_bridge.py`
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/runtime.py`

**Implementation checklist**
- [x] Make Delta-backed writes the only normalize output path.
- [x] Thread run_id and commit version into normalize writes.
- [ ] Apply DataFusion write policy (partitioning, sort order, file sizing).
- [ ] Replace normalize output writes with DataFusion INSERT/COPY (no Ibis writer).
- [x] Remove fallback to `write_deltalake` or Parquet for normalize outputs.

---

## Scope 4: Nested span objects as canonical schema

**Status:** complete (span structs landed)

**Objective:** replace flat span columns with nested span structs and
span-related attribute maps across all normalize outputs.

**Representative code snippet**
```python
# src/normalize/ibis_spans.py
span = ibis.struct(
    bstart=bstart.cast("int64"),
    bend=bend.cast("int64"),
    ok=span_ok.cast("boolean"),
    line_base=line_base.cast("int32"),
    col_unit=col_unit.cast("string"),
    end_exclusive=end_exclusive.cast("boolean"),
)
return joined.mutate(span=span, span_id=span_id).drop(
    ["bstart", "bend", "span_ok", "line_base", "col_unit", "end_exclusive"],
)
```

**Target files**
- `src/normalize/ibis_spans.py`
- `src/normalize/dataset_fields.py`
- `src/normalize/dataset_rows.py`
- `src/datafusion_engine/schema_registry.py`
- `src/normalize/schemas.py`

**Implementation checklist**
- [x] Define canonical span struct types and replace flat span columns.
- [x] Update dataset specs and schemas to expose nested span objects.
- [ ] Add compatibility views for legacy consumers (nested-to-flat) if needed.
- [x] Update span pipelines to emit span structs and span_id consistently.

---

## Scope 5: Normalize plan builders stay in Ibis (no early materialization)

**Status:** in progress (some materialization remains)

**Objective:** keep plan builders purely in Ibis until execution, eliminating
`to_pyarrow` and any materialization during plan construction.

**Representative code snippet**
```python
# src/normalize/ibis_plan_builders.py
expr_rows = _expr_type_rows(exprs, ctx=ctx, type_node_columns=type_node_columns)
scip_rows = _scip_type_rows(scip, ctx=ctx, type_node_columns=type_node_columns)
combined = scip_rows.mutate(source_priority=ibis.literal(0)).union(
    expr_rows.mutate(source_priority=ibis.literal(1)),
    distinct=False,
)
preferred = combined.order_by("source_priority").distinct(on=["type_id"])
```

**Target files**
- `src/normalize/ibis_plan_builders.py`
- `src/normalize/ibis_spans.py`
- `src/normalize/ibis_api.py`
- `src/normalize/runner.py`

**Implementation checklist**
- [ ] Remove all `to_pyarrow` calls in normalize plan construction paths (e.g., type row preference).
- [ ] Replace materialized existence checks with plan-level preference logic.
- [ ] Ensure Ibis execution uses DataFusion streaming readers by default.

---

## Scope 6: SQLGlot compiler pipeline for normalize rules

**Status:** in progress (SQLGlot diagnostics + lineage recorded)

**Objective:** treat SQLGlot as the canonical IR for normalize plans, with
qualification, normalization, AST fingerprints, serde, and semantic diffs.

**Representative code snippet**
```python
# src/normalize/sqlglot_policy.py
expr = ibis_to_sqlglot(plan.expr, backend=backend, params=params)
normalized = normalize_expr(
    expr,
    options=NormalizeExprOptions(schema=schema_map, policy=policy),
)
fingerprint = plan_fingerprint(
    normalized,
    dialect=policy.write_dialect,
    policy_hash=policy_hash,
    schema_map_hash=schema_hash,
)
serde_payload = serialize_ast_artifact(normalized, policy=policy)
```

**Target files**
- `src/normalize/runner.py`
- `src/ibis_engine/compiler_checkpoint.py`
- `src/sqlglot_tools/optimizer.py`
- `src/sqlglot_tools/bridge.py`
- `src/sqlglot_tools/lineage.py`

**Implementation checklist**
- [ ] Normalize all rule plans through SQLGlot qualification + normalization.
- [x] Record AST fingerprints and serde payloads for every normalize plan.
- [x] Record SQLGlot lineage and diagnostics metadata per plan.
- [ ] Add semantic diff gating for incremental plan rebuilds.

---

## Scope 7: Lineage-driven projection pushdown

**Status:** planned

**Objective:** use SQLGlot lineage to compute required columns and push them
into DataFusion scan options for every normalize input.

**Representative code snippet**
```python
# src/normalize/scan_policy.py
required = required_columns_by_table(expr, backend=backend, policy=policy)
scan_options = DataFusionScanOptions(
    projection_exprs=tuple(required.get(table_name, ())),
    parquet_pruning=True,
)
```

**Target files**
- `src/normalize/runner.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/ibis_engine/lineage.py`
- `src/datafusion_engine/compile_options.py`

**Implementation checklist**
- [ ] Compute required columns for every normalize rule input.
- [ ] Push projection expressions into DataFusion scan options.
- [ ] Emit projection metadata as diagnostics artifacts.

---

## Scope 8: Dynamic builtin function catalog (information_schema driven)

**Status:** in progress (runtime catalog exists; normalize wiring pending)

**Objective:** replace static builtin function maps with a runtime catalog
derived from DataFusion information_schema and SHOW FUNCTIONS.

**Representative code snippet**
```python
# src/datafusion_engine/builtin_registry.py
catalog = FunctionCatalog.from_information_schema(
    routines=introspector.routines_snapshot(),
    parameters=introspector.parameters_snapshot(),
    parameters_available=True,
)
is_builtin = func_id.lower() in catalog.function_names
```

**Target files**
- `src/datafusion_engine/udf_catalog.py`
- `src/datafusion_engine/builtin_registry.py`
- `src/datafusion_engine/schema_introspection.py`
- `src/normalize/runtime_validation.py`

**Implementation checklist**
- [x] Build a cached function catalog from information_schema snapshots.
- [ ] Replace static builtin registries with catalog-backed lookups.
- [ ] Record function catalog snapshots in diagnostics.

---

## Scope 9: Delta advanced features (CDF, scan config, row tracking)

**Status:** planned

**Objective:** enable Delta change data feed, row tracking, and session-derived
DeltaScanConfig, and expose them as normalize execution primitives.

**Representative code snippet**
```python
# src/normalize/delta_cdf.py
provider = delta_cdf_table_provider(
    table_uri=table_uri,
    storage_options=storage_options,
    options=DeltaCdfOptions(starting_version=starting_version),
)
ctx.register_table("normalize_cdf", provider)
```

**Target files**
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/normalize/runtime.py`

**Implementation checklist**
- [ ] Register Delta CDF tables through the Rust-backed provider.
- [ ] Build DeltaScanConfig from session defaults before overrides.
- [ ] Enable row tracking and in-commit timestamps on normalize outputs.
- [ ] Capture CDF metadata and scan config in diagnostics artifacts.

---

## Scope 10: Diagnostics and plan artifacts for normalize runs

**Status:** in progress (SQLGlot artifacts captured)

**Objective:** persist plan diagnostics (EXPLAIN, Substrait bytes, SQLGlot AST)
and write policies for reproducible normalize runs.

**Representative code snippet**
```python
# src/normalize/diagnostics.py
explain = ctx.sql(f"EXPLAIN VERBOSE {sql}").to_pyarrow_table()
diagnostics.record_artifact(
    "normalize_plan_artifacts_v1",
    {
        "plan_hash": plan_hash,
        "sql": sql,
        "explain": explain.to_pylist(),
        "substrait": base64.b64encode(plan_bytes).decode("utf-8"),
    },
)
```

**Target files**
- `src/normalize/runner.py`
- `src/normalize/runtime.py`
- `src/datafusion_engine/runtime.py`
- `src/ibis_engine/compiler_checkpoint.py`

**Implementation checklist**
- [ ] Capture EXPLAIN and Substrait payloads per plan.
- [x] Capture SQLGlot serde payloads and SQL text per plan.
- [ ] Persist write policy metadata for every output write.
- [ ] Record DDL provenance and runtime configuration snapshots.

---

## Scope 11: Runtime tuning and metadata caches

**Status:** planned

**Objective:** enforce DataFusion runtime settings for normalize workloads,
including spill, partitioning, and metadata cache tuning.

**Representative code snippet**
```python
# src/datafusion_engine/runtime.py
config = SessionConfig().with_target_partitions(256).with_batch_size(8192)
ctx = SessionContext(config=config, runtime=runtime_env)
ctx.sql("SET datafusion.execution.parquet.metadata_cache.enable = true").collect()
```

**Target files**
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/cache_introspection.py`
- `src/normalize/runtime.py`

**Implementation checklist**
- [ ] Tune target_partitions, batch_size, and spill settings for normalize runs.
- [ ] Enable and introspect parquet/list-files metadata caches.
- [ ] Record runtime settings snapshots for every normalize run.

---

## Scope 12: Contract tests and end-to-end validation

**Status:** planned

**Objective:** validate schema contracts, pushdowns, and idempotent writes with
deterministic test cases and EXPLAIN assertions.

**Representative code snippet**
```python
# tests/unit/test_normalize_outputs.py
def test_normalize_delta_outputs_are_registered(ctx: SessionContext) -> None:
    tables = {row["table_name"] for row in ctx.sql("SHOW TABLES").collect()}
    assert "ast_nodes_norm_v1" in tables
```

**Target files**
- `tests/unit/test_normalize_outputs.py` (new)
- `tests/unit/test_normalize_spans.py` (new)
- `tests/unit/test_normalize_pushdowns.py` (new)

**Implementation checklist**
- [ ] Add schema contract tests for nested span objects.
- [ ] Add EXPLAIN-based tests for projection and predicate pushdowns.
- [ ] Add idempotent Delta write tests with repeatable run_id/version.

---

## Decommission and deletion map (explicit)

The items below are deletions (symbols removed from the codebase) or
decommissions (symbols removed from normalize call paths after replacements
land).

### Delete from codebase
- [x] `src/normalize/span_pipeline.py` (module) and its public symbols:
  `append_span_columns`, `append_alias_cols`, `span_error_table`.
- [x] `src/normalize/ibis_spans.py`: `add_ast_byte_spans_ibis`,
  `anchor_instructions_ibis`, `add_scip_occurrence_byte_spans_ibis`,
  `normalize_cst_callsites_spans_ibis`, `normalize_cst_imports_spans_ibis`,
  `normalize_cst_defs_spans_ibis`.
- [ ] `src/normalize/ibis_plan_builders.py`: `_prefer_type_rows` (still present).
- [ ] `src/ibis_engine/sources.py`: `IbisDeltaReadOptions`,
  `IbisDeltaWriteOptions`, `read_delta_ibis`, `write_delta_ibis`,
  `_commit_properties`, `_merge_storage_options`, `_delta_table_version`.

### Decommission from normalize paths (may remain elsewhere temporarily)
- [ ] `src/ibis_engine/io_bridge.py`: `write_ibis_dataset_delta`,
  `write_ibis_named_datasets_delta` (normalize outputs still use Ibis writer).
- [ ] `src/storage/deltalake/delta.py`: `write_table_delta` and
  `write_deltalake_idempotent` usage for normalize outputs.

---

## Global legacy decommission list
- [x] Remove normalize output paths that write Parquet or Arrow tables directly.
- [ ] Remove `to_pyarrow` checks during plan construction in normalize code.
- [ ] Remove any non-DDL table registration paths for normalize datasets.
- [x] Remove fallback write paths to `write_deltalake` for normalize outputs.
