# ArrowDSL Fallback Decommission Plan

## Purpose
Remove the ArrowDSL fallback engine (IR, plan compiler, runtime) and migrate all
call sites to Ibis/DataFusion as the single execution surface. No compatibility
shim remains; ArrowDSL plan execution is deleted outright.

## Objectives
- Delete `src/arrowdsl/ir`, `src/arrowdsl/compile`, and `src/arrowdsl/exec`.
- Replace `arrowdsl.plan.plan.Plan` usage with `ibis_engine.plan.IbisPlan`.
- Replace ArrowDSL plan/IR construction with Ibis expressions and DataFusion
  execution via `ibis_engine.runner` and `datafusion_engine.bridge`.
- Keep query/scan ergonomics by re-implementing them as Ibis-native helpers.
- Update tests and docs to reference DataFusion/Ibis only.
- Treat `SessionContext` as the single execution kernel, with a versioned runtime profile.
- Standardize streaming-first output surfaces and avoid eager materialization by default.
- Persist logical/physical/Substrait plan artifacts per run for reproducibility.
- Enforce SQLGlot AST policy boundaries and capability gating via `has_operation(...)`.
- Standardize persistent storage on Delta Lake tables over parquet directories.

## Non-goals
- No compatibility shim for ArrowDSL plans or fallback lanes.
- No new ArrowDSL APIs or incremental adapters for PlanIR.

## Progress update (current)
Completed:
- Ibis-native schema validation in `src/schema_spec/system.py`.
- Plan schema resolution via `plan.expr.schema().to_pyarrow()` in
  `src/normalize/runner.py` and `src/hamilton_pipeline/modules/outputs.py`.

In progress:
- Rule compiler post-kernel transforms expressed as Ibis plan transforms in
  `src/relspec/compiler.py`.
- Compile graph/schema helpers now use Ibis plan schema in
  `src/relspec/compiler_graph.py`.
- Rule graph union uses Ibis in `src/relspec/rules/graph.py`.
- Evidence planning removed PlanCatalog/ExecutionContext in
  `src/relspec/rules/evidence.py`.
- Incremental relspec updates materialize via Ibis in
  `src/incremental/relspec_update.py`.

Not started:
- All remaining scopes and checklist items not explicitly marked completed below.

## Universal practices (apply everywhere)
- **Single SessionContext**: all execution paths use the runtime-profiled context and
  `ibis.datafusion.connect(ctx)` to share UDFs, object stores, and registry state.
- **Streaming-first output**: default to `__arrow_c_stream__` or `to_pyarrow_batches`
  for large outputs; use `to_arrow_table()` only for small diagnostics.
- **Plan artifacts**: persist optimized logical plan, physical plan, and Substrait bytes.
- **IO policy knobs**: always pass explicit scan options (partition columns, pruning,
  file sort order, skip metadata) and use listing tables for directory sources.
- **SQLGlot AST boundary**: compile Ibis → SQLGlot, apply policy, then execute via
  SQLGlot AST (avoid raw SQL strings except for DDL).
- **Capability gating**: use `has_operation(...)` in validation to prevent unsupported
  ops from entering execution plans.
- **Delta-first storage**: persist durable datasets as Delta tables, keep `_delta_log`
  artifacts as first-class assets, and register `DeltaTable` providers (avoid Arrow
  Dataset fallbacks unless explicitly forced).

## Scope items

### Scope 1: SessionContext ownership + runtime profile
Goal: consolidate DataFusion configuration and context creation, and ensure Ibis
connects to the same `SessionContext`.

Representative code patterns:
```python
from datafusion import SessionContext

from datafusion_engine.runtime import DataFusionRuntimeProfile


def build_ctx(profile: DataFusionRuntimeProfile) -> SessionContext:
    return SessionContext(profile.session_config(), profile.runtime_env_builder())
```

Target files:
- `src/datafusion_engine/runtime.py`
- `src/ibis_engine/execution.py`
- `src/engine/runtime_profile.py`
- `tests/unit/test_sql_policy_matrix.py`
- `tests/unit/test_from_arrow_ingest.py`

Implementation checklist:
- [ ] Centralize `SessionContext` creation via runtime profiles.
- [ ] Ensure Ibis backend uses `ibis.datafusion.connect(ctx)` everywhere.
- [ ] Remove ad-hoc SessionContext instantiation outside runtime profile helpers.

Status (current): not started.

### Scope 2: Ibis-native scan and query surface
Goal: replace `Plan` and `PlanSource` with Ibis-backed scan/query helpers.

Representative code patterns:
```python
from ibis_engine.plan import IbisPlan
from ibis_engine.sources import SourceToIbisOptions, source_to_ibis


def query_to_ibis_plan(
    source: object,
    *,
    backend: BaseBackend,
    name: str | None,
    ordering: Ordering,
) -> IbisPlan:
    return source_to_ibis(
        source,
        options=SourceToIbisOptions(
            backend=backend,
            name=name,
            ordering=ordering,
        ),
    )
```

Target files:
- `src/arrowdsl/plan/scan_io.py`
- `src/arrowdsl/plan/query.py`
- `src/arrowdsl/plan/scan_builder.py`
- `src/arrowdsl/plan/source_normalize.py`
- `src/schema_spec/system.py`
- `src/normalize/runner.py`

Implementation checklist:
- [ ] Replace `PlanSource` with an Ibis plan source union.
- [ ] Route dataset/table sources through `ibis_engine.sources.source_to_ibis`.
- [ ] Update `QuerySpec.to_plan` to return `IbisPlan`.
- [ ] Remove any `PlanBuilder` or `PlanIR` references from scan/query paths.

Status (current): in progress (schema validation and incremental relspec update
paths now use Ibis scan helpers; core `arrowdsl.plan.*` surfaces still pending).

### Scope 3: ExprSpec/FilterSpec migration to Ibis expressions
Goal: eliminate ArrowDSL IR/macros by compiling expression specs directly to
Ibis expressions for DataFusion execution.

Representative code patterns:
```python
from ibis_engine.expr_compiler import expr_ir_to_ibis


def predicate_expr(
    expr_ir: ExprIR,
    table: ibis.Table,
) -> ibis.Value:
    return expr_ir_to_ibis(expr_ir, table)
```

Target files:
- `src/arrowdsl/compute/macros.py`
- `src/arrowdsl/compute/filters.py`
- `src/arrowdsl/spec/expr_ir.py`
- `src/arrowdsl/compute/expr_core.py`
- `src/arrowdsl/compile/expr_compiler.py`

Implementation checklist:
- [ ] Replace `ExprSpec.to_expression()` with Ibis expression compilation.
- [ ] Remove `ExprCompiler`, `ExprNode`, and `expr_from_expr_ir` usage.
- [ ] Update `FilterSpec.apply_plan` to operate on `IbisPlan`.
- [ ] Ensure ExprIR options route through `ibis_engine.expr_compiler`.

Status (current): not started.

### Scope 4: Replace Plan ops with Ibis equivalents
Goal: remove ArrowDSL `Plan` operators and re-express operations as Ibis transforms.

Representative code patterns:
```python
def apply_filter(plan: IbisPlan, predicate: ibis.Value) -> IbisPlan:
    return IbisPlan(expr=plan.expr.filter(predicate), ordering=plan.ordering)


def apply_project(plan: IbisPlan, names: list[str], exprs: list[ibis.Value]) -> IbisPlan:
    return IbisPlan(expr=plan.expr.select(exprs).rename(dict(zip(names, names))), ordering=plan.ordering)


def apply_union(left: IbisPlan, right: IbisPlan) -> IbisPlan:
    return IbisPlan(expr=left.expr.union(right.expr), ordering=Ordering.unordered())
```

Target files:
- `src/arrowdsl/plan/plan.py`
- `src/arrowdsl/plan/planner.py`
- `src/arrowdsl/plan/builder.py`
- `src/arrowdsl/plan/schema_utils.py`
- `src/arrowdsl/plan/joins.py`
- `src/arrowdsl/plan/runner.py`

Implementation checklist:
- [ ] Replace filter/project/order/aggregate/join/union/explode/winner_select with Ibis ops.
- [ ] Remove `PlanIR` construction and segmentation logic.
- [ ] Move any ordering metadata handling to `ibis_engine.plan` utilities.

Status (current): in progress (post-kernel transforms moved to Ibis in
`src/relspec/compiler.py`; union now Ibis in `src/relspec/rules/graph.py`).

### Scope 5: Remove ArrowDSL runtime and compiler
Goal: delete the fallback execution pipeline and Acero runtime logic.

Representative code patterns:
```python
from ibis_engine.runner import materialize_plan


def execute_ibis_plan(plan: IbisPlan, *, execution: IbisPlanExecutionOptions) -> TableLike:
    return materialize_plan(plan, execution=execution)
```

Target files:
- `src/arrowdsl/exec/runtime.py`
- `src/arrowdsl/compile/plan_compiler.py`
- `src/arrowdsl/compile/kernel_compiler.py`
- `src/arrowdsl/compile/expr_compiler.py`

Implementation checklist:
- [ ] Delete `PlanIR` compiler and runtime execution.
- [ ] Remove Acero-specific runtime policies and plan segmentation.
- [ ] Update callers to use `ibis_engine.runner` or `datafusion_engine.bridge`.

Status (current): not started.

### Scope 6: Streaming-first output contract
Goal: make batch streaming the default output surface.

Representative code patterns:
```python
def to_stream(df: DataFrame) -> object:
    return df.__arrow_c_stream__()


def to_batches(plan: IbisPlan) -> pa.RecordBatchReader:
    return plan.expr.to_pyarrow_batches()
```

Target files:
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/ibis_engine/io_bridge.py`
- `src/normalize/ibis_spans.py`
- `src/incremental/impact.py`

Implementation checklist:
- [ ] Replace large-path `.to_arrow_table()` calls with streaming where possible.
- [ ] Reserve eager materialization for diagnostics and tiny samples.
- [ ] Ensure downstream writers accept `RecordBatchReader` inputs.

Status (current): not started.

### Scope 7: Plan artifacts + Substrait snapshots
Goal: capture optimized logical plan, physical plan, and Substrait bytes per run.

Representative code patterns:
```python
from datafusion.substrait import Serde


def substrait_bytes(ctx: SessionContext, sql: str) -> bytes | None:
    return Serde.serialize_bytes(sql, ctx)
```

Target files:
- `src/datafusion_engine/bridge.py`
- `src/obs/repro.py`
- `src/obs/manifest.py`
- `tests/integration/test_substrait_cross_validation.py`

Implementation checklist:
- [ ] Add plan artifact capture in execution diagnostics.
- [ ] Persist Substrait bytes when SQL is available and tables are registered.
- [ ] Record policy hashes alongside plan artifacts.

Status (current): not started.

### Scope 8: Listing tables + object store policy
Goal: standardize listing-table registration and object store setup.

Representative code patterns:
```python
ctx.register_object_store("s3://", store, None)
ctx.register_listing_table(
    name,
    path,
    file_extension=".parquet",
    table_partition_cols=partition_cols,
)
```

Target files:
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/registry_loader.py`
- `src/schema_spec/system.py`
- `tests/integration/test_listing_cache_lifecycle.py`

Implementation checklist:
- [ ] Route all directory dataset scans through listing tables.
- [ ] Expose partition columns + pruning + file sort order via policy.
- [ ] Ensure object stores are registered before dataset registration.

Status (current): not started.

### Scope 9: SQLGlot AST boundary + policy enforcement
Goal: standardize SQLGlot AST use for compilation and policy rewrites.

Representative code patterns:
```python
sg_expr = compile_sqlglot_expr(expr, backend=backend, options=options)
table = execute_raw_sql(backend, sql=sg_expr.sql(dialect="datafusion"), sqlglot_expr=sg_expr)
```

Target files:
- `src/ibis_engine/runner.py`
- `src/ibis_engine/sql_bridge.py`
- `src/datafusion_engine/bridge.py`
- `src/relspec/rules/validation.py`

Implementation checklist:
- [ ] Always compile through SQLGlot before DataFusion execution.
- [ ] Apply SQLGlot policy rewrites consistently across rules and ad-hoc SQL.
- [ ] Emit SQLGlot AST artifacts for diagnostics.

Status (current): not started.

### Scope 10: Capability gating via `has_operation`
Goal: prevent unsupported ops from entering plans.

Representative code patterns:
```python
if not backend.has_operation(ops.WindowFunction):
    raise ValueError("Window functions are not supported by the backend.")
```

Target files:
- `src/ibis_engine/expr_compiler.py`
- `src/relspec/rules/validation.py`
- `src/relspec/rules/coverage.py`

Implementation checklist:
- [ ] Add capability checks for ops known to be backend-limited.
- [ ] Fail fast during rule validation rather than during execution.

Status (current): not started.

### Scope 11: Update schema/validation/evidence consumers
Goal: make downstream systems accept `IbisPlan` only.
Goal: make downstream systems accept `IbisPlan` only.

Representative code patterns:
```python
def plan_schema(plan: IbisPlan, *, ctx: ExecutionContext) -> pa.Schema:
    return plan.expr.schema().to_pyarrow()
```

Target files:
- `src/schema_spec/system.py`
- `src/arrowdsl/plan/schema_utils.py`
- `src/extract/evidence_plan.py`
- `src/obs/manifest.py`
- `src/relspec/rules/evidence.py`

Implementation checklist:
- [ ] Replace `Plan` types with `IbisPlan` in schema validation outputs.
- [ ] Remove ArrowDSL plan schema helpers or rewrite to Ibis.
- [ ] Update evidence plan compilation to Ibis-only surfaces.

Status (current): in progress (schema validation now Ibis; evidence plan partially
updated; ArrowDSL schema helpers still present).

### Scope 12: Remove ArrowDSL fallback artifacts and tests
Goal: delete unused modules/tests and update test coverage to DataFusion/Ibis.

Representative code patterns:
```python
def test_datafusion_plan_execution() -> None:
    plan = IbisPlan(expr=backend.table("input_table"))
    output = materialize_plan(plan, execution=execution)
    assert output is not None
```

Target files:
- `src/arrowdsl/ir/__init__.py`
- `src/arrowdsl/ir/expr.py`
- `src/arrowdsl/ir/plan.py`
- `tests/unit/test_acero_streaming.py`
- `tests/unit/test_scan_from_batches.py`
- `tests/unit/test_required_columns_scan.py`

Implementation checklist:
- [ ] Delete ArrowDSL IR and fallback plan modules.
- [ ] Remove Acero/Plan-specific tests and replace with Ibis/DataFusion equivalents.
- [ ] Ensure remaining tests use `IbisPlan` and DataFusion execution.

Status (current): not started.

### Scope 13: Docs and diagnostics cleanup
Goal: remove ArrowDSL fallback references in documentation and diagnostics.

Representative code patterns:
```python
diagnostics.record(
    {"execution_engine": "datafusion", "plan_type": "ibis"}
)
```

Target files:
- `docs/plans/fallback_coverage_report.md`
- `docs/plans/datafusion_displacement_mapping.md`
- `docs/python_library_reference/datafusion.md`
- `docs/python_library_reference/datafusion_addendum.md`

Implementation checklist:
- [ ] Remove ArrowDSL fallback references from planning docs.
- [ ] Update diagnostics payloads to indicate DataFusion/Ibis only.
- [ ] Regenerate coverage artifacts after removal.

Status (current): not started.

### Scope 14: Delta Lake storage standardization
Goal: ensure persistent tables are Delta-first, with DataFusion reading via Delta
TableProvider and Delta log artifacts treated as required table assets.

Representative code patterns:
```python
from deltalake import DeltaTable

delta_table = DeltaTable(path, storage_options=storage_options)
ctx.register_table(name, delta_table)
```

```python
from deltalake.writer import write_deltalake

write_deltalake(
    path,
    batches,
    mode="overwrite",
    storage_options=storage_options,
    commit_properties=commit_properties,
)
```

Target files:
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/registry_loader.py`
- `src/ibis_engine/io_bridge.py`
- `src/storage/deltalake/delta.py`
- `src/incremental/diff.py`
- `src/schema_spec/system.py`

Implementation checklist:
- [ ] Register Delta tables via `DeltaTable` or table provider; avoid `register_dataset`
      for Delta paths unless explicitly forced by policy.
- [ ] Treat `_delta_log` artifacts as part of the table contract (commit JSON, checkpoints,
      `_last_checkpoint`, sidecars, optional `.crc`), and ensure these are preserved alongside
      data files.
- [ ] Preserve `_change_data/` and deletion vector files when CDF/DV features are enabled.
- [ ] Prefer DataFusion Delta CDF table provider when available; avoid Arrow fallback CDF.
- [ ] Record Delta snapshot/version metadata in diagnostics for reproducibility.

Status (current): not started.

## Universal practice audit targets
SessionContext ownership:
- `src/datafusion_engine/runtime.py:1181`
- `src/relspec/rules/fallback_coverage.py:364`
- `tests/unit/test_sql_policy_matrix.py`

Streaming-first outputs:
- `src/datafusion_engine/bridge.py:438`
- `src/datafusion_engine/runtime.py:1516`
- `src/datafusion_engine/kernels.py:261`
- `src/normalize/ibis_spans.py:144`
- `src/incremental/impact.py:82`

Listing table / object store policy:
- `src/datafusion_engine/registry_bridge.py:433`
- `src/datafusion_engine/registry_bridge.py:1123`
- `src/datafusion_engine/runtime.py:1194`

SQLGlot AST boundary:
- `src/ibis_engine/runner.py:49`
- `src/ibis_engine/sql_bridge.py:207`
- `src/datafusion_engine/bridge.py:467`

Capability gating:
- `src/ibis_engine/expr_compiler.py`
- `src/relspec/rules/validation.py`
- `src/relspec/rules/coverage.py`

Delta-first storage:
- `src/datafusion_engine/registry_bridge.py:607`
- `src/datafusion_engine/registry_bridge.py:976`
- `src/ibis_engine/io_bridge.py:470`
- `src/storage/deltalake/delta.py:708`

## Legacy code to decommission and delete
- `src/arrowdsl/ir/*`
- `src/arrowdsl/compile/*`
- `src/arrowdsl/exec/*`
- `src/arrowdsl/plan/plan.py`
- `src/arrowdsl/plan/planner.py`
- `src/arrowdsl/plan/builder.py`
- `src/arrowdsl/plan/runner.py`
- `src/arrowdsl/plan/runner_types.py`

## Acceptance checklist
- [ ] All ArrowDSL fallback modules removed from the codebase.
- [ ] No remaining imports of `arrowdsl.ir`, `arrowdsl.compile`, or `arrowdsl.exec`.
- [ ] All plan/query paths return `IbisPlan`.
- [ ] DataFusion execution passes via `ibis_engine.runner` or `datafusion_engine.bridge`.
- [ ] Streaming outputs are the default for non-trivial results.
- [ ] Plan artifacts (logical/physical/Substrait) are persisted per run.
- [ ] Ruff, Pyrefly, and Pyright clean.
