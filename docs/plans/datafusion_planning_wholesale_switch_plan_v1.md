# DataFusion-First Planning And Scheduling — Wholesale Switch Plan (Design-Phase, Breaking Changes OK)

> **Goal**: Replace SQLGlot- and Ibis-based planning, lineage, and scheduling with a single, canonical **DataFusion-native planning architecture**.
>
> **Non-negotiable constraint**: There is **no parallel architecture**. Each scope item moves the codebase wholesale to the go-forward DataFusion-first design and removes legacy surfaces as part of the same scope.

---

## Go-forward architecture (target end state)

All planning, lineage, scheduling, and plan fingerprinting are derived from DataFusion itself:

1. **Authoring surface**: DataFusion `DataFrame` / `Expr` / `SessionContext`.
2. **Canonical planning surface**: `df.optimized_logical_plan()`.
3. **Canonical lineage surface**: structured traversal of DataFusion `LogicalPlan` variants.
4. **Execution hints surface**: `df.execution_plan()` plus `display_indent()` parsing for `DataSourceExec` signals.
5. **Canonical fingerprint surface**: Substrait bytes produced from the DataFusion plan.

In this target state, SQLGlot and Ibis are not part of the internal execution architecture.

---

## Scope 1 — Canonical DataFusion plan bundle (single planning entrypoint)

**Intent**: Introduce a single plan-bundle artifact that every execution and scheduling path uses, and remove the SQLGlot/Ibis compilation pipeline.

**Status (2026-01-27)**: Partial — `DataFusionPlanBundle` and `build_plan_bundle(...)` are implemented in `src/datafusion_engine/plan_bundle.py`, and `compile_to_bundle(...)` exists in `src/datafusion_engine/execution_facade.py`, but the SQLGlot/Ibis compilation pipeline still exists and is still used in many paths.

### Representative pattern

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from datafusion import SessionContext
from datafusion.dataframe import DataFrame
from datafusion.plan import ExecutionPlan, LogicalPlan
from datafusion.substrait import Producer


@dataclass(frozen=True)
class DataFusionPlanBundle:
    """Canonical plan artifact for all planning and scheduling."""

    df: DataFrame
    logical_plan: LogicalPlan
    optimized_logical_plan: LogicalPlan
    execution_plan: ExecutionPlan | None
    substrait_bytes: bytes | None
    plan_fingerprint: str
    plan_details: Mapping[str, object]


def build_plan_bundle(ctx: SessionContext, df: DataFrame) -> DataFusionPlanBundle:
    logical = df.logical_plan()
    optimized = df.optimized_logical_plan()
    execution = _safe_execution_plan(df)
    substrait_bytes = _to_substrait_bytes(ctx, optimized)
    fingerprint = _hash_plan(substrait_bytes=substrait_bytes, optimized=optimized)
    details = _plan_details(df, logical=logical, optimized=optimized, execution=execution)
    return DataFusionPlanBundle(
        df=df,
        logical_plan=logical,
        optimized_logical_plan=optimized,
        execution_plan=execution,
        substrait_bytes=substrait_bytes,
        plan_fingerprint=fingerprint,
        plan_details=details,
    )
```

### Target files to modify

- `src/datafusion_engine/execution_helpers.py`
- `src/datafusion_engine/execution_facade.py`
- `src/datafusion_engine/compile_options.py`
- `src/engine/session.py`
- `src/engine/runtime.py`

### Modules to delete

- `src/datafusion_engine/compile_pipeline.py`
- `src/datafusion_engine/sql_policy_engine.py`
- `src/datafusion_engine/sql_safety.py`
- `src/sqlglot_tools/bridge.py`

### Implementation checklist

- [x] Add `DataFusionPlanBundle` and a single `build_plan_bundle(...)` entrypoint.
- [ ] Replace facade compilation with plan-bundle construction from DataFusion objects.
- [ ] Remove SQLGlot policy and safety enforcement from internal paths.
- [ ] Delete the SQLGlot/Ibis compilation pipeline modules listed above.

---

## Scope 2 — DataFusion-native lineage extraction (logical-plan first)

**Intent**: Make DataFusion logical plans the sole source of dataset dependencies, required columns, predicate lineage, and join keys.

**Status (2026-01-27)**: Partial — `src/datafusion_engine/lineage_datafusion.py` exists and is wired into plan-bundle inference paths, but SQLGlot lineage helpers and fallbacks are still present in `relspec` and view registration surfaces.

### Representative pattern

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from datafusion.plan import LogicalPlan


@dataclass(frozen=True)
class ScanLineage:
    dataset_name: str
    projected_columns: tuple[str, ...]
    pushed_filters: tuple[str, ...]


@dataclass(frozen=True)
class LineageReport:
    scans: tuple[ScanLineage, ...] = ()
    joins: tuple[str, ...] = ()
    filters: tuple[str, ...] = ()
    required_columns_by_dataset: dict[str, tuple[str, ...]] = field(default_factory=dict)


def extract_lineage(plan: LogicalPlan) -> LineageReport:
    nodes = _logical_nodes(plan)
    required = _propagate_required_columns(nodes)
    scans = _extract_scans(nodes, required)
    joins = _extract_join_signals(nodes)
    filters = _extract_filter_signals(nodes)
    return LineageReport(
        scans=tuple(scans),
        joins=tuple(joins),
        filters=tuple(filters),
        required_columns_by_dataset=_required_by_dataset(scans),
    )
```

### Target files to modify

- `src/datafusion_engine/execution_helpers.py`
- `src/datafusion_engine/view_artifacts.py`
- `src/relspec/inferred_deps.py`
- `src/relspec/execution_plan.py`

### Modules to delete

- `src/sqlglot_tools/lineage.py`
- SQLGlot lineage helpers referenced from `relspec` and `view_graph` surfaces

### Implementation checklist

- [x] Add a `datafusion_engine/lineage_datafusion.py` module with logical-plan traversal.
- [x] Implement required-column propagation on DataFusion logical nodes.
- [ ] Replace SQLGlot lineage calls in `relspec` with DataFusion lineage extraction.
- [ ] Delete SQLGlot lineage helpers and their call sites.

---

## Scope 3 — Scheduling and inference rebuilt on DataFusion lineage

**Intent**: Rebuild dependency inference and readiness checks on DataFusion-derived lineage only.

**Status (2026-01-27)**: Partial — `infer_deps_from_plan_bundle(...)` exists in `src/relspec/inferred_deps.py` and is preferred when `plan_bundle` is present, but SQLGlot-based inference and readiness logic still remain as fallbacks.

### Representative pattern

```python
from __future__ import annotations

from datafusion_engine.lineage_datafusion import extract_lineage
from datafusion_engine.plans import build_plan_bundle
from relspec.inferred_deps import InferredDeps


def infer_deps_from_dataframe(ctx, df, *, task_name: str, output: str) -> InferredDeps:
    bundle = build_plan_bundle(ctx, df)
    lineage = extract_lineage(bundle.optimized_logical_plan)
    return InferredDeps(
        task_name=task_name,
        output=output,
        inputs=tuple(sorted(lineage.required_columns_by_dataset)),
        required_columns=lineage.required_columns_by_dataset,
        plan_fingerprint=bundle.plan_fingerprint,
        required_udfs=_required_udfs_from_plan(bundle.optimized_logical_plan),
    )
```

### Target files to modify

- `src/relspec/inferred_deps.py`
- `src/relspec/rustworkx_graph.py`
- `src/relspec/graph_edge_validation.py`
- `src/relspec/execution_plan.py`

### Modules to delete

- SQLGlot-specific dependency inference helpers
- SQLGlot AST fingerprinting dependencies inside `relspec`

### Implementation checklist

- [x] Add a plan-bundle inference path and make `infer_deps_from_view_nodes(...)` prefer it.
- [ ] Change `InferredDepsInputs` to accept DataFusion `DataFrame` or `LogicalPlan`.
- [ ] Remove SQLGlot-based table and column extraction.
- [ ] Re-key readiness checks to DataFusion-derived required columns and types.
- [ ] Delete SQLGlot inference paths.

---

## Scope 4 — View graph becomes DataFusion-native (no AST or Ibis fields)

**Intent**: Make view registration and graph compilation operate on DataFusion DataFrames and plan bundles only.

**Status (2026-01-27)**: Partial — `ViewNode.plan_bundle` exists and bundle-based artifacts are supported in `src/datafusion_engine/view_graph_registry.py` and `src/datafusion_engine/view_artifacts.py`, but `sqlglot_ast`/`ibis_expr` fields and legacy fallback paths remain.

### Representative pattern

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.plans import DataFusionPlanBundle, build_plan_bundle


@dataclass(frozen=True)
class ViewNode:
    name: str
    deps: tuple[str, ...]
    builder: Callable[[SessionContext], DataFrame]
    plan_bundle: DataFusionPlanBundle | None = None


def materialize_view_node(ctx: SessionContext, node: ViewNode) -> ViewNode:
    df = node.builder(ctx)
    bundle = build_plan_bundle(ctx, df)
    deps = _deps_from_plan(bundle.optimized_logical_plan)
    return ViewNode(name=node.name, deps=deps, builder=node.builder, plan_bundle=bundle)
```

### Target files to modify

- `src/datafusion_engine/view_graph_registry.py`
- `src/datafusion_engine/view_registry_specs.py`
- `src/datafusion_engine/view_artifacts.py`
- `src/datafusion_engine/view_registry.py`
- `src/normalize/view_builders.py`
- `src/cpg/view_builders.py`

### Modules to delete

- Ibis-backed view build paths and Ibis expression storage on `ViewNode`
- SQLGlot AST fields and SQLGlot-derived dependency extraction

### Implementation checklist

- [x] Add `plan_bundle` to `ViewNode` and prefer bundle-based deps/UDF extraction when present.
- [ ] Redefine `ViewNode` to remove `sqlglot_ast` and `ibis_expr` fields.
- [ ] Compute dependencies from DataFusion plan bundles.
- [ ] Build view artifacts from DataFusion plans and schemas only.
- [ ] Remove Ibis-backed view registration requirements.

---

## Scope 5 — Execution facade and runtime simplified to DataFusion-only

**Intent**: Collapse compilation and execution into a single DataFusion-native path.

**Status (2026-01-27)**: Partial — `compile_to_bundle(...)` and bundle-aware `ExecutionResult` are implemented in `src/datafusion_engine/execution_facade.py`, but the legacy compile pipeline, SQLGlot policy wiring, and Ibis coupling still remain in facade/runtime surfaces.

### Representative pattern

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.plans import DataFusionPlanBundle, build_plan_bundle


DataFrameBuilder = Callable[[SessionContext], DataFrame]


@dataclass(frozen=True)
class ExecutionResult:
    bundle: DataFusionPlanBundle


def execute_builder(ctx: SessionContext, builder: DataFrameBuilder) -> ExecutionResult:
    df = builder(ctx)
    bundle = build_plan_bundle(ctx, df)
    return ExecutionResult(bundle=bundle)
```

### Target files to modify

- `src/datafusion_engine/execution_facade.py`
- `src/datafusion_engine/execution_helpers.py`
- `src/engine/session.py`
- `src/engine/runtime.py`
- `src/engine/runtime_profile.py`

### Modules to delete

- `src/sqlglot_tools/optimizer.py`
- `src/sqlglot_tools/compat.py`
- SQLGlot policy and snapshot wiring in engine runtime profiles

### Implementation checklist

- [x] Add `compile_to_bundle(...)` and attach `DataFusionPlanBundle` to `ExecutionResult`.
- [ ] Replace compile/execute surfaces with DataFrame-builder execution.
- [ ] Remove SQLGlot policy hashes and snapshots from runtime profiles.
- [ ] Remove Ibis backend coupling from `EngineSession`.
- [ ] Delete SQLGlot optimizer and compat layers.

---

## Scope 6 — UDF platform and planner extensions become planning-critical

**Intent**: Treat Rust UDF registration, planner extensions, and TableProviders as first-class planning inputs.

**Status (2026-01-27)**: Partial — planner extensions are installed in `DataFusionExecutionFacade.__post_init__`, and plan-based UDF extraction utilities exist in `src/datafusion_engine/plan_udf_analysis.py`, but UDF enforcement still relies on legacy AST/Ibis paths in several call sites.

### Representative pattern

```python
from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.udf_platform import RustUdfPlatformOptions, install_rust_udf_platform


def configure_planning_surface(ctx: SessionContext) -> None:
    install_rust_udf_platform(
        ctx,
        options=RustUdfPlatformOptions(
            enable_udfs=True,
            enable_function_factory=True,
            enable_expr_planners=True,
            expr_planner_names=("codeanatomy_domain",),
            strict=True,
        ),
    )
```

### Target files to modify

- `src/datafusion_engine/udf_platform.py`
- `src/datafusion_engine/expr_planner.py`
- `src/datafusion_engine/function_factory.py`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/registry_bridge.py`

### Modules to delete

- Ibis UDF snapshot registration and SQLGlot-based required-UDF extraction

### Implementation checklist

- [x] Ensure UDF platform installation occurs before any plan-bundle construction.
- [x] Add plan-based UDF extraction utilities and optional bundle validation.
- [ ] Remove Ibis UDF snapshot registration.
- [ ] Remove SQLGlot UDF call extraction.

---

## Scope 7 — Schema contracts and introspection pinned to DataFusion catalogs

**Intent**: Eliminate SQLGlot and Ibis schema conversion lanes. Schema contracts come directly from DataFusion catalog and plan schemas.

**Status (2026-01-27)**: Partial — DataFusion catalog introspection is strong in `src/datafusion_engine/schema_introspection.py`, and SQLGlot/Ibis schema conversion helpers in `src/schema_spec/specs.py` are explicitly deprecated, but legacy schema conversion and DDL surfaces still exist.

### Representative pattern

```python
from __future__ import annotations

import pyarrow as pa
from datafusion import SessionContext


def schema_from_table(ctx: SessionContext, name: str) -> pa.Schema:
    df = ctx.table(name)
    schema = df.schema()
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    if isinstance(schema, pa.Schema):
        return schema
    raise TypeError("Unable to resolve DataFusion schema to Arrow schema.")
```

### Target files to modify

- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/schema_contracts.py`
- `src/schema_spec/system.py`
- `src/schema_spec/specs.py`

### Modules to delete

- Ibis schema conversion helpers
- SQLGlot DDL and schema emission helpers

### Implementation checklist

- [x] Deprecate SQLGlot/Ibis schema conversion helpers in `schema_spec/specs.py`.
- [ ] Remove SQLGlot schema mapping and DDL emission dependencies.
- [ ] Use DataFusion catalog and plan schemas as the schema source of truth.
- [ ] Delete Ibis schema conversion helpers.
- [ ] Delete SQLGlot DDL builders.

---

## Scope 8 — Plan artifacts and fingerprints become DataFusion-native

**Intent**: Plan artifacts, caching keys, and incremental fingerprints should be based on DataFusion plans and Substrait bytes only.

**Status (2026-01-27)**: Partial — bundle-based fingerprint helpers exist in `src/datafusion_engine/execution_helpers.py` and plan bundles carry Substrait bytes, but plan cache keys, artifact payloads, and incremental persistence still include SQLGlot-era fields and paths.

### Representative pattern

```python
from __future__ import annotations

import hashlib

from datafusion.plan import LogicalPlan


def plan_fingerprint(*, substrait_bytes: bytes | None, optimized: LogicalPlan) -> str:
    if substrait_bytes is not None:
        return hashlib.sha256(substrait_bytes).hexdigest()
    return hashlib.sha256(str(optimized.display_indent_schema()).encode("utf-8")).hexdigest()
```

### Target files to modify

- `src/datafusion_engine/execution_helpers.py`
- `src/engine/plan_cache.py`
- `src/incremental/plan_fingerprints.py`
- `src/datafusion_engine/runtime.py`

### Modules to delete

- SQLGlot AST artifact payloads
- SQLGlot policy hash dependencies in plan fingerprints

### Implementation checklist

- [x] Add bundle-based fingerprinting helpers (`plan_fingerprint_from_bundle`, `plan_bundle_cache_key`).
- [ ] Remove SQLGlot AST artifacts from plan artifact payloads.
- [ ] Key plan cache entries on DataFusion Substrait hashes only.
- [ ] Rewrite incremental plan fingerprints to use DataFusion plan bundles.
- [ ] Delete SQLGlot-driven fingerprint payloads.

---

## Scope 9 — Wholesale removal of SQLGlot and Ibis from internal architecture

**Intent**: Delete SQLGlot and Ibis surfaces from the core execution architecture once the DataFusion-first scopes above are implemented.

### Representative pattern

```python
from datafusion import SessionContext, col, lit
from datafusion.dataframe import DataFrame

# Old (remove):
# compiled = compile_sql_policy(sqlglot_ast, schema=schema, profile=profile)
# df = ibis_backend.raw_sql(compiled_ast)

# New (canonical):
def build_df(ctx: SessionContext) -> DataFrame:
    base = ctx.table("source")
    return base.filter(col("col_a") > lit(0)).select(col("col_a"), col("col_b"))
```

### Target files to modify

- `src/engine/session.py`
- `src/engine/runtime.py`
- `src/engine/runtime_profile.py`
- `src/datafusion_engine/execution_facade.py`
- `src/datafusion_engine/execution_helpers.py`
- `src/datafusion_engine/view_graph_registry.py`
- `src/datafusion_engine/view_registry_specs.py`
- `src/relspec/inferred_deps.py`

### Modules to delete

- `src/sqlglot_tools/`
- `src/ibis_engine/`
- SQLGlot and Ibis integration layers in `schema_spec`, `engine`, and `datafusion_engine`

### Implementation checklist

- [ ] Remove SQLGlot and Ibis from all internal planning and scheduling paths.
- [ ] Delete SQLGlot and Ibis integration packages.
- [ ] Eliminate SQLGlot policy snapshots and AST artifacts from diagnostics.
- [ ] Ensure no internal module depends on SQLGlot or Ibis types.

---

## Scope 10 — DataFusion Expr/DataFrame becomes the only authoring surface

**Intent**: Eliminate SQLGlot AST builders and Ibis plan builders as internal authoring surfaces. All calculation logic is expressed as DataFusion expressions and DataFrame pipelines.

**Status (2026-01-27)**: Partial — DataFusion-native builders now exist for relspec (`src/relspec/relationship_datafusion.py`), normalize (`src/normalize/df_view_builders.py`), and CPG (`src/cpg/view_builders_df.py`), but view registration still depends heavily on SQLGlot/Ibis in `src/datafusion_engine/view_registry_specs.py`.

### Representative pattern

```python
from __future__ import annotations

from datafusion import SessionContext, col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame


def build_relation_output(ctx: SessionContext) -> DataFrame:
    refs = ctx.table("cst_refs")
    return (
        refs.filter(col("ref_text").is_not_null() & (f.length(col("ref_text")) > lit(0)))
        .select(
            col("ref_id").alias("src_id"),
            col("ref_text").alias("dst_symbol"),
            lit("cst_ref_text").alias("resolution_method"),
            lit(0.5).alias("confidence"),
        )
    )
```

### Target files to modify

- `src/normalize/view_builders.py`
- `src/cpg/view_builders.py`
- `src/relspec/relationship_sql.py`
- `src/datafusion_engine/view_registry.py`
- `src/datafusion_engine/view_registry_specs.py`

### Modules to deprecate/delete

- SQLGlot AST builder modules and SQLGlot expression specs
- Ibis plan-builder surfaces used for internal transforms

### Implementation checklist

- [x] Implement DataFusion-native builders for relspec, normalize, and CPG outputs.
- [ ] Replace SQLGlot AST builders with DataFusion expression/DataFrame builders.
- [ ] Replace Ibis-based view builders with DataFusion-native builders.
- [ ] Remove SQLGlot expression specs from internal calculation logic.
- [ ] Ensure all view nodes can be built from `SessionContext` alone.

---

## Scope 11 — IO contracts move into DataFusion registration and DDL

**Intent**: Standardize IO contracts on DataFusion’s native registration and DDL/DML surfaces, especially for multi-file datasets, partitioned layouts, and object-store routing.

**Status (2026-01-27)**: Partial — IO registration is centralized in `src/datafusion_engine/io_adapter.py`, but `src/datafusion_engine/registry_bridge.py` still depends on Ibis registry surfaces and SQLGlot compilation for projection overrides and some registration paths.

### Representative pattern

```python
from __future__ import annotations

import pyarrow as pa
from datafusion import SessionContext
from datafusion.object_store import AmazonS3


def register_raw_events(ctx: SessionContext) -> None:
    s3 = AmazonS3(region="us-east-1")
    ctx.register_object_store("s3://", s3, None)
    ctx.register_listing_table(
        "raw_events",
        "s3://codeanatomy/raw/events/",
        file_extension=".parquet",
        table_partition_cols=[("repo", pa.string()), ("day", pa.string())],
        file_sort_order=[("repo", "ascending"), ("day", "ascending")],
    )
```

### Target files to modify

- `src/datafusion_engine/registry_bridge.py`
- `src/schema_spec/system.py`
- `src/schema_spec/specs.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/table_provider_metadata.py`

### Modules to deprecate/delete

- SQLGlot-driven DDL builders used to define IO contracts
- Ibis-based dataset registration lanes

### Implementation checklist

- [x] Centralize object-store and dataset registration in `DataFusionIOAdapter`.
- [ ] Standardize multi-file datasets on `register_listing_table(...)` or TableProviders.
- [ ] Standardize object-store routing on `register_object_store(...)`.
- [ ] Move IO contract knobs into DataFusion registration and DDL options.
- [ ] Remove SQLGlot/Ibis dataset registration helpers.

---

## Scope 12 — Catalog and information_schema become schema source of truth

**Intent**: Pin schema discovery, schema contracts, and schema validation to DataFusion’s catalog and `information_schema` surfaces rather than SQLGlot schema maps or Ibis schemas.

**Status (2026-01-27)**: Partial — `SchemaIntrospector` uses `information_schema` extensively in `src/datafusion_engine/schema_introspection.py`, but SQLGlot schema maps (`schema_map_for_sqlglot`) and Ibis/SQLGlot conversion lanes remain in contracts/specs surfaces.

### Representative pattern

```python
from __future__ import annotations

import pyarrow as pa
from datafusion import SessionContext


def schema_from_catalog(ctx: SessionContext, name: str) -> pa.Schema:
    df = ctx.table(name)
    schema = df.schema()
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = f"Unable to resolve Arrow schema for {name!r}."
    raise TypeError(msg)
```

### Target files to modify

- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/schema_contracts.py`
- `src/schema_spec/system.py`
- `src/schema_spec/specs.py`
- `src/relspec/evidence.py`

### Modules to deprecate/delete

- SQLGlot schema mapping and qualification surfaces
- Ibis schema conversion helpers

### Implementation checklist

- [x] Expand `SchemaIntrospector` to source metadata from `information_schema`.
- [ ] Remove SQLGlot schema maps from internal qualification and validation.
- [ ] Build contracts from DataFusion catalog and `information_schema`.
- [ ] Ensure all schema validation runs against DataFusion-resolved schemas.
- [ ] Delete Ibis and SQLGlot schema conversion helpers.

---

## Scope 13 — Planner extensions become core product features

**Intent**: Treat Rust UDFs, `ExprPlanner`, `FunctionRewrite`, `RelationPlanner`, and table functions as planning-critical extension planes rather than optional add-ons.

**Status (2026-01-27)**: Partial — planner extensions are installed eagerly in `DataFusionExecutionFacade.__post_init__`, but planner-extension outputs are not yet the sole source of required-UDF enforcement or view graph derivation.

### Representative pattern

```python
from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.udf_platform import RustUdfPlatformOptions, install_rust_udf_platform


def configure_domain_planning(ctx: SessionContext) -> None:
    install_rust_udf_platform(
        ctx,
        options=RustUdfPlatformOptions(
            enable_udfs=True,
            enable_function_factory=True,
            enable_expr_planners=True,
            expr_planner_names=("codeanatomy_domain",),
            strict=True,
        ),
    )
```

### Target files to modify

- `src/datafusion_engine/udf_platform.py`
- `src/datafusion_engine/expr_planner.py`
- `src/datafusion_engine/function_factory.py`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/domain_planner.py`
- `src/datafusion_engine/registry_bridge.py`

### Modules to deprecate/delete

- SQLGlot-based required-UDF extraction and enforcement
- Ibis UDF snapshot registration lanes

### Implementation checklist

- [x] Treat planner extensions as planning-critical via `DataFusionExecutionFacade.__post_init__`.
- [ ] Require planner extensions to be installed before plan-bundle construction.
- [ ] Derive required UDFs from DataFusion logical plans.
- [ ] Route domain syntax through `ExprPlanner` and `FunctionRewrite`.
- [ ] Remove SQLGlot and Ibis UDF enforcement paths.

---

## Scope 14 — Schema drift is handled at scan-time via schema adapters

**Intent**: Move schema evolution, normalization, and drift resolution into scan-time adapters and TableProvider boundaries, not downstream casts and ad hoc projections.

**Status (2026-01-27)**: Partial — schema evolution adapter factory wiring exists in `src/datafusion_engine/registry_bridge.py` and profile/runtime wiring, but drift handling is still mixed with legacy transform-based approaches.

### Representative pattern

```python
from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.registry_bridge import register_dataset_spec


def register_with_evolution(ctx: SessionContext, name: str, spec) -> None:
    register_dataset_spec(ctx, name=name, spec=spec)
    # Schema adapter factories are attached during registration/runtime wiring.
```

### Target files to modify

- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/table_provider_capsule.py`
- `src/datafusion_engine/table_provider_metadata.py`

### Modules to deprecate/delete

- Drift-handling logic embedded in Ibis transforms
- Schema-drift patches expressed as SQLGlot rewrites

### Implementation checklist

- [x] Wire schema-evolution adapter factories into registry/runtime wiring.
- [ ] Attach schema adapter factories during dataset registration.
- [ ] Ensure drift resolution happens at scan-time providers.
- [ ] Remove downstream drift normalization transforms when redundant.
- [ ] Eliminate SQLGlot/Ibis-based schema drift patches.

---

## Scope 15 — DataFusion-native writes become the canonical sink

**Intent**: Standardize all writing on DataFusion’s native write surfaces (`write_parquet_with_options`, `write_table`, and SQL `COPY`/`INSERT`) rather than Ibis write bridges.

**Status (2026-01-27)**: Partial — `WritePipeline` documents DataFusion-native write surfaces in `src/datafusion_engine/write_pipeline.py`, but it still accepts SQLGlot expressions and still uses Ibis bridges for Delta and other persistence paths.

### Representative pattern

```python
from __future__ import annotations

from datafusion import DataFrameWriteOptions, SessionContext
from datafusion.dataframe import DataFrame


def write_curated(df: DataFrame) -> None:
    write_options = DataFrameWriteOptions(
        partition_by=("repo", "day"),
        single_file_output=False,
        sort_by=None,
    )
    df.write_parquet("s3://codeanatomy/curated/events/", write_options=write_options)
```

### Target files to modify

- `src/datafusion_engine/write_pipeline.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/incremental/plan_fingerprints.py`
- `src/storage/deltalake/` integration points that rely on Ibis bridges

### Modules to deprecate/delete

- `src/ibis_engine/io_bridge.py`
- `src/ibis_engine/execution_factory.py`
- Ibis-backed Delta write paths

### Implementation checklist

- [x] Add a unified `WritePipeline` with DataFusion-native writer surfaces documented.
- [ ] Route all writes through DataFusion write APIs or provider inserts.
- [ ] Standardize write layout through `DataFrameWriteOptions`.
- [ ] Replace Ibis-based Delta writes with DataFusion-native sinks where supported.
- [ ] Delete Ibis write bridges and related execution factories.

---

## Scope 16 — SQL becomes ingress-only and is gated by SQLOptions

**Intent**: Remove SQLGlot policy and safety lanes from internal execution. If SQL is accepted at all, it is treated strictly as external ingress and gated by DataFusion `SQLOptions`.

**Status (2026-01-27)**: Partial — centralized SQLOptions helpers exist in `src/datafusion_engine/sql_options.py`, and SQL ingress gating is referenced in facade/runtime surfaces, but SQLGlot policy/safety wiring is still present and still used in multiple core paths.

### Representative pattern

```python
from __future__ import annotations

from datafusion import SQLOptions, SessionContext
from datafusion.dataframe import DataFrame


def ingress_sql(ctx: SessionContext, query: str) -> DataFrame:
    options = (
        SQLOptions()
        .with_allow_ddl(False)
        .with_allow_dml(False)
        .with_allow_statements(False)
    )
    return ctx.sql_with_options(query, options)
```

### Target files to modify

- `src/datafusion_engine/execution_facade.py`
- `src/datafusion_engine/execution_helpers.py`
- `src/engine/runtime.py`
- `src/engine/runtime_profile.py`
- `src/datafusion_engine/compile_options.py`

### Modules to deprecate/delete

- `src/datafusion_engine/sql_policy_engine.py`
- `src/datafusion_engine/sql_safety.py`
- SQLGlot policy, safety, and snapshot wiring

### Implementation checklist

- [x] Introduce centralized SQLOptions resolution helpers (`datafusion_engine/sql_options.py`).
- [ ] Remove SQLGlot policy and safety from internal paths.
- [ ] Gate any remaining SQL ingress with `SQLOptions`.
- [ ] Ensure internal execution is builder/plan-based only.
- [ ] Delete SQL policy and safety modules.

---

## Scope 17 — Deferred deletions that require all prior scopes to land

These modules should be deleted only after Scopes 1–16 are complete and no call sites remain.

### Modules to delete last

- `src/sqlglot_tools/` (entire package)
- `src/ibis_engine/` (entire package)
- `src/datafusion_engine/compile_pipeline.py`
- `src/datafusion_engine/sql_policy_engine.py`
- `src/datafusion_engine/sql_safety.py`
- `src/schema_spec/specs.py` SQLGlot and Ibis conversion surfaces
- `docs/plans/calculation_driven_scheduling_and_orchestration.md` references to SQLGlot/Ibis planning

### Deferred deletion checklist

- [ ] Confirm no imports reference SQLGlot or Ibis across `src/`.
- [ ] Confirm view graph compilation depends only on DataFusion plans.
- [ ] Confirm scheduler inference depends only on DataFusion lineage.
- [ ] Confirm plan artifacts and incremental fingerprints contain no SQLGlot payloads.
- [ ] Remove any remaining SQLGlot/Ibis docs and plan files that describe the deprecated architecture.

---

## Definition of done (architecture-level)

The wholesale switch is complete when all of the following are true:

- All planning, lineage, and scheduling derive from DataFusion `LogicalPlan` and `ExecutionPlan` surfaces.
- Plan fingerprinting is derived from Substrait bytes or DataFusion plan displays only.
- All internal authoring surfaces use DataFusion expressions and DataFrame pipelines.
- IO contracts and dataset registration are expressed via DataFusion registration, TableProviders, and DDL/DML.
- Schema discovery, contracts, and validation are sourced from DataFusion catalogs and `information_schema`.
- Planner extensions (UDFs, planners, rewrites, table functions) are installed before any plan construction.
- All canonical writes flow through DataFusion-native sinks and writer options.
- SQL is ingress-only and gated by `SQLOptions`, and no internal lane depends on SQLGlot or Ibis.
- SQLGlot and Ibis packages are absent from internal imports and internal architecture documentation.
