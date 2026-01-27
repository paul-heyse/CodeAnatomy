# DataFusion + Delta Lake Best-in-Class Architecture — Wholesale Switch Implementation Plan (v1)

> Goal: Define the absolute best-in-class target architecture for programmatic operation definition, scheduling, and execution driven by view creation, using **DataFusion + Delta Lake only**.
>
> Constraint: This is a wholesale switch. There is no parallel architecture and no legacy fallback path in the go-forward design.

---

## Target end state (non-negotiable contracts)

1. **Single engine**: DataFusion is the only planner and executor.
2. **Single canonical artifact**: every view compiles to an enriched `DataFusionPlanBundle`.
3. **Delta-native physical boundary**: all persisted tables are Delta tables with pinned versions for deterministic runs.
4. **Plan-driven scheduling**: lineage, scan planning, and execution graphs are derived from DataFusion plans.
5. **Rust UDFs as planning primitives**: the UDF platform is installed before planning and participates in plan rewriting.
6. **Reproducibility as a product feature**: plans, settings, UDF versions, Delta versions, and scan units are persisted as queryable artifacts.

---

## Scope 1 — SessionRuntime as the authoritative planning boundary

**Intent**: Make session configuration and catalog registration deterministic, introspectable, and required for planning.

### Representative pattern

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion import SessionConfig, SessionContext

from datafusion_engine.domain_planner import domain_planner_names_from_snapshot
from datafusion_engine.udf_catalog import rewrite_tag_index
from datafusion_engine.udf_platform import RustUdfPlatformOptions, install_rust_udf_platform
from datafusion_engine.udf_runtime import rust_udf_snapshot_hash

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile


@dataclass(frozen=True)
class SessionRuntime:
    """Authoritative runtime surface for planning and execution."""

    ctx: SessionContext
    profile: DataFusionRuntimeProfile
    udf_snapshot_hash: str
    udf_rewrite_tags: tuple[str, ...]
    domain_planner_names: tuple[str, ...]


def build_session_runtime(profile: DataFusionRuntimeProfile) -> SessionRuntime:
    """Build a DataFusion session with deterministic settings."""
    cfg = (
        SessionConfig()
        .with_information_schema(True)
        .with_default_catalog_and_schema(profile.default_catalog, profile.default_schema)
        .with_target_partitions(profile.target_partitions)
    )
    ctx = SessionContext(cfg)
    platform = install_rust_udf_platform(
        ctx,
        options=RustUdfPlatformOptions(
            enable_udfs=True,
            enable_function_factory=True,
            enable_expr_planners=True,
            strict=True,
        ),
    )
    snapshot_hash = rust_udf_snapshot_hash(platform.snapshot or {})
    tag_index = rewrite_tag_index(platform.snapshot or {})
    rewrite_tags = tuple(sorted(tag_index))
    planner_names = domain_planner_names_from_snapshot(platform.snapshot)
    profile.object_store_registry.register_all(ctx)
    profile.catalog_bootstrap.register_all(ctx)
    return SessionRuntime(
        ctx=ctx,
        profile=profile,
        udf_snapshot_hash=snapshot_hash,
        udf_rewrite_tags=rewrite_tags,
        domain_planner_names=planner_names,
    )
```

### Target files to modify

- `src/datafusion_engine/runtime.py`
- `src/engine/session.py`
- `src/datafusion_engine/execution_facade.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/diagnostics.py`
- `src/datafusion_engine/udf_platform.py`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/domain_planner.py`
- `src/datafusion_engine/expr_planner.py`

### Code and modules to delete

- Any ad-hoc session construction entrypoints that bypass `DataFusionRuntimeProfile`.

### Implementation checklist

- [ ] Introduce `SessionRuntime` as a first-class object.
- [ ] Require `information_schema` to be enabled in all runtime profiles.
- [ ] Require Rust UDF platform installation before any planning or lineage extraction.
- [ ] Snapshot UDF identity (hash), rewrite tags, and enabled domain planners on the runtime.
- [ ] Centralize object store and catalog registration behind runtime bootstrap.
- [ ] Persist a session settings snapshot (`information_schema.df_settings`) per plan bundle.

---

## Scope 2 — Enriched plan bundle as the system-of-record

**Intent**: Expand `DataFusionPlanBundle` so it fully captures planning, reproducibility, and scheduling inputs.

### Representative pattern

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from datafusion import SessionContext
from datafusion.dataframe import DataFrame


@dataclass(frozen=True)
class PlanArtifacts:
    """Serializable planning artifacts used for reproducibility and scheduling."""

    logical_plan_display: str | None
    optimized_plan_display: str | None
    optimized_plan_graphviz: str | None
    optimized_plan_pgjson: str | None
    execution_plan_display: str | None
    df_settings: Mapping[str, str]
    udf_snapshot_hash: str
    function_registry_snapshot: Mapping[str, object]
    rewrite_tags: tuple[str, ...]


@dataclass(frozen=True)
class DeltaInputPin:
    """Pinned Delta version information for a scan."""

    dataset_name: str
    version: int | None
    timestamp: str | None


@dataclass(frozen=True)
class DataFusionPlanBundle:
    """Canonical plan artifact; the only supported planning contract."""

    df: DataFrame
    logical_plan: object
    optimized_logical_plan: object
    execution_plan: object | None
    substrait_bytes: bytes | None
    plan_fingerprint: str
    artifacts: PlanArtifacts
    delta_inputs: tuple[DeltaInputPin, ...] = ()
    required_udfs: tuple[str, ...] = ()
    required_rewrite_tags: tuple[str, ...] = ()
    plan_details: Mapping[str, object] = field(default_factory=dict)
```

### Target files to modify

- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/execution_helpers.py`
- `src/engine/plan_cache.py`
- `src/datafusion_engine/diagnostics.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/udf_catalog.py`

### Code and modules to delete

- Legacy plan fingerprint derivations that do not depend on Substrait and settings snapshots.

### Implementation checklist

- [ ] Add UDF snapshot hash, rewrite tags, and function registry snapshot fields to `PlanArtifacts`.
- [ ] Add `required_udfs` and `required_rewrite_tags` as first-class plan-bundle fields.
- [ ] Capture EXPLAIN artifacts including pgjson and graphviz where available.
- [ ] Snapshot session settings into the plan bundle.
- [ ] Include Delta version pins for all scans.
- [ ] Update cache keys to incorporate plan fingerprint, UDF snapshot hash, required UDFs/tags, settings hash, and Delta pins.

---

## Scope 3 — Structured logical-plan lineage (no string parsing)

**Intent**: Replace display-string parsing with structured traversal over DataFusion logical-plan variants and expressions.

### Representative pattern

```python
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from datafusion_engine.udf_catalog import rewrite_tag_index


@dataclass(frozen=True)
class ExprInfo:
    """Structured expression lineage extracted from a plan."""

    kind: str
    referenced_columns: tuple[tuple[str, str], ...]
    referenced_udfs: tuple[str, ...]


@dataclass(frozen=True)
class LineageReport:
    """Structured lineage report enriched with UDF requirements."""

    scans: tuple[ScanLineage, ...] = ()
    joins: tuple[JoinLineage, ...] = ()
    exprs: tuple[ExprInfo, ...] = ()
    required_udfs: tuple[str, ...] = ()
    required_rewrite_tags: tuple[str, ...] = ()
    required_columns_by_dataset: dict[str, tuple[str, ...]] = field(default_factory=dict)


def _variant_name(variant: Any) -> str:
    """Return the logical-plan variant tag name."""
    return type(variant).__name__


def extract_lineage(plan: object, *, udf_snapshot: dict[str, object]) -> LineageReport:
    """Traverse `plan.to_variant()` and `inputs()` for lineage and UDF requirements."""
    stack: list[object] = [plan]
    scans: list[ScanLineage] = []
    joins: list[JoinLineage] = []
    exprs: list[ExprInfo] = []
    referenced_udfs: set[str] = set()

    while stack:
        node = stack.pop()
        variant = getattr(node, "to_variant", lambda: None)()
        tag = _variant_name(variant) if variant is not None else type(node).__name__

        scans.extend(_extract_scan_lineage(tag=tag, variant=variant))
        joins.extend(_extract_join_lineage(tag=tag, variant=variant))
        expr_infos = _extract_expr_lineage(tag=tag, variant=variant)
        exprs.extend(expr_infos)
        for info in expr_infos:
            referenced_udfs.update(info.referenced_udfs)

        inputs: Iterable[object] = getattr(node, "inputs", lambda: ())()
        stack.extend(inputs)

    tag_index = rewrite_tag_index(udf_snapshot)
    required_tags = sorted(
        {
            tag
            for tag, names in tag_index.items()
            if any(name in referenced_udfs for name in names)
        }
    )
    return build_lineage_report(
        scans=scans,
        joins=joins,
        exprs=exprs,
        required_udfs=tuple(sorted(referenced_udfs)),
        required_rewrite_tags=tuple(required_tags),
    )
```

### Target files to modify

- `src/datafusion_engine/lineage_datafusion.py`
- `src/datafusion_engine/plan_udf_analysis.py`
- `src/datafusion_engine/view_registry_specs.py`
- `src/datafusion_engine/view_graph_registry.py`
- `src/relspec/inferred_deps.py`
- `src/relspec/execution_plan.py`
- `src/relspec/rustworkx_graph.py`
- `src/datafusion_engine/udf_catalog.py`
- `src/datafusion_engine/udf_runtime.py`

### Code and modules to delete

- Any lineage extraction helpers that depend on parsing `display_indent()` output.

### Implementation checklist

- [ ] Implement structured logical-plan traversal using `to_variant()` and `inputs()`.
- [ ] Extract scans, projections, predicates, join keys, and subqueries structurally.
- [ ] Extract UDF references from expression trees and emit `required_udfs` as lineage output.
- [ ] Derive required rewrite tags from the UDF registry snapshot and emit them in lineage.
- [ ] Propagate required columns per dataset with a structured dependency model.
- [ ] Validate that required UDFs are available before lineage is accepted as canonical.
- [ ] Make structured lineage the only supported lineage path.

---

## Scope 4 — Delta-aware ScanPlanner (file pruning before execution)

**Intent**: Introduce a scan-planning layer that uses Delta metadata and pruning policies to define deterministic scan units.

### Representative pattern

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from datafusion import SessionContext

from storage.deltalake.file_index import build_delta_file_index
from storage.deltalake.file_pruning import FilePruningPolicy, prune_file_index


@dataclass(frozen=True)
class ScanUnit:
    """A deterministic, pruned scan description for scheduling."""

    dataset_name: str
    delta_version: int | None
    candidate_files: tuple[Path, ...]
    pushed_filters: tuple[str, ...]
    projected_columns: tuple[str, ...]


def plan_scan_unit(ctx: SessionContext, *, dataset: DatasetLocation, lineage: ScanLineage) -> ScanUnit:
    """Use Delta log metadata to prune files before registration."""
    file_index = build_delta_file_index(dataset.delta_table())
    policy = FilePruningPolicy.from_lineage(lineage)
    pruned = prune_file_index(file_index=file_index, policy=policy)
    return ScanUnit(
        dataset_name=dataset.name,
        delta_version=dataset.delta_version,
        candidate_files=pruned.files,
        pushed_filters=lineage.pushed_filters,
        projected_columns=lineage.projected_columns,
    )
```

### Target files to modify

- `src/storage/deltalake/file_index.py`
- `src/storage/deltalake/file_pruning.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/dataset_registry.py`
- `src/relspec/execution_plan.py`
- `src/relspec/rustworkx_graph.py`

### Code and modules to delete

- Scan orchestration that relies on opaque table registration without pinned versions or pruning.

### Implementation checklist

- [ ] Introduce `scan_planner.py` with `ScanUnit` and planning entrypoints.
- [ ] Pin Delta versions for all scan units.
- [ ] Prune candidate files using Delta log metadata and declared lineage filters.
- [ ] Register restricted providers using `datafusion_ext` Delta provider surfaces when scan units are present.
- [ ] Integrate scan units into scheduling and execution planning.

---

## Scope 5 — Plan-driven task graph with scan units as first-class nodes

**Intent**: Make scheduling explicitly reflect scan work, plan structure, and pinned inputs.

### Representative pattern

```python
from __future__ import annotations

from dataclasses import dataclass

import rustworkx as rx


@dataclass(frozen=True)
class TaskNode:
    """A schedulable node derived from plans and scans."""

    key: str
    kind: str
    view_name: str | None = None
    dataset_name: str | None = None


def build_task_graph(views: list[ViewNode], scans: list[ScanUnit]) -> rx.PyDiGraph:
    """Create a single graph covering scans and view computation."""
    graph = rx.PyDiGraph()
    node_ids: dict[str, int] = {}

    for scan in scans:
        key = f"scan::{scan.dataset_name}::{scan.delta_version}"
        node_ids[key] = graph.add_node(TaskNode(key=key, kind="scan", dataset_name=scan.dataset_name))

    for view in views:
        key = f"view::{view.view_name}::{view.plan_bundle.plan_fingerprint}"
        node_ids[key] = graph.add_node(TaskNode(key=key, kind="view", view_name=view.view_name))

    _add_edges_from_lineage(graph=graph, node_ids=node_ids, views=views, scans=scans)
    return graph
```

### Target files to modify

- `src/relspec/rustworkx_graph.py`
- `src/relspec/execution_plan.py`
- `src/relspec/inferred_deps.py`
- `src/hamilton_pipeline/scheduling_hooks.py`
- `src/hamilton_pipeline/driver_factory.py`

### Code and modules to delete

- Task graph construction that does not model scan work or pinned inputs explicitly.

### Implementation checklist

- [ ] Extend task node models to represent scans and view computation distinctly.
- [ ] Incorporate Delta version pins into task keys and cache keys.
- [ ] Derive edges exclusively from structured plan lineage and scan units.
- [ ] Ensure scheduling surfaces consume `DataFusionPlanBundle` and `ScanUnit` only.

---

## Scope 6 — Execution from plan bundles (Substrait-first replay contract)

**Intent**: Execute the same plan that was scheduled by rehydrating from Substrait and pinned runtime settings.

### Representative pattern

```python
from __future__ import annotations

from datafusion import SessionContext
from datafusion.substrait import Consumer, Serde

from datafusion_engine.udf_runtime import rust_udf_snapshot, rust_udf_snapshot_hash, validate_required_udfs


def _ensure_udf_compatibility(ctx: SessionContext, bundle: DataFusionPlanBundle) -> None:
    """Fail fast if the execution UDF platform diverges from the planned bundle."""
    snapshot = rust_udf_snapshot(ctx)
    snapshot_hash = rust_udf_snapshot_hash(snapshot)
    if snapshot_hash != bundle.artifacts.udf_snapshot_hash:
        msg = (
            "UDF snapshot mismatch between planning and execution. "
            f"planned={bundle.artifacts.udf_snapshot_hash} execution={snapshot_hash}"
        )
        raise RuntimeError(msg)
    if bundle.required_udfs:
        validate_required_udfs(snapshot, required=bundle.required_udfs)


def execute_plan_bundle(ctx: SessionContext, bundle: DataFusionPlanBundle) -> ExecutionResult:
    """Execute a scheduled bundle with maximal determinism."""
    _ensure_udf_compatibility(ctx, bundle)
    if bundle.substrait_bytes:
        plan = Serde.deserialize_bytes(bundle.substrait_bytes)
        logical = Consumer.from_substrait_plan(ctx, plan)
        df = ctx.create_dataframe_from_logical_plan(logical)
    else:
        df = bundle.df
    return ExecutionResult.from_dataframe(df, plan_bundle=bundle)
```

### Target files to modify

- `src/datafusion_engine/execution_facade.py`
- `src/datafusion_engine/execution_helpers.py`
- `src/datafusion_engine/view_registry.py`
- `src/datafusion_engine/view_graph_registry.py`
- `src/hamilton_pipeline/driver_factory.py`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/plan_bundle.py`

### Code and modules to delete

- Execution entrypoints that bypass plan bundles or re-plan implicitly at execution time.

### Implementation checklist

- [ ] Add `execute_plan_bundle(...)` as the primary execution entrypoint.
- [ ] Rehydrate from Substrait when present to eliminate planner drift.
- [ ] Require plan bundles (not builders) across scheduling and execution boundaries.
- [ ] Validate UDF snapshot hash and required UDF coverage before execution.
- [ ] Record executed plan artifacts and runtime settings alongside execution results.

---

## Scope 7 — Delta-native write service and table lifecycle management

**Intent**: Make Delta the first-class write and table lifecycle contract, including features, idempotency, and maintenance.

### Representative pattern

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from datafusion import SessionContext
from deltalake import DeltaTable

from storage.deltalake.delta import enable_delta_features, idempotent_commit_properties


@dataclass(frozen=True)
class DeltaWriteSpec:
    """Declarative spec for Delta writes."""

    table_uri: str
    mode: str
    partition_by: tuple[str, ...]
    target_file_size: int | None
    table_properties: Mapping[str, str]


def prepare_delta_table(table: DeltaTable) -> None:
    """Enable table features required for incremental scheduling."""
    enable_delta_features(table, features=("changeDataFeed", "rowTracking", "deletionVectors"))


def write_delta(ctx: SessionContext, df: DataFrame, spec: DeltaWriteSpec) -> WriteResult:
    """Write via provider insert when possible, otherwise delta-rs writer."""
    commit_props = idempotent_commit_properties(operation="view_write", mode=spec.mode)
    return write_with_fallback(ctx=ctx, df=df, spec=spec, commit_properties=commit_props)
```

### Target files to modify

- `src/datafusion_engine/write_pipeline.py`
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/dataset_registry.py`
- `src/schema_spec/dataset_handle.py`
- `src/datafusion_engine/registry_bridge.py`

### Code and modules to delete

- Write paths that do not set deterministic commit properties or that bypass Delta as the physical table format.

### Implementation checklist

- [ ] Introduce a declarative `DeltaWriteSpec` that captures layout and lifecycle requirements.
- [ ] Enable Delta features required for incremental execution and change capture.
- [ ] Prefer provider insert for Delta when available; fall back to delta-rs writer explicitly.
- [ ] Standardize commit properties for idempotency and reproducibility.
- [ ] Persist write metadata (version, timestamp, commit props) into plan artifacts.

---

## Scope 8 — UDF platform as a required planning extension layer

**Intent**: Treat Rust UDFs, expression planners, and function rewrites as part of planning, not an optional runtime add-on.

### Representative pattern

```python
from __future__ import annotations

from dataclasses import dataclass

from datafusion import SessionContext

from datafusion_engine.domain_planner import domain_planner_names_from_snapshot
from datafusion_engine.udf_catalog import rewrite_tag_index
from datafusion_engine.udf_platform import RustUdfPlatformOptions, install_rust_udf_platform
from datafusion_engine.udf_runtime import rust_udf_snapshot_hash


@dataclass(frozen=True)
class UdfPlanningSnapshot:
    """Planning-time UDF identity for determinism and caching."""

    snapshot_hash: str
    rewrite_tags: tuple[str, ...]
    domain_planner_names: tuple[str, ...]


def install_udf_layer(ctx: SessionContext) -> UdfPlanningSnapshot:
    """Install UDFs and return the planning snapshot identity."""
    platform = install_rust_udf_platform(
        ctx,
        options=RustUdfPlatformOptions(
            enable_udfs=True,
            enable_function_factory=True,
            enable_expr_planners=True,
            strict=True,
        ),
    )
    snapshot_hash = rust_udf_snapshot_hash(platform.snapshot or {})
    tag_index = rewrite_tag_index(platform.snapshot or {})
    rewrite_tags = tuple(sorted(tag_index))
    planner_names = domain_planner_names_from_snapshot(platform.snapshot)
    return UdfPlanningSnapshot(
        snapshot_hash=snapshot_hash,
        rewrite_tags=rewrite_tags,
        domain_planner_names=planner_names,
    )
```

### Target files to modify

- `src/datafusion_engine/udf_platform.py`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/expr_planner.py`
- `src/datafusion_engine/function_factory.py`
- `src/datafusion_engine/domain_planner.py`
- `src/datafusion_engine/diagnostics.py`
- `src/datafusion_engine/udf_catalog.py`
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udaf_builtin.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `rust/datafusion_ext/src/lib.rs`

### Code and modules to delete

- UDF registration surfaces that are invoked after planning or that are optional in planning paths.

### Implementation checklist

- [ ] Make UDF installation a prerequisite of planning.
- [ ] Make UDF installation and snapshot validation a prerequisite of lineage extraction and scheduling.
- [ ] Embed UDF snapshot hash, rewrite tags, and enabled domain planners into plan bundles and cache keys.
- [ ] Ensure UDF docs and signatures are visible via `information_schema` surfaces.
- [ ] Ensure rewrite tags are populated for all UDF families and drive domain-planner enablement from the snapshot.
- [ ] Expand expression planning and rewrite hooks for domain-specific operators and canonical primitives.

---

## Scope 9 — Plan artifacts store (queryable, Delta-backed observability)

**Intent**: Persist planning, lineage, scan planning, and execution diagnostics as Delta tables for introspection and reproducibility.

### Representative pattern

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class PlanArtifactRow:
    """Serializable plan artifact persisted to Delta."""

    view_name: str
    plan_fingerprint: str
    udf_snapshot_hash: str
    function_registry_hash: str
    required_udfs_json: str
    required_rewrite_tags_json: str
    domain_planner_names_json: str
    delta_inputs_json: str
    df_settings_json: str
    optimized_plan_pgjson: str | None
    optimized_plan_graphviz: str | None
    lineage_json: str
    scan_units_json: str
    plan_details_json: str
    udf_compatibility_ok: bool
```

### Target files to modify

- `src/datafusion_engine/diagnostics.py`
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/view_registry.py`
- `src/datafusion_engine/execution_facade.py`
- `src/datafusion_engine/registry_loader.py`
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/udf_catalog.py`

### Code and modules to delete

- Ad-hoc or file-based artifact outputs that are not queryable via DataFusion.

### Implementation checklist

- [ ] Define Delta-backed artifact schemas for plan bundles, lineage, scan units, and execution stats.
- [ ] Persist artifacts as part of plan compilation and execution flows.
- [ ] Persist UDF snapshot hash, function registry hash, required UDFs, rewrite tags, and enabled planner names per plan.
- [ ] Make artifacts queryable via registered Delta tables in the session catalog.
- [ ] Use artifacts to validate determinism (plan, UDF snapshot, required UDFs/tags, settings, and Delta pins).

---

## Scope 10 — Catalog and provider registry as the single registration contract

**Intent**: Consolidate all dataset/view registration through a single provider registry model aligned with DataFusion’s Catalog → Schema → TableProvider contracts.

### Representative pattern

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from datafusion import SessionContext

from datafusion_engine.udf_runtime import rust_udf_snapshot, rust_udf_snapshot_hash
from datafusion_engine.registry_bridge import register_delta_table_provider


@dataclass(frozen=True)
class TableSpec:
    """Declarative table registration spec."""

    name: str
    kind: Literal["delta", "listing", "memory"]
    uri: str
    version: int | None


def register_udf_surfaces(ctx: SessionContext) -> str:
    """Ensure UDF registry surfaces are present and return the snapshot hash."""
    snapshot = rust_udf_snapshot(ctx)
    return rust_udf_snapshot_hash(snapshot)


def register_table_spec(ctx: SessionContext, spec: TableSpec) -> str:
    """Register the table using the correct provider surface."""
    udf_hash = register_udf_surfaces(ctx)
    if spec.kind == "delta":
        register_delta_table_provider(ctx=ctx, name=spec.name, uri=spec.uri, version=spec.version)
        return udf_hash
    register_listing_or_memory(ctx=ctx, spec=spec)
    return udf_hash
```

### Target files to modify

- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/dataset_registry.py`
- `src/datafusion_engine/registry_loader.py`
- `src/datafusion_engine/io_adapter.py`
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/udf_catalog.py`
- `src/datafusion_engine/udf_platform.py`

### Code and modules to delete

- Direct registration paths that bypass the provider registry and do not capture version pins.

### Implementation checklist

- [ ] Introduce a declarative table spec model for all registrations.
- [ ] Register Delta tables using provider-based registration only.
- [ ] Treat UDF registry surfaces (`udf_registry`, `udf_docs`, and `information_schema` parity) as part of the registration contract.
- [ ] Ensure registration invalidates and refreshes `information_schema` caches deterministically.
- [ ] Emit registration metadata, including the UDF snapshot hash, into the plan artifacts store.

---

## Scope 11 — Identifier and fingerprint primitives as first-class UDFs

**Intent**: Replace repeated `concat_ws + coalesce + stable_id/prefixed_hash64` scaffolding with canonical identifier primitives that enforce determinism and null semantics centrally.

### Representative pattern

```python
from __future__ import annotations

from dataclasses import dataclass

from datafusion import col
from datafusion.dataframe import DataFrame

from datafusion_ext import prefixed_hash_parts64, stable_id_parts


@dataclass(frozen=True)
class IdentifierSpec:
    """Declarative identifier construction contract."""

    prefix: str
    parts: tuple[str, ...]


def add_identifier_columns(df: DataFrame, *, spec: IdentifierSpec) -> DataFrame:
    """Apply canonical identifier UDFs without SQL scaffolding."""
    part_exprs = tuple(col(name) for name in spec.parts)
    return (
        df.with_column("stable_id", stable_id_parts(spec.prefix, *part_exprs))
        .with_column("hash64", prefixed_hash_parts64(spec.prefix, *part_exprs))
    )
```

### Target files to modify

- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udf_registry.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_ext.pyi`
- `src/datafusion_engine/expr_spec.py`
- `src/datafusion_engine/hashing.py`
- `src/datafusion_engine/view_registry.py`
- `src/normalize/df_view_builders.py`
- `src/relspec/relationship_datafusion.py`

### Code and modules to delete

- Manual identifier scaffolding that concatenates and null-coalesces parts before calling `stable_id` or `prefixed_hash64`.
- Redundant helper logic that exists only to prepare inputs for identifier hashing.

### Implementation checklist

- [ ] Add `stable_id_parts(...)`, `prefixed_hash_parts64(...)`, and `stable_hash_any(...)` as Rust scalar UDFs.
- [ ] Enforce null-sentinel, separator, and required-field semantics inside the UDF implementations.
- [ ] Expose Python wrappers and ExprIR mappings for the new primitives.
- [ ] Replace identifier scaffolding across view builders, normalize builders, and relationship builders.
- [ ] Tag these UDFs with rewrite tags like `id` and `hash` in the registry snapshot.

---

## Scope 12 — Span and position algebra as canonical UDFs

**Intent**: Make span construction, overlap checks, and span-derived identifiers a first-class, reusable algebra instead of repeated struct-building logic.

### Representative pattern

```python
from __future__ import annotations

from dataclasses import dataclass

from datafusion import col, lit
from datafusion.dataframe import DataFrame

from datafusion_ext import span_id, span_make, span_overlaps


@dataclass(frozen=True)
class SpanSpec:
    """Declarative span construction contract."""

    path_col: str = "path"
    bstart_col: str = "bstart"
    bend_col: str = "bend"


def add_span_columns(df: DataFrame, *, spec: SpanSpec) -> DataFrame:
    """Attach canonical span and span_id columns."""
    span = span_make(col(spec.bstart_col), col(spec.bend_col), col_unit=lit("byte"))
    span_key = span_id("span", col(spec.path_col), col(spec.bstart_col), col(spec.bend_col))
    return df.with_column("span", span).with_column("span_id", span_key)


def filter_span_overlaps(df: DataFrame) -> DataFrame:
    """Example span algebra predicate without SQL."""
    return df.filter(span_overlaps(col("span"), col("candidate_span")))
```

### Target files to modify

- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udf_registry.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_ext.pyi`
- `src/datafusion_engine/view_registry.py`
- `src/arrowdsl/core/ids.py`
- `src/normalize/df_view_builders.py`
- `src/datafusion_engine/schema_registry.py`

### Code and modules to delete

- Repeated span struct construction helpers that exist only to emulate a span algebra.
- Local span-overlap and span-length utilities that are implemented ad hoc.

### Implementation checklist

- [ ] Implement `span_make(...)`, `span_len(...)`, `span_overlaps(...)`, `span_contains(...)`, and `span_id(...)` as Rust scalar UDFs.
- [ ] Ensure span UDFs use `return_field_from_args` and also implement `return_type` for `information_schema` visibility.
- [ ] Replace manual span struct-building logic with the span UDFs in core view builders.
- [ ] Tag span UDFs with rewrite tags like `span` and `position_encoding`.

---

## Scope 13 — String and symbol normalization primitives

**Intent**: Centralize string normalization, null-if-blank semantics, and symbol/qname shaping in planner-visible UDFs rather than long chains of built-in functions.

### Representative pattern

```python
from __future__ import annotations

from datafusion import col, lit
from datafusion.dataframe import DataFrame

from datafusion_ext import qname_normalize, utf8_normalize, utf8_null_if_blank


def normalize_symbols(df: DataFrame) -> DataFrame:
    """Apply canonical symbol normalization without SQL."""
    normalized = utf8_normalize(col("symbol"), form=lit("NFKC"), casefold=lit(True))
    cleaned = utf8_null_if_blank(normalized)
    qname = qname_normalize(cleaned, module=col("module"), lang=lit("python"))
    return df.with_column("symbol_norm", cleaned).with_column("qname_norm", qname)
```

### Target files to modify

- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udf_registry.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_ext.pyi`
- `src/cpg/prop_transforms.py`
- `src/datafusion_engine/view_registry.py`
- `src/normalize/df_view_builders.py`
- `src/datafusion_engine/expr_spec.py`

### Code and modules to delete

- Long normalization chains that only exist to emulate consistent string normalization.
- Local helper functions that duplicate normalization policies across modules.

### Implementation checklist

- [ ] Implement `utf8_normalize(...)`, `utf8_null_if_blank(...)`, and `qname_normalize(...)` as Rust scalar UDFs.
- [ ] Provide literal-fast paths and cached-regex patterns where applicable.
- [ ] Expose ExprIR mappings so normalization can be declared in schema specs.
- [ ] Replace normalization chains in property transforms and view builders.
- [ ] Tag normalization UDFs with rewrite tags like `string_norm` and `symbol`.

---

## Scope 14 — Nested map/list/struct utilities for schema-driven operations

**Intent**: Make nested operations (map extraction, list cleanup, struct shaping) declarative and stable by introducing UDFs that encode the semantics you repeatedly implement across nested schemas.

### Representative pattern

```python
from __future__ import annotations

from datafusion import col, lit
from datafusion.dataframe import DataFrame

from datafusion_ext import list_compact, list_unique_sorted, map_get_default, struct_pick


def normalize_nested_attrs(df: DataFrame) -> DataFrame:
    """Apply canonical nested operations without SQL."""
    attrs = map_get_default(col("attrs"), lit("error_stage"), lit("unknown"))
    tags = list_unique_sorted(list_compact(col("tags")))
    minimal = struct_pick(col("payload"), lit("kind"), lit("span"), lit("symbol"))
    return df.with_column("error_stage", attrs).with_column("tags_norm", tags).with_column(
        "payload_min",
        minimal,
    )
```

### Target files to modify

- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udf_registry.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_ext.pyi`
- `src/datafusion_engine/view_registry.py`
- `src/datafusion_engine/schema_registry.py`
- `src/normalize/df_view_builders.py`
- `src/datafusion_engine/expr_spec.py`

### Code and modules to delete

- Nested helper scaffolding that manually emulates `map_get_default`, list compaction, and struct shaping.
- Repeated `map_extract + list_extract` patterns that should be modeled as canonical primitives.

### Implementation checklist

- [ ] Implement `map_get_default(...)`, `map_normalize(...)`, `list_compact(...)`, `list_unique_sorted(...)`, and `struct_pick(...)` as Rust scalar UDFs.
- [ ] Ensure nested UDFs preserve metadata and nullability via `return_field_from_args`.
- [ ] Expose these primitives in Python wrappers and ExprIR mappings.
- [ ] Replace nested scaffolding patterns in nested schema view registration.
- [ ] Tag nested UDFs with rewrite tags like `nested`, `map`, `list`, and `struct`.

---

## Scope 15 — Deterministic aggregates for canonicalization and reproducibility

**Intent**: Introduce deterministic aggregate primitives so canonicalization, dedupe, and evidence shaping do not rely on non-deterministic aggregate semantics.

### Representative pattern

```python
from __future__ import annotations

from datafusion import col
from datafusion.dataframe import DataFrame

from datafusion_ext import any_value_det, arg_max, collect_set


def canonicalize_group(df: DataFrame) -> DataFrame:
    """Use deterministic aggregates for canonical outputs."""
    return df.aggregate(
        group_exprs=[col("entity_id")],
        aggr_exprs=[
            collect_set(col("symbol")).alias("symbols"),
            arg_max(col("confidence"), col("updated_at")).alias("best_confidence"),
            any_value_det(col("path"), col("updated_at")).alias("canonical_path"),
        ],
    )
```

### Target files to modify

- `rust/datafusion_ext/src/udaf_builtin.rs`
- `rust/datafusion_ext/src/udf_registry.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_ext.pyi`
- `src/normalize/dataset_rows.py`
- `src/datafusion_engine/view_registry.py`
- `src/datafusion_engine/finalize.py`
- `src/datafusion_engine/expr_spec.py`

### Code and modules to delete

- Canonicalization logic that depends on non-deterministic aggregates or implicit engine behavior.
- Dedupe scaffolding that only exists to compensate for missing deterministic aggregates.

### Implementation checklist

- [ ] Implement `any_value_det(...)`, `arg_max(...)`, `arg_min(...)`, `collect_set(...)`, and `count_if(...)` as Rust aggregate UDFs.
- [ ] Ensure aggregates provide stable ordering guarantees (for example, explicit ordering keys).
- [ ] Expose aggregate wrappers and ExprIR mappings.
- [ ] Update dedupe and canonicalization specs to use deterministic aggregates.
- [ ] Tag aggregate UDFs with rewrite tags like `aggregate` and `deterministic`.

---

## Scope 16 — Incremental and Delta/CDF helper primitives

**Intent**: Simplify incremental scheduling and change-data handling by encoding change-type ranking and Delta/CDF semantics in planner-visible UDFs.

### Representative pattern

```python
from __future__ import annotations

from datafusion import col
from datafusion.dataframe import DataFrame

from datafusion_ext import cdf_change_rank, cdf_is_delete, cdf_is_upsert


def classify_cdf_changes(df: DataFrame) -> DataFrame:
    """Classify CDF change semantics without SQL."""
    return (
        df.with_column("change_rank", cdf_change_rank(col("_change_type")))
        .with_column("is_upsert", cdf_is_upsert(col("_change_type")))
        .with_column("is_delete", cdf_is_delete(col("_change_type")))
    )
```

### Target files to modify

- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udf_registry.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_ext.pyi`
- `src/incremental/changes.py`
- `src/incremental/deltas.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`

### Code and modules to delete

- Ad hoc change-type ranking and change classification logic duplicated across incremental modules.
- Stringly-typed change-type handling that should be a canonical primitive.

### Implementation checklist

- [ ] Implement `cdf_change_rank(...)`, `cdf_is_upsert(...)`, and `cdf_is_delete(...)` as Rust scalar UDFs.
- [ ] Ensure these UDFs encode change-type policies consistently across the engine.
- [ ] Replace ad hoc change-type logic in incremental helpers.
- [ ] Tag incremental UDFs with rewrite tags like `incremental`, `cdf`, and `delta`.

---

## Scope 17 — UDF platform upgrades: planner-aware semantics, docs, and tags

**Intent**: Upgrade the UDF platform itself so UDFs are planner-aware, introspectable via `information_schema`, and drive domain planners and rewrites through structured tags.

### Representative pattern

```rust
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature};

#[derive(Debug, PartialEq, Eq, Hash)]
struct StableIdPartsUdf {
    signature: Signature,
    doc: Documentation,
}

impl ScalarUDFImpl for StableIdPartsUdf {
    fn name(&self) -> &str {
        "stable_id_parts"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&self.doc)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<datafusion_expr::ColumnarValue> {
        let _ = args;
        Err(DataFusionError::NotImplemented("stable_id_parts is planned".into()))
    }
}
```

### Target files to modify

- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udaf_builtin.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `rust/datafusion_ext/src/function_rewrite.rs`
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/udf_catalog.py`
- `src/datafusion_engine/domain_planner.py`
- `src/datafusion_engine/function_factory.py`
- `src/datafusion_engine/expr_planner.py`
- `src/datafusion_engine/udf_platform.py`
- `src/datafusion_engine/diagnostics.py`

### Code and modules to delete

- UDF definitions that do not provide documentation metadata or rewrite tags.
- Planner integrations that rely on implicit UDF coverage rather than snapshot-driven validation.

### Implementation checklist

- [ ] Ensure dynamic or metadata-sensitive UDFs implement `return_field_from_args` and also implement `return_type` for `information_schema` parity.
- [ ] Implement `simplify(...)` hooks for high-level primitives when simplification can be schema-preserving.
- [ ] Extend rewrite tags beyond `hash` and `position_encoding` to include `id`, `span`, `string_norm`, `nested`, `aggregate`, and `incremental`.
- [ ] Wire rewrite tags into domain planner enablement and diagnostics.
- [ ] Expose UDF docs through `udf_docs()` and validate parity against `information_schema`.
- [ ] Expand ExprIR coverage so all new primitives are reachable without SQL.

---

## Scope 18 — Final decommissioning and deletions (only after all scopes above)

**Intent**: Remove residual legacy surfaces and transitional helpers only once plan-driven runtime, lineage, scan planning, and execution are fully in place.

### Code and modules to delete at the end

- `tools/sqlglot_rewrite_harness.py`
- Any remaining SQLGlot- or Ibis-named modules, shims, or documentation references.
- Any fallback lineage paths that parse plan display strings.
- Any execution paths that accept builders without immediately compiling to a plan bundle.

### Implementation checklist

- [ ] Run a repo-wide search for legacy names (`ibis`, `sqlglot`, `legacy`, `fallback`) and remove or rename.
- [ ] Delete fallback plan parsing and non-Substrait fingerprints once structured lineage is complete.
- [ ] Ensure all scheduling and execution flows require a plan bundle.
- [ ] Re-run all quality gates and update documentation to reflect the single-engine architecture.

---

## Recommended execution order (wholesale switch sequencing)

1. Scope 1 — SessionRuntime boundary.
2. Scope 8 — UDF planning layer.
3. Scope 2 — Enriched plan bundle.
4. Scope 3 — Structured lineage.
5. Scope 4 — ScanPlanner.
6. Scope 5 — Plan-driven task graph.
7. Scope 6 — Execution from plan bundles.
8. Scope 7 — Delta-native write service.
9. Scope 11 — Identifier and fingerprint primitives.
10. Scope 12 — Span and position algebra primitives.
11. Scope 13 — String and symbol normalization primitives.
12. Scope 14 — Nested map/list/struct utilities.
13. Scope 15 — Deterministic aggregates.
14. Scope 16 — Incremental and Delta/CDF helpers.
15. Scope 17 — UDF platform upgrades.
16. Scope 9 — Plan artifacts store.
17. Scope 10 — Catalog/provider registry consolidation.
18. Scope 18 — Final decommissioning and deletions.

This order minimizes planner drift: it locks down the session and UDF layers before making lineage and scheduling depend on enriched plan artifacts.
