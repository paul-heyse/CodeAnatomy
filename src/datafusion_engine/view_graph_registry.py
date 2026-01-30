"""Dependency-aware view registration for view-driven pipelines."""

from __future__ import annotations

import tempfile
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.bundle_extraction import (
    arrow_schema_from_df,
    extract_lineage_from_bundle,
    resolve_required_udfs_from_bundle,
)
from datafusion_engine.diagnostics import record_artifact
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.schema_contracts import SchemaContract, ValidationViolation, ViolationType
from datafusion_engine.schema_introspection import SchemaIntrospector
from datafusion_engine.udf_runtime import (
    udf_names_from_snapshot,
    validate_required_udfs,
    validate_rust_udf_snapshot,
)
from datafusion_engine.view_artifacts import (
    ViewArtifactLineage,
    ViewArtifactRequest,
    build_view_artifact_from_bundle,
)
from serde_artifacts import ViewCacheArtifact, ViewCacheArtifactEnvelope
from serde_msgspec import convert, to_builtins
from utils.uuid_factory import uuid7_hex
from utils.validation import validate_required_items

if TYPE_CHECKING:
    from datafusion_engine.plan_bundle import DataFusionPlanBundle
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from datafusion_engine.scan_planner import ScanUnit


@dataclass(frozen=True)
class ViewNode:
    """Declarative view definition with explicit dependencies.

    Attributes
    ----------
    name : str
        View name for registration.
    deps : tuple[str, ...]
        Dependency names (table/view references).
    builder : Callable[[SessionContext], DataFrame]
        Function that builds the DataFrame for this view.
    contract_builder : Callable[[pa.Schema], SchemaContract] | None
        Optional schema contract builder.
    required_udfs : tuple[str, ...]
        Required UDF names for this view.
    plan_bundle : DataFusionPlanBundle | None
        DataFusion plan bundle (preferred source of truth for lineage).
    cache_policy : Literal["none", "memory", "delta_staging", "delta_output"]
        Cache policy for view materialization.
    """

    name: str
    deps: tuple[str, ...]
    builder: Callable[[SessionContext], DataFrame]
    contract_builder: Callable[[pa.Schema], SchemaContract] | None = None
    required_udfs: tuple[str, ...] = ()
    plan_bundle: DataFusionPlanBundle | None = None
    cache_policy: Literal["none", "memory", "delta_staging", "delta_output"] = "none"


class SchemaContractViolationError(ValueError):
    """Raised when a schema contract fails validation."""

    def __init__(
        self,
        *,
        table_name: str,
        violations: Sequence[ValidationViolation],
    ) -> None:
        self.table_name = table_name
        self.violations = tuple(violations)
        details = [
            f"{violation.violation_type.value}:{violation.column_name}"
            for violation in self.violations
        ]
        msg = f"Schema contract violations for {table_name!r}: {details}."
        super().__init__(msg)


@dataclass(frozen=True)
class ViewGraphOptions:
    """Configuration for view graph registration."""

    overwrite: bool = True
    temporary: bool = False
    validate_schema: bool = True


@dataclass(frozen=True)
class ViewGraphRuntimeOptions:
    """Runtime options for view graph registration."""

    runtime_profile: DataFusionRuntimeProfile | None = None
    require_artifacts: bool = False


@dataclass(frozen=True)
class ViewCacheContext:
    """Context for view cache materialization."""

    runtime: ViewGraphRuntimeOptions
    options: ViewGraphOptions


def register_view_graph(
    ctx: SessionContext,
    *,
    nodes: Sequence[ViewNode],
    snapshot: Mapping[str, object],
    runtime_options: ViewGraphRuntimeOptions | None = None,
    options: ViewGraphOptions | None = None,
) -> None:
    """Register a dependency-sorted view graph on a SessionContext.

    Raises
    ------
    ValueError
        Raised when artifact recording is required without a runtime profile.
    """
    resolved = options or ViewGraphOptions()
    runtime = runtime_options or ViewGraphRuntimeOptions()
    if runtime.require_artifacts and runtime.runtime_profile is None:
        msg = "Runtime profile is required for view artifact recording."
        raise ValueError(msg)
    validate_rust_udf_snapshot(snapshot)
    materialized = _materialize_nodes(nodes, snapshot=snapshot)
    ordered = _topo_sort_nodes(materialized)
    adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime.runtime_profile)
    runtime_hash: str | None = None
    if runtime.runtime_profile is not None:
        from datafusion_engine.runtime import session_runtime_hash

        runtime_hash = session_runtime_hash(runtime.runtime_profile.session_runtime())
    for node in ordered:
        _validate_deps(ctx, node, materialized)
        _validate_udf_calls(snapshot, node)
        validate_required_udfs(snapshot, required=node.required_udfs)
        _validate_required_functions(ctx, node.required_udfs)
        if runtime.runtime_profile is not None and node.plan_bundle is not None:
            scan_units = _plan_scan_units_for_bundle(
                ctx,
                bundle=node.plan_bundle,
                runtime_profile=runtime.runtime_profile,
            )
            if scan_units:
                from datafusion_engine.dataset_resolution import apply_scan_unit_overrides

                apply_scan_unit_overrides(
                    ctx,
                    scan_units=scan_units,
                    runtime_profile=runtime.runtime_profile,
                )
        df = node.builder(ctx)
        registered = _register_view_with_cache(
            ctx,
            adapter=adapter,
            node=node,
            df=df,
            cache=ViewCacheContext(runtime=runtime, options=resolved),
        )
        schema = arrow_schema_from_df(registered)
        if resolved.validate_schema and node.contract_builder is not None:
            contract = node.contract_builder(schema)
            _validate_schema_contract(ctx, contract, schema=schema)
        if runtime.runtime_profile is not None:
            from datafusion_engine.runtime import record_view_definition

            # Prefer plan_bundle-based artifact (DataFusion-native)
            if node.plan_bundle is None:
                msg = f"View {node.name!r} missing plan bundle for artifact recording."
                raise ValueError(msg)
            artifact = build_view_artifact_from_bundle(
                node.plan_bundle,
                request=ViewArtifactRequest(
                    name=node.name,
                    schema=schema,
                    lineage=ViewArtifactLineage(
                        required_udfs=node.required_udfs,
                        referenced_tables=node.deps,
                    ),
                    runtime_hash=runtime_hash,
                ),
            )
            record_view_definition(runtime.runtime_profile, artifact=artifact)


def _record_cache_artifact(
    cache: ViewCacheContext,
    *,
    node: ViewNode,
    cache_path: str | None,
    status: str,
    hit: bool | None = None,
) -> None:
    profile = cache.runtime.runtime_profile
    if profile is None:
        return
    artifact = ViewCacheArtifact(
        view_name=node.name,
        cache_policy=node.cache_policy,
        cache_path=cache_path,
        plan_fingerprint=node.plan_bundle.plan_fingerprint if node.plan_bundle else None,
        status=status,
        hit=hit,
    )
    envelope = ViewCacheArtifactEnvelope(payload=artifact)
    validated = convert(
        to_builtins(envelope, str_keys=True),
        target_type=ViewCacheArtifactEnvelope,
        strict=True,
    )
    payload = to_builtins(validated, str_keys=True)
    record_artifact(
        profile,
        "view_cache_artifacts_v1",
        cast("Mapping[str, object]", payload),
    )


def _register_view_with_cache(
    ctx: SessionContext,
    *,
    adapter: DataFusionIOAdapter,
    node: ViewNode,
    df: DataFrame,
    cache: ViewCacheContext,
) -> DataFrame:
    if node.cache_policy == "memory":
        cached = df.cache()
        adapter.register_view(
            node.name,
            cached,
            overwrite=cache.options.overwrite,
            temporary=cache.options.temporary,
        )
        _record_cache_artifact(cache, node=node, cache_path=None, status="cached", hit=None)
        return cached
    if node.cache_policy == "delta_staging":
        if cache.runtime.runtime_profile is None:
            msg = "Delta staging cache requires a runtime profile."
            raise ValueError(msg)
        staging_path = _delta_staging_path(node)
        from datafusion_engine.dataset_registration import register_dataset_df
        from datafusion_engine.dataset_registry import DatasetLocation
        from datafusion_engine.write_pipeline import (
            WriteFormat,
            WriteMode,
            WritePipeline,
            WriteRequest,
        )

        pipeline = WritePipeline(ctx, runtime_profile=cache.runtime.runtime_profile)
        plan_fingerprint = (
            node.plan_bundle.plan_fingerprint if node.plan_bundle is not None else None
        )
        pipeline.write(
            WriteRequest(
                source=df,
                destination=staging_path,
                format=WriteFormat.DELTA,
                mode=WriteMode.OVERWRITE,
                plan_fingerprint=plan_fingerprint,
            )
        )
        register_dataset_df(
            ctx,
            name=node.name,
            location=DatasetLocation(path=staging_path, format="delta"),
            runtime_profile=cache.runtime.runtime_profile,
        )
        _record_cache_artifact(
            cache,
            node=node,
            cache_path=staging_path,
            status="cached",
            hit=None,
        )
        return ctx.table(node.name)
    if node.cache_policy == "delta_output":
        if cache.runtime.runtime_profile is None:
            msg = "Delta output cache requires a runtime profile."
            raise ValueError(msg)
        location = cache.runtime.runtime_profile.dataset_location(node.name)
        if location is None:
            msg = f"Delta output cache missing dataset location for {node.name!r}."
            raise ValueError(msg)
        target_path = str(location.path)
        from datafusion_engine.dataset_registration import register_dataset_df
        from datafusion_engine.write_pipeline import (
            WriteFormat,
            WriteMode,
            WritePipeline,
            WriteRequest,
        )

        pipeline = WritePipeline(ctx, runtime_profile=cache.runtime.runtime_profile)
        plan_bundle = node.plan_bundle
        plan_fingerprint = plan_bundle.plan_fingerprint if plan_bundle is not None else None
        plan_identity_hash = plan_bundle.plan_identity_hash if plan_bundle is not None else None
        pipeline.write(
            WriteRequest(
                source=df,
                destination=target_path,
                format=WriteFormat.DELTA,
                mode=WriteMode.OVERWRITE,
                plan_fingerprint=plan_fingerprint,
                plan_identity_hash=plan_identity_hash,
            )
        )
        register_dataset_df(
            ctx,
            name=node.name,
            location=location,
            runtime_profile=cache.runtime.runtime_profile,
        )
        _record_cache_artifact(
            cache,
            node=node,
            cache_path=target_path,
            status="cached",
            hit=None,
        )
        return ctx.table(node.name)
    adapter.register_view(
        node.name,
        df,
        overwrite=cache.options.overwrite,
        temporary=cache.options.temporary,
    )
    return df


def _delta_staging_path(
    node: ViewNode,
) -> str:
    cache_root = Path(tempfile.gettempdir()) / "datafusion_view_cache"
    cache_root.mkdir(parents=True, exist_ok=True)
    fingerprint = node.plan_bundle.plan_fingerprint if node.plan_bundle is not None else uuid7_hex()
    safe_name = node.name.replace("/", "_").replace(":", "_")
    return str(cache_root / f"{safe_name}__{fingerprint}")


def _validate_deps(
    ctx: SessionContext,
    node: ViewNode,
    nodes: Sequence[ViewNode],
) -> None:
    known = {candidate.name for candidate in nodes}
    missing: list[str] = []
    for dep in node.deps:
        if dep in known:
            continue
        if not ctx.table_exist(dep):
            missing.append(dep)
    if missing:
        msg = f"Missing dependencies for view {node.name!r}: {sorted(missing)}."
        raise ValueError(msg)


def _validate_udf_calls(snapshot: Mapping[str, object], node: ViewNode) -> None:
    if node.plan_bundle is None:
        msg = f"View {node.name!r} missing plan bundle for UDF validation."
        raise ValueError(msg)
    required_udfs = node.plan_bundle.required_udfs
    if not required_udfs:
        lineage = extract_lineage_from_bundle(node.plan_bundle)
        required_udfs = lineage.required_udfs
    if not required_udfs:
        return
    available = {name.lower() for name in udf_names_from_snapshot(snapshot)}
    required_lower = [name.lower() for name in required_udfs]
    try:
        validate_required_items(
            required_lower,
            available,
            item_label=f"UDFs referenced by view {node.name!r}",
            error_type=ValueError,
        )
    except ValueError:
        missing = [name for name in required_udfs if name.lower() not in available]
        msg = f"View {node.name!r} references non-Rust UDFs: {sorted(missing)}."
        raise ValueError(msg) from None


def _materialize_nodes(
    nodes: Sequence[ViewNode],
    *,
    snapshot: Mapping[str, object],
) -> tuple[ViewNode, ...]:
    resolved: list[ViewNode] = []
    for node in nodes:
        if node.plan_bundle is None:
            msg = f"View {node.name!r} missing plan bundle for lineage extraction."
            raise ValueError(msg)
        deps = _deps_from_plan_bundle(node.plan_bundle)
        required = resolve_required_udfs_from_bundle(node.plan_bundle, snapshot=snapshot)
        resolved.append(replace(node, deps=deps, required_udfs=required))
    return tuple(resolved)


def _deps_from_plan_bundle(bundle: DataFusionPlanBundle) -> tuple[str, ...]:
    """Extract dependencies from DataFusion plan bundle (preferred path).

    Parameters
    ----------
    bundle : DataFusionPlanBundle
        Plan bundle with optimized logical plan.

    Returns
    -------
    tuple[str, ...]
        Dependency names inferred from the plan bundle.
    """
    lineage = extract_lineage_from_bundle(bundle)
    return lineage.referenced_tables


def _plan_scan_units_for_bundle(
    ctx: SessionContext,
    *,
    bundle: DataFusionPlanBundle,
    runtime_profile: DataFusionRuntimeProfile,
) -> tuple[ScanUnit, ...]:
    from datafusion_engine.scan_planner import plan_scan_unit

    lineage = extract_lineage_from_bundle(bundle)
    scan_units: dict[str, ScanUnit] = {}
    for scan in lineage.scans:
        location = runtime_profile.dataset_location(scan.dataset_name)
        if location is None:
            continue
        unit = plan_scan_unit(
            ctx,
            dataset_name=scan.dataset_name,
            location=location,
            lineage=scan,
        )
        scan_units[unit.key] = unit
    return tuple(sorted(scan_units.values(), key=lambda unit: unit.key))


def _validate_schema_contract(
    ctx: SessionContext,
    contract: SchemaContract,
    *,
    schema: pa.Schema | None = None,
) -> None:
    introspector = SchemaIntrospector(ctx)
    snapshot = introspector.snapshot
    if snapshot is None:
        msg = "Schema introspection snapshot unavailable for view validation."
        raise ValueError(msg)
    violations = contract.validate_against_introspection(snapshot)
    if schema is not None:
        violations.extend(_schema_metadata_violations(schema, contract))
    if violations:
        raise SchemaContractViolationError(
            table_name=contract.table_name,
            violations=violations,
        )


def _schema_metadata_violations(
    schema: pa.Schema,
    contract: SchemaContract,
) -> list[ValidationViolation]:
    expected = contract.schema_metadata or {}
    if not expected:
        return []
    from datafusion_engine.schema_contracts import SCHEMA_ABI_FINGERPRINT_META

    actual = schema.metadata or {}
    violations: list[ValidationViolation] = []
    expected_abi = expected.get(SCHEMA_ABI_FINGERPRINT_META)
    if expected_abi is not None:
        actual_abi = schema_identity_hash(schema).encode("utf-8")
        if actual_abi != expected_abi:
            violations.append(
                ValidationViolation(
                    violation_type=ViolationType.METADATA_MISMATCH,
                    table_name=contract.table_name,
                    column_name=_metadata_key_label(SCHEMA_ABI_FINGERPRINT_META),
                    expected=_format_metadata_value(expected_abi),
                    actual=_format_metadata_value(actual_abi),
                )
            )
    for key, expected_value in expected.items():
        if key == SCHEMA_ABI_FINGERPRINT_META:
            continue
        actual_value = actual.get(key)
        if actual_value is None:
            continue
        if actual_value == expected_value:
            continue
        violations.append(
            ValidationViolation(
                violation_type=ViolationType.METADATA_MISMATCH,
                table_name=contract.table_name,
                column_name=_metadata_key_label(key),
                expected=_format_metadata_value(expected_value),
                actual=_format_metadata_value(actual_value),
            )
        )
    return violations


def _metadata_key_label(key: bytes) -> str:
    try:
        return key.decode("utf-8")
    except UnicodeDecodeError:
        return key.hex()


def _format_metadata_value(value: bytes | None) -> str | None:
    if value is None:
        return None
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return value.hex()


def _validate_required_functions(ctx: SessionContext, required: Sequence[str]) -> None:
    if not required:
        return
    introspector = SchemaIntrospector(ctx)
    catalog = introspector.function_catalog_snapshot(include_parameters=False)
    available: set[str] = set()
    for row in catalog:
        name = row.get("function_name") or row.get("routine_name") or row.get("name")
        if isinstance(name, str):
            available.add(name.lower())
    required_lower = [name.lower() for name in required]
    try:
        validate_required_items(
            required_lower,
            available,
            item_label="information_schema functions",
            error_type=ValueError,
        )
    except ValueError:
        missing = [name for name in required if name.lower() not in available]
        msg = f"information_schema missing required functions: {sorted(missing)}."
        raise ValueError(msg) from None


def _schema_from_table(ctx: SessionContext, name: str) -> pa.Schema:
    try:
        schema = ctx.table(name).schema()
    except (KeyError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to resolve schema for {name!r}."
        raise ValueError(msg) from exc
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = f"Failed to resolve DataFusion schema for {name!r}."
    raise TypeError(msg)


def _topo_sort_nodes(nodes: Sequence[ViewNode]) -> tuple[ViewNode, ...]:
    node_map = {node.name: node for node in nodes}
    ordered = _topo_sort_nodes_rx(node_map, nodes)
    if ordered is not None:
        return ordered
    return _topo_sort_nodes_kahn(node_map, nodes)


def _topo_sort_nodes_rx(
    node_map: Mapping[str, ViewNode],
    nodes: Sequence[ViewNode],
) -> tuple[ViewNode, ...] | None:
    try:
        import rustworkx as rx
    except ImportError:
        return None
    graph = rx.PyDiGraph()
    index_by_name: dict[str, int] = {}
    for name in sorted(node_map):
        index_by_name[name] = graph.add_node(name)
    for node in nodes:
        dst_idx = index_by_name[node.name]
        for dep in node.deps:
            src_idx = index_by_name.get(dep)
            if src_idx is None:
                continue
            graph.add_edge(src_idx, dst_idx, None)
    ordered_names = rx.lexicographical_topological_sort(graph, key=lambda name: name)
    return tuple(node_map[name] for name in ordered_names)


def _topo_sort_nodes_kahn(
    node_map: Mapping[str, ViewNode],
    nodes: Sequence[ViewNode],
) -> tuple[ViewNode, ...]:
    indegree: dict[str, int] = dict.fromkeys(node_map, 0)
    adjacency: dict[str, set[str]] = {name: set() for name in node_map}
    for node in nodes:
        for dep in node.deps:
            if dep not in node_map:
                continue
            adjacency[dep].add(node.name)
            indegree[node.name] += 1
    queue = deque(sorted(name for name, degree in indegree.items() if degree == 0))
    ordered: list[ViewNode] = []
    while queue:
        name = queue.popleft()
        ordered.append(node_map[name])
        for neighbor in sorted(adjacency[name]):
            indegree[neighbor] -= 1
            if indegree[neighbor] == 0:
                queue.append(neighbor)
    if len(ordered) != len(node_map):
        remaining = sorted(name for name, degree in indegree.items() if degree > 0)
        msg = f"View dependency cycle detected among: {remaining}."
        raise ValueError(msg)
    return tuple(ordered)


__all__ = [
    "SchemaContractViolationError",
    "ViewGraphOptions",
    "ViewNode",
    "register_view_graph",
]
