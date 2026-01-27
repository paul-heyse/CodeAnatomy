"""Dependency-aware view registration for view-driven pipelines."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.schema_contracts import (
    SchemaContract,
    SchemaViolation,
    SchemaViolationType,
)
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

if TYPE_CHECKING:
    from datafusion_engine.lineage_datafusion import LineageReport
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
    """

    name: str
    deps: tuple[str, ...]
    builder: Callable[[SessionContext], DataFrame]
    contract_builder: Callable[[pa.Schema], SchemaContract] | None = None
    required_udfs: tuple[str, ...] = ()
    plan_bundle: DataFusionPlanBundle | None = None


class SchemaContractViolationError(ValueError):
    """Raised when a schema contract fails validation."""

    def __init__(
        self,
        *,
        table_name: str,
        violations: Sequence[SchemaViolation],
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
                from datafusion_engine.scan_overrides import apply_scan_unit_overrides

                apply_scan_unit_overrides(
                    ctx,
                    scan_units=scan_units,
                    runtime_profile=runtime.runtime_profile,
                )
        df = node.builder(ctx)
        if resolved.temporary:
            adapter.register_view(
                node.name,
                df,
                overwrite=resolved.overwrite,
                temporary=resolved.temporary,
            )
        else:
            adapter.register_view(
                node.name,
                df,
                overwrite=resolved.overwrite,
                temporary=False,
            )
        schema = _schema_from_df(df)
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
        lineage = _lineage_from_bundle(node.plan_bundle)
        required_udfs = lineage.required_udfs
    if not required_udfs:
        return
    available = {name.lower() for name in udf_names_from_snapshot(snapshot)}
    missing = [name for name in required_udfs if name.lower() not in available]
    if missing:
        msg = f"View {node.name!r} references non-Rust UDFs: {sorted(missing)}."
        raise ValueError(msg)


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
        required = _required_udfs_from_plan_bundle(node.plan_bundle, snapshot=snapshot)
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
    lineage = _lineage_from_bundle(bundle)
    return lineage.referenced_tables


def _plan_scan_units_for_bundle(
    ctx: SessionContext,
    *,
    bundle: DataFusionPlanBundle,
    runtime_profile: DataFusionRuntimeProfile,
) -> tuple[ScanUnit, ...]:
    from datafusion_engine.scan_planner import plan_scan_unit

    lineage = _lineage_from_bundle(bundle)
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


def _required_udfs_from_plan_bundle(
    bundle: DataFusionPlanBundle,
    *,
    snapshot: Mapping[str, object],
) -> tuple[str, ...]:
    """Extract required UDFs from DataFusion plan bundle (preferred path).

    Parameters
    ----------
    bundle : DataFusionPlanBundle
        Plan bundle with optimized logical plan.
    snapshot : Mapping[str, object]
        Rust UDF snapshot.

    Returns
    -------
    tuple[str, ...]
        Required UDF names.
    """
    required_udfs = bundle.required_udfs
    if not required_udfs:
        lineage = _lineage_from_bundle(bundle)
        required_udfs = lineage.required_udfs
    if not required_udfs:
        return ()
    snapshot_names = udf_names_from_snapshot(snapshot)
    lookup = {name.lower(): name for name in snapshot_names}
    required = {
        lookup[name.lower()]
        for name in required_udfs
        if isinstance(name, str) and name.lower() in lookup
    }
    return tuple(sorted(required))


def _lineage_from_bundle(bundle: DataFusionPlanBundle) -> LineageReport:
    if bundle.optimized_logical_plan is None:
        msg = "DataFusion plan bundle missing optimized logical plan."
        raise ValueError(msg)
    from datafusion_engine.lineage_datafusion import extract_lineage

    snapshot = bundle.artifacts.udf_snapshot
    return extract_lineage(bundle.optimized_logical_plan, udf_snapshot=snapshot)


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
) -> list[SchemaViolation]:
    expected = contract.schema_metadata or {}
    if not expected:
        return []
    from arrowdsl.schema.abi import schema_fingerprint
    from datafusion_engine.schema_contracts import SCHEMA_ABI_FINGERPRINT_META

    actual = schema.metadata or {}
    violations: list[SchemaViolation] = []
    expected_abi = expected.get(SCHEMA_ABI_FINGERPRINT_META)
    if expected_abi is not None:
        actual_abi = schema_fingerprint(schema).encode("utf-8")
        if actual_abi != expected_abi:
            violations.append(
                SchemaViolation(
                    violation_type=SchemaViolationType.METADATA_MISMATCH,
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
            SchemaViolation(
                violation_type=SchemaViolationType.METADATA_MISMATCH,
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
    missing = [name for name in required if name.lower() not in available]
    if missing:
        msg = f"information_schema missing required functions: {sorted(missing)}."
        raise ValueError(msg)


def _schema_from_df(df: DataFrame) -> pa.Schema:
    schema = df.schema()
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Failed to resolve DataFusion schema."
    raise TypeError(msg)


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
