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
    ViewArtifactInputs,
    build_view_artifact,
    build_view_artifact_from_bundle,
)
from ibis_engine.sources import SourceToIbisOptions, register_ibis_view

if TYPE_CHECKING:
    from ibis.expr.types import Table

    from datafusion_engine.plan_bundle import DataFusionPlanBundle
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from datafusion_engine.sql_policy_engine import SQLPolicyProfile
    from sqlglot_tools.compat import Expression


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
    sqlglot_ast : Expression | None
        DEPRECATED: SQLGlot AST for the view. Use plan_bundle for lineage extraction.
    ibis_expr : Table | None
        DEPRECATED: Ibis expression for the view. Use plan_bundle for lineage extraction.
    plan_bundle : DataFusionPlanBundle | None
        DataFusion plan bundle (preferred source of truth for lineage).
    """

    name: str
    deps: tuple[str, ...]
    builder: Callable[[SessionContext], DataFrame]
    contract_builder: Callable[[pa.Schema], SchemaContract] | None = None
    required_udfs: tuple[str, ...] = ()
    sqlglot_ast: Expression | None = None  # DEPRECATED: Use plan_bundle
    ibis_expr: Table | None = None  # DEPRECATED: Use plan_bundle
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
    policy_profile: SQLPolicyProfile | None = None
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
    ibis_backend = None
    if any(node.ibis_expr is not None for node in ordered):
        import ibis

        ibis_backend = ibis.datafusion.connect(ctx)
        from ibis_engine.builtin_udfs import register_ibis_udf_snapshot

        register_ibis_udf_snapshot(snapshot)
    from datafusion_engine.sql_policy_engine import SQLPolicyProfile

    policy_profile = runtime.policy_profile or SQLPolicyProfile()
    for node in ordered:
        _validate_deps(ctx, node, materialized)
        _validate_udf_calls(snapshot, node)
        validate_required_udfs(snapshot, required=node.required_udfs)
        _validate_required_functions(ctx, node.required_udfs)
        df = node.builder(ctx)
        if resolved.temporary:
            adapter.register_view(
                node.name,
                df,
                overwrite=resolved.overwrite,
                temporary=resolved.temporary,
            )
        # DataFusion-native path (preferred)
        elif node.ibis_expr is not None and ibis_backend is not None:
            # Legacy Ibis path for backward compatibility
            register_ibis_view(
                node.ibis_expr,
                options=SourceToIbisOptions(
                    backend=ibis_backend,
                    name=node.name,
                    ordering=None,
                    overwrite=resolved.overwrite,
                    runtime_profile=runtime.runtime_profile,
                ),
            )
        else:
            # DataFusion-native persistent registration (preferred)
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
            if node.plan_bundle is not None:
                artifact = build_view_artifact_from_bundle(
                    node.plan_bundle,
                    name=node.name,
                    schema=schema,
                    required_udfs=node.required_udfs,
                    referenced_tables=node.deps,
                )
            elif node.sqlglot_ast is not None:
                # Fall back to legacy AST-based artifact
                artifact = build_view_artifact(
                    ViewArtifactInputs(
                        ctx=ctx,
                        name=node.name,
                        ast=node.sqlglot_ast,
                        schema=schema,
                        required_udfs=node.required_udfs,
                        policy_profile=policy_profile,
                    )
                )
            else:
                # No artifact can be built - skip recording
                continue
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
    # Prefer plan_bundle for UDF extraction when available (DataFusion-native)
    if node.plan_bundle is not None:
        udfs_from_bundle = _required_udfs_from_plan_bundle(node.plan_bundle, snapshot=snapshot)
        if udfs_from_bundle:
            # All UDFs from bundle are pre-validated against snapshot in _required_udfs_from_plan_bundle
            return
    elif node.sqlglot_ast is not None:
        # Fall back to legacy AST-based validation
        from sqlglot_tools.lineage import referenced_udf_calls

        udf_calls = referenced_udf_calls(node.sqlglot_ast)
        if not udf_calls:
            return
        available = {name.lower() for name in udf_names_from_snapshot(snapshot)}
        missing = [name for name in udf_calls if name.lower() not in available]
        if missing:
            msg = f"View {node.name!r} references non-Rust UDFs: {sorted(missing)}."
            raise ValueError(msg)
    # If neither plan_bundle nor sqlglot_ast is available, skip validation


def _materialize_nodes(
    nodes: Sequence[ViewNode],
    *,
    snapshot: Mapping[str, object],
) -> tuple[ViewNode, ...]:
    resolved: list[ViewNode] = []
    for node in nodes:
        # Prefer plan_bundle for dependency/UDF extraction when available
        deps_from_bundle = _deps_from_plan_bundle(node.plan_bundle)
        udfs_from_bundle = _required_udfs_from_plan_bundle(node.plan_bundle, snapshot=snapshot)

        if deps_from_bundle is not None and udfs_from_bundle is not None:
            # Use DataFusion-native lineage (preferred path)
            deps = deps_from_bundle
            required = udfs_from_bundle
        elif node.sqlglot_ast is not None:
            # Fall back to SQLGlot AST (legacy path)
            deps = _deps_from_ast(node.sqlglot_ast)
            required = _required_udfs_from_ast(node.sqlglot_ast, snapshot=snapshot)
        else:
            msg = f"View {node.name!r} missing both plan_bundle and SQLGlot AST."
            raise ValueError(msg)
        resolved.append(replace(node, deps=deps, required_udfs=required))
    return tuple(resolved)


def _deps_from_ast(expr: Expression) -> tuple[str, ...]:
    """Extract dependencies from SQLGlot AST (legacy path).

    Returns
    -------
    tuple[str, ...]
        Referenced table names.
    """
    from sqlglot_tools.lineage import referenced_tables

    return tuple(referenced_tables(expr))


def _deps_from_plan_bundle(bundle: DataFusionPlanBundle | None) -> tuple[str, ...] | None:
    """Extract dependencies from DataFusion plan bundle (preferred path).

    Parameters
    ----------
    bundle : DataFusionPlanBundle | None
        Plan bundle with optimized logical plan.

    Returns
    -------
    tuple[str, ...] | None
        Dependency names, or None if bundle unavailable.
    """
    if bundle is None:
        return None
    if bundle.optimized_logical_plan is None:
        return None
    try:
        from datafusion_engine.lineage_datafusion import extract_lineage

        lineage = extract_lineage(bundle.optimized_logical_plan)
    except (RuntimeError, TypeError, ValueError):
        return None
    else:
        return lineage.referenced_tables


def _required_udfs_from_ast(
    expr: Expression,
    *,
    snapshot: Mapping[str, object],
) -> tuple[str, ...]:
    """Extract required UDFs from SQLGlot AST (legacy path).

    Returns
    -------
    tuple[str, ...]
        Required UDF names.
    """
    from sqlglot_tools.lineage import referenced_udf_calls

    udf_calls = referenced_udf_calls(expr)
    if not udf_calls:
        return ()
    snapshot_names = udf_names_from_snapshot(snapshot)
    lookup = {name.lower(): name for name in snapshot_names}
    required = {
        lookup[name.lower()]
        for name in udf_calls
        if isinstance(name, str) and name.lower() in lookup
    }
    return tuple(sorted(required))


def _required_udfs_from_plan_bundle(
    bundle: DataFusionPlanBundle | None,
    *,
    snapshot: Mapping[str, object],
) -> tuple[str, ...] | None:
    """Extract required UDFs from DataFusion plan bundle (preferred path).

    Parameters
    ----------
    bundle : DataFusionPlanBundle | None
        Plan bundle with optimized logical plan.
    snapshot : Mapping[str, object]
        Rust UDF snapshot.

    Returns
    -------
    tuple[str, ...] | None
        Required UDF names, or None if bundle unavailable.
    """
    if bundle is None:
        return None
    if bundle.optimized_logical_plan is None:
        return None

    # Prefer DataFusion-native lineage extraction with UDF support
    try:
        from datafusion_engine.lineage_datafusion import extract_lineage

        lineage = extract_lineage(bundle.optimized_logical_plan)
        if lineage.referenced_udfs:
            # Filter to only UDFs that exist in snapshot
            snapshot_names = udf_names_from_snapshot(snapshot)
            lookup = {name.lower(): name for name in snapshot_names}
            required = {
                lookup[name.lower()]
                for name in lineage.referenced_udfs
                if isinstance(name, str) and name.lower() in lookup
            }
            return tuple(sorted(required))
    except (RuntimeError, TypeError, ValueError):
        pass

    # Fallback: Parse function calls from the plan display (legacy path)
    plan_display = bundle.display_optimized_plan()
    if not plan_display:
        return None
    snapshot_names = udf_names_from_snapshot(snapshot)
    lookup = {name.lower(): name for name in snapshot_names}
    # Look for function calls in the plan display
    import re

    # Pattern for function calls in DataFusion plan display
    pattern = r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\("
    matches = re.findall(pattern, plan_display)
    required = {
        lookup[name.lower()] for name in matches if isinstance(name, str) and name.lower() in lookup
    }
    return tuple(sorted(required))


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
