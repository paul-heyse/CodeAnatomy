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
from datafusion_engine.schema_contracts import SchemaContract, SchemaViolation
from datafusion_engine.schema_introspection import SchemaIntrospector
from datafusion_engine.udf_runtime import (
    udf_names_from_snapshot,
    validate_required_udfs,
    validate_rust_udf_snapshot,
)

if TYPE_CHECKING:
    from sqlglot_tools.compat import Expression


@dataclass(frozen=True)
class ViewNode:
    """Declarative view definition with explicit dependencies."""

    name: str
    deps: tuple[str, ...]
    builder: Callable[[SessionContext], DataFrame]
    contract_builder: Callable[[pa.Schema], SchemaContract] | None = None
    required_udfs: tuple[str, ...] = ()
    sqlglot_ast: Expression | None = None


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


def register_view_graph(
    ctx: SessionContext,
    *,
    nodes: Sequence[ViewNode],
    snapshot: Mapping[str, object],
    options: ViewGraphOptions | None = None,
) -> None:
    """Register a dependency-sorted view graph on a SessionContext."""
    resolved = options or ViewGraphOptions()
    validate_rust_udf_snapshot(snapshot)
    materialized = _materialize_nodes(nodes, snapshot=snapshot)
    ordered = _topo_sort_nodes(materialized)
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    for node in ordered:
        _validate_deps(ctx, node, materialized)
        _validate_udf_calls(snapshot, node)
        required = _required_udfs(node, snapshot=snapshot)
        validate_required_udfs(snapshot, required=required)
        _validate_required_functions(ctx, required)
        df = node.builder(ctx)
        adapter.register_view(
            node.name,
            df,
            overwrite=resolved.overwrite,
            temporary=resolved.temporary,
        )
        if resolved.validate_schema and node.contract_builder is not None:
            contract = node.contract_builder(_schema_from_df(df))
            _validate_schema_contract(ctx, contract)


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
    if node.sqlglot_ast is None:
        return
    from sqlglot_tools.lineage import referenced_udf_calls

    udf_calls = referenced_udf_calls(node.sqlglot_ast)
    if not udf_calls:
        return
    available = {name.lower() for name in udf_names_from_snapshot(snapshot)}
    missing = [name for name in udf_calls if name.lower() not in available]
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
        deps = node.deps
        required = node.required_udfs
        if node.sqlglot_ast is not None:
            deps = _deps_from_ast(node.sqlglot_ast)
            required = _required_udfs_from_ast(node.sqlglot_ast, snapshot=snapshot)
        if deps is node.deps and required is node.required_udfs:
            resolved.append(node)
        else:
            resolved.append(replace(node, deps=deps, required_udfs=required))
    return tuple(resolved)


def _deps_from_ast(expr: Expression) -> tuple[str, ...]:
    from sqlglot_tools.lineage import referenced_tables

    return tuple(referenced_tables(expr))


def _required_udfs_from_ast(
    expr: Expression,
    *,
    snapshot: Mapping[str, object],
) -> tuple[str, ...]:
    from sqlglot_tools.lineage import referenced_udf_calls

    udf_calls = referenced_udf_calls(expr)
    if not udf_calls:
        return ()
    snapshot_names = udf_names_from_snapshot(snapshot)
    lookup = {name.lower(): name for name in snapshot_names}
    required = {
        lookup[name.lower()] for name in udf_calls if isinstance(name, str) and name.lower() in lookup
    }
    return tuple(sorted(required))


def _validate_schema_contract(ctx: SessionContext, contract: SchemaContract) -> None:
    introspector = SchemaIntrospector(ctx)
    snapshot = introspector.snapshot
    if snapshot is None:
        msg = "Schema introspection snapshot unavailable for view validation."
        raise ValueError(msg)
    violations = contract.validate_against_introspection(snapshot)
    if violations:
        raise SchemaContractViolationError(
            table_name=contract.table_name,
            violations=violations,
        )


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


def _required_udfs(node: ViewNode, *, snapshot: Mapping[str, object]) -> tuple[str, ...]:
    if node.required_udfs:
        return tuple(node.required_udfs)
    if node.sqlglot_ast is None:
        return ()
    return _required_udfs_from_ast(node.sqlglot_ast, snapshot=snapshot)


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
