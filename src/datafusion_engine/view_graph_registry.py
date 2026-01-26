"""Dependency-aware view registration for view-driven pipelines."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.schema_contracts import SchemaContract
from datafusion_engine.schema_introspection import SchemaIntrospector
from datafusion_engine.udf_runtime import (
    validate_required_udfs,
    validate_rust_udf_snapshot,
)


@dataclass(frozen=True)
class ViewNode:
    """Declarative view definition with explicit dependencies."""

    name: str
    deps: tuple[str, ...]
    builder: Callable[[SessionContext], DataFrame]
    schema_contract: SchemaContract | None
    required_udfs: tuple[str, ...] = ()


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
    ordered = _topo_sort_nodes(nodes)
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    for node in ordered:
        _validate_deps(ctx, node, nodes)
        validate_required_udfs(snapshot, required=node.required_udfs)
        df = node.builder(ctx)
        adapter.register_view(
            node.name,
            df,
            overwrite=resolved.overwrite,
            temporary=resolved.temporary,
        )
        if resolved.validate_schema and node.schema_contract is not None:
            _validate_schema_contract(ctx, node.schema_contract)


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


def _validate_schema_contract(ctx: SessionContext, contract: SchemaContract) -> None:
    introspector = SchemaIntrospector(ctx)
    snapshot = introspector.snapshot
    if snapshot is None:
        msg = "Schema introspection snapshot unavailable for view validation."
        raise ValueError(msg)
    violations = contract.validate_against_introspection(snapshot)
    if violations:
        details = [
            f"{violation.violation_type.value}:{violation.column_name}"
            for violation in violations
        ]
        msg = f"Schema contract violations for {contract.table_name!r}: {details}."
        raise ValueError(msg)


def _topo_sort_nodes(nodes: Sequence[ViewNode]) -> tuple[ViewNode, ...]:
    node_map = {node.name: node for node in nodes}
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


__all__ = ["ViewGraphOptions", "ViewNode", "register_view_graph"]
