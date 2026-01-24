"""Rustworkx-backed task graph helpers for inference-driven scheduling."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

import pyarrow as pa
import rustworkx as rx

from relspec.errors import RelspecValidationError
from relspec.inferred_deps import InferredDeps
from storage.ipc import payload_hash

NodeKind = Literal["evidence", "task"]
EdgeKind = Literal["requires", "produces"]
OutputPolicy = Literal["all_producers"]

TASK_GRAPH_SNAPSHOT_VERSION = 1
_TASK_GRAPH_NODE_SCHEMA = pa.struct(
    [
        pa.field("id", pa.int32(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
        pa.field("output", pa.string(), nullable=True),
        pa.field("priority", pa.int32(), nullable=True),
        pa.field("signature", pa.string(), nullable=True),
    ]
)
_TASK_GRAPH_EDGE_SCHEMA = pa.struct(
    [
        pa.field("source", pa.int32(), nullable=False),
        pa.field("target", pa.int32(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
        pa.field("required_columns", pa.list_(pa.string()), nullable=False),
        pa.field(
            "required_types",
            pa.list_(
                pa.struct(
                    [
                        pa.field("name", pa.string(), nullable=False),
                        pa.field("type", pa.string(), nullable=False),
                    ]
                )
            ),
            nullable=False,
        ),
        pa.field(
            "required_metadata",
            pa.list_(
                pa.struct(
                    [
                        pa.field("key", pa.binary(), nullable=False),
                        pa.field("value", pa.binary(), nullable=False),
                    ]
                )
            ),
            nullable=False,
        ),
    ]
)
_TASK_GRAPH_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32(), nullable=False),
        pa.field("label", pa.string(), nullable=False),
        pa.field("output_policy", pa.string(), nullable=False),
        pa.field("nodes", pa.list_(_TASK_GRAPH_NODE_SCHEMA), nullable=False),
        pa.field("edges", pa.list_(_TASK_GRAPH_EDGE_SCHEMA), nullable=False),
    ]
)


@dataclass(frozen=True)
class EvidenceNode:
    """Evidence dataset node payload."""

    name: str


@dataclass(frozen=True)
class TaskNode:
    """Task node payload."""

    name: str
    output: str
    inputs: tuple[str, ...]
    sources: tuple[str, ...]
    priority: int


@dataclass(frozen=True)
class GraphNode:
    """Graph node wrapper with explicit kind."""

    kind: NodeKind
    payload: EvidenceNode | TaskNode


@dataclass(frozen=True)
class GraphEdge:
    """Graph edge payload."""

    kind: EdgeKind
    name: str
    required_columns: tuple[str, ...] = ()
    required_types: tuple[tuple[str, str], ...] = ()
    required_metadata: tuple[tuple[bytes, bytes], ...] = ()
    inferred: bool = False
    plan_fingerprint: str | None = None


@dataclass(frozen=True)
class TaskEdgeRequirements:
    """Required edge metadata for task graph construction."""

    columns: Mapping[str, Mapping[str, tuple[str, ...]]]
    types: Mapping[str, Mapping[str, tuple[tuple[str, str], ...]]]
    metadata: Mapping[str, Mapping[str, tuple[tuple[bytes, bytes], ...]]]


@dataclass(frozen=True)
class TaskGraph:
    """Rustworkx graph plus lookup indices."""

    graph: rx.PyDiGraph
    evidence_idx: Mapping[str, int]
    task_idx: Mapping[str, int]
    output_policy: OutputPolicy


@dataclass(frozen=True)
class TaskGraphSnapshot:
    """Deterministic snapshot of a task graph."""

    version: int
    label: str
    output_policy: OutputPolicy
    nodes: tuple[dict[str, object], ...]
    edges: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class GraphDiagnostics:
    """Diagnostics for task graphs."""

    status: Literal["ok", "cycle"]
    cycles: tuple[tuple[int, ...], ...] = ()
    scc: tuple[tuple[int, ...], ...] = ()
    critical_path_length: int | None = None
    critical_path: tuple[int, ...] | None = None
    dot: str | None = None
    node_map: dict[int, int] | None = None


def build_task_graph_from_inferred_deps(
    deps: Sequence[InferredDeps],
    *,
    output_policy: OutputPolicy = "all_producers",
    priority: int = 100,
) -> TaskGraph:
    """Build a task graph from inferred dependencies.

    Returns
    -------
    TaskGraph
        Graph constructed from inferred dependencies.
    """
    task_nodes = tuple(
        TaskNode(
            name=dep.task_name,
            output=dep.output,
            inputs=dep.inputs,
            sources=dep.inputs,
            priority=priority,
        )
        for dep in deps
    )
    fingerprints = {dep.task_name: dep.plan_fingerprint for dep in deps}
    requirements = TaskEdgeRequirements(
        columns={dep.task_name: dep.required_columns for dep in deps},
        types={dep.task_name: dep.required_types for dep in deps},
        metadata={dep.task_name: dep.required_metadata for dep in deps},
    )
    return _build_task_graph_inferred(
        task_nodes,
        output_policy=output_policy,
        fingerprints=fingerprints,
        requirements=requirements,
    )


def task_graph_snapshot(
    graph: TaskGraph,
    *,
    label: str,
    task_signatures: Mapping[str, str] | None = None,
) -> TaskGraphSnapshot:
    """Return a deterministic snapshot of a task graph.

    Returns
    -------
    TaskGraphSnapshot
        Deterministic snapshot for hashing or diagnostics.
    """
    signatures = dict(task_signatures or {})
    node_map = {id(graph.graph[idx]): idx for idx in graph.graph.node_indices()}
    try:
        ordered_nodes = rx.lexicographical_topological_sort(
            graph.graph,
            key=_node_sort_key,
        )
        ordered_pairs = [(node_map[id(node)], node) for node in ordered_nodes]
    except ValueError:
        ordered_pairs = [(idx, graph.graph[idx]) for idx in sorted(graph.graph.node_indices())]
    nodes = tuple(
        _node_payload(node_id, node, signatures=signatures) for node_id, node in ordered_pairs
    )
    edges = tuple(_edge_payloads(graph.graph))
    return TaskGraphSnapshot(
        version=TASK_GRAPH_SNAPSHOT_VERSION,
        label=label,
        output_policy=graph.output_policy,
        nodes=nodes,
        edges=edges,
    )


def task_graph_signature(snapshot: TaskGraphSnapshot) -> str:
    """Return a stable signature for a task graph snapshot.

    Returns
    -------
    str
        Stable signature of the snapshot payload.
    """
    payload = {
        "version": snapshot.version,
        "label": snapshot.label,
        "output_policy": snapshot.output_policy,
        "nodes": list(snapshot.nodes),
        "edges": list(snapshot.edges),
    }
    return payload_hash(payload, _TASK_GRAPH_SCHEMA)


def task_graph_diagnostics(graph: TaskGraph) -> GraphDiagnostics:
    """Return cycle and visualization diagnostics for a task graph.

    Returns
    -------
    GraphDiagnostics
        Diagnostic payload for the graph.
    """
    g = graph.graph
    if not rx.is_directed_acyclic_graph(g):
        cycles = tuple(tuple(cycle) for cycle in rx.simple_cycles(g))
        scc = tuple(tuple(component) for component in rx.strongly_connected_components(g))
        return GraphDiagnostics(status="cycle", cycles=cycles, scc=scc)
    reduced, mapping = rx.transitive_reduction(g)
    critical_path = tuple(rx.dag_longest_path(g))
    critical_path_length = rx.dag_longest_path_length(g)
    return GraphDiagnostics(
        status="ok",
        critical_path_length=critical_path_length,
        critical_path=critical_path,
        dot=reduced.to_dot(),
        node_map=dict(mapping),
    )


def _build_task_graph_inferred(
    tasks: Sequence[TaskNode],
    *,
    output_policy: OutputPolicy,
    fingerprints: Mapping[str, str],
    requirements: TaskEdgeRequirements,
) -> TaskGraph:
    if output_policy != "all_producers":
        msg = f"Unsupported output policy: {output_policy!r}."
        raise ValueError(msg)
    _validate_task_names(tasks)
    evidence_names = _collect_evidence_names(tasks)
    graph = rx.PyDiGraph(multigraph=False, check_cycle=False, attrs={"label": "relspec"})
    evidence_idx: dict[str, int] = {}
    task_idx: dict[str, int] = {}
    for name in sorted(evidence_names):
        evidence_idx[name] = graph.add_node(GraphNode("evidence", EvidenceNode(name)))
    for task in sorted(tasks, key=lambda item: item.name):
        task_idx[task.name] = graph.add_node(GraphNode("task", task))
    for task in tasks:
        task_node_idx = task_idx[task.name]
        task_required = requirements.columns.get(task.name, {})
        task_required_types = requirements.types.get(task.name, {})
        task_required_metadata = requirements.metadata.get(task.name, {})
        for source in task.sources:
            source_idx = evidence_idx[source]
            graph.add_edge(
                source_idx,
                task_node_idx,
                GraphEdge(
                    kind="requires",
                    name=source,
                    required_columns=tuple(task_required.get(source, ())),
                    required_types=tuple(task_required_types.get(source, ())),
                    required_metadata=tuple(task_required_metadata.get(source, ())),
                    inferred=True,
                    plan_fingerprint=fingerprints.get(task.name),
                ),
            )
        output_idx = evidence_idx[task.output]
        graph.add_edge(
            task_node_idx,
            output_idx,
            GraphEdge(
                kind="produces",
                name=task.output,
                inferred=True,
                plan_fingerprint=fingerprints.get(task.name),
            ),
        )
    return TaskGraph(
        graph=graph,
        evidence_idx=evidence_idx,
        task_idx=task_idx,
        output_policy=output_policy,
    )


def _collect_evidence_names(tasks: Sequence[TaskNode]) -> set[str]:
    names: set[str] = set()
    for task in tasks:
        names.add(task.output)
        names.update(task.inputs)
    return names


def _validate_task_names(tasks: Sequence[TaskNode]) -> None:
    seen: set[str] = set()
    for task in tasks:
        if not task.name:
            msg = "Task names must be non-empty."
            raise RelspecValidationError(msg)
        if task.name in seen:
            msg = f"Duplicate task name: {task.name!r}."
            raise RelspecValidationError(msg)
        seen.add(task.name)


def _node_sort_key(node: GraphNode) -> str:
    if node.kind == "evidence":
        return f"0:{node.payload.name}"
    payload = node.payload
    if isinstance(payload, TaskNode):
        return f"1:{payload.name}"
    return "1:"


def _node_payload(
    node_id: int,
    node: GraphNode,
    *,
    signatures: Mapping[str, str],
) -> dict[str, object]:
    if node.kind == "evidence":
        payload = node.payload
        if not isinstance(payload, EvidenceNode):
            msg = "Expected EvidenceNode payload for evidence graph node."
            raise TypeError(msg)
        return {
            "id": node_id,
            "kind": node.kind,
            "name": payload.name,
            "output": None,
            "priority": None,
            "signature": None,
        }
    payload = node.payload
    if not isinstance(payload, TaskNode):
        msg = "Expected TaskNode payload for task graph node."
        raise TypeError(msg)
    return {
        "id": node_id,
        "kind": node.kind,
        "name": payload.name,
        "output": payload.output,
        "priority": payload.priority,
        "signature": signatures.get(payload.name),
    }


def _edge_payloads(graph: rx.PyDiGraph) -> Sequence[dict[str, object]]:
    edges: list[dict[str, object]] = []
    for source, target, payload in graph.weighted_edge_list():
        if not isinstance(payload, GraphEdge):
            continue
        edges.append(
            {
                "source": source,
                "target": target,
                "kind": payload.kind,
                "name": payload.name,
                "required_columns": list(payload.required_columns),
                "required_types": [
                    {"name": name, "type": dtype} for name, dtype in payload.required_types
                ],
                "required_metadata": [
                    {"key": key, "value": value} for key, value in payload.required_metadata
                ],
            }
        )
    return edges


__all__ = [
    "EvidenceNode",
    "GraphDiagnostics",
    "GraphEdge",
    "GraphNode",
    "TaskEdgeRequirements",
    "TaskGraph",
    "TaskGraphSnapshot",
    "TaskNode",
    "build_task_graph_from_inferred_deps",
    "task_graph_diagnostics",
    "task_graph_signature",
    "task_graph_snapshot",
]
