"""Rustworkx-backed task graph helpers for inference-driven scheduling."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
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
_CYCLE_EDGE_MIN_LEN = 2
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
    cycle_sample: tuple[int, ...] = ()
    critical_path_length: int | None = None
    critical_path: tuple[int, ...] | None = None
    dot: str | None = None
    node_map: dict[int, int] | None = None
    weak_components: tuple[tuple[int, ...], ...] = ()
    isolates: tuple[int, ...] = ()
    isolate_labels: tuple[str, ...] = ()
    node_link_json: str | None = None


def build_task_graph_from_inferred_deps(
    deps: Sequence[InferredDeps],
    *,
    output_policy: OutputPolicy = "all_producers",
    priority: int = 100,
    priorities: Mapping[str, int] | None = None,
) -> TaskGraph:
    """Build a task graph from inferred dependencies.

    Returns
    -------
    TaskGraph
        Graph constructed from inferred dependencies.
    """
    priority_map = priorities or {}
    task_nodes = tuple(
        TaskNode(
            name=dep.task_name,
            output=dep.output,
            inputs=dep.inputs,
            sources=dep.inputs,
            priority=priority_map.get(dep.task_name, priority),
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


def task_graph_diagnostics(
    graph: TaskGraph,
    *,
    include_cycles: bool = False,
    include_node_link: bool = False,
) -> GraphDiagnostics:
    """Return cycle and visualization diagnostics for a task graph.

    Returns
    -------
    GraphDiagnostics
        Diagnostic payload for the graph.
    """
    g = graph.graph
    node_link_json = task_graph_node_link_json(graph) if include_node_link else None
    if not rx.is_directed_acyclic_graph(g):
        return _cycle_graph_diagnostics(
            g,
            include_cycles=include_cycles,
            node_link_json=node_link_json,
        )
    return _dag_graph_diagnostics(graph, node_link_json=node_link_json)


def _cycle_graph_diagnostics(
    graph: rx.PyDiGraph,
    *,
    include_cycles: bool,
    node_link_json: str | None,
) -> GraphDiagnostics:
    cycle_sample = _cycle_sample_nodes(rx.digraph_find_cycle(graph))
    cycles = tuple(tuple(cycle) for cycle in rx.simple_cycles(graph)) if include_cycles else ()
    scc = tuple(tuple(component) for component in rx.strongly_connected_components(graph))
    return GraphDiagnostics(
        status="cycle",
        cycles=cycles,
        scc=scc,
        cycle_sample=cycle_sample,
        node_link_json=node_link_json,
    )


def _cycle_sample_nodes(cycle: Iterable[object]) -> tuple[int, ...]:
    nodes: list[int] = []
    for item in cycle:
        if isinstance(item, int):
            nodes.append(item)
            continue
        if isinstance(item, tuple) and len(item) >= _CYCLE_EDGE_MIN_LEN:
            left, right = item[0], item[1]
            if isinstance(left, int):
                nodes.append(left)
            if isinstance(right, int):
                nodes.append(right)
    if not nodes:
        return ()
    deduped: list[int] = []
    for node in nodes:
        if not deduped or deduped[-1] != node:
            deduped.append(node)
    return tuple(deduped)


def _dag_graph_diagnostics(
    graph: TaskGraph,
    *,
    node_link_json: str | None,
) -> GraphDiagnostics:
    reduced, mapping = rx.transitive_reduction(graph.graph)
    critical_path = tuple(rx.dag_longest_path(graph.graph))
    critical_path_length = rx.dag_longest_path_length(graph.graph)
    weak_components = tuple(
        tuple(component) for component in rx.weakly_connected_components(graph.graph)
    )
    isolates = tuple(rx.isolates(graph.graph))
    return GraphDiagnostics(
        status="ok",
        critical_path_length=critical_path_length,
        critical_path=critical_path,
        dot=reduced.to_dot(),
        node_map=dict(mapping),
        weak_components=weak_components,
        isolates=isolates,
        isolate_labels=task_graph_isolate_labels(graph, isolates),
        node_link_json=node_link_json,
    )


def task_graph_node_label(graph: TaskGraph, node_idx: int) -> str:
    """Return a stable label for a task graph node.

    Raises
    ------
    TypeError
        Raised when the node payload is not a GraphNode.

    Returns
    -------
    str
        Node label prefixed with kind.
    """
    node = graph.graph[node_idx]
    if not isinstance(node, GraphNode):
        msg = "Expected GraphNode payload for task graph node."
        raise TypeError(msg)
    payload = node.payload
    name = payload.name if isinstance(payload, (EvidenceNode, TaskNode)) else ""
    return f"{node.kind}:{name}"


def task_graph_node_link_json(graph: TaskGraph) -> str:
    """Return node-link JSON for a task graph.

    Returns
    -------
    str
        Node-link JSON string for the graph.
    """
    payload = rx.node_link_json(
        graph.graph,
        graph_attrs=_node_link_graph_attrs,
        node_attrs=_node_link_node_attrs,
        edge_attrs=_node_link_edge_attrs,
    )
    return payload or ""


def task_graph_isolate_labels(
    graph: TaskGraph,
    isolates: Iterable[int],
) -> tuple[str, ...]:
    """Return human-readable labels for isolated nodes.

    Returns
    -------
    tuple[str, ...]
        Sorted labels for isolates in the graph.
    """
    node_ids = set(graph.graph.node_indices())
    labels: set[str] = set()
    for node_idx in isolates:
        if node_idx not in node_ids:
            continue
        labels.add(task_graph_node_label(graph, node_idx))
    return tuple(sorted(labels))


def task_graph_subgraph(
    graph: TaskGraph,
    *,
    node_ids: Iterable[int],
) -> TaskGraph:
    """Return a TaskGraph subgraph for selected node ids.

    Returns
    -------
    TaskGraph
        Subgraph with matching nodes and edges.
    """
    node_id_set = set(node_ids)
    nodes_to_add: list[tuple[int, GraphNode]] = []
    node_labels: dict[int, str] = {}
    for node_idx in node_id_set:
        node = graph.graph[node_idx]
        if not isinstance(node, GraphNode):
            continue
        nodes_to_add.append((node_idx, node))
        node_labels[node_idx] = task_graph_node_label(graph, node_idx)
    nodes_to_add.sort(key=lambda item: (_node_sort_key(item[1]), item[0]))
    edge_payloads: list[tuple[int, int, GraphEdge]] = []
    for source, target, payload in graph.graph.weighted_edge_list():
        if source not in node_id_set or target not in node_id_set:
            continue
        if not isinstance(payload, GraphEdge):
            continue
        edge_payloads.append((source, target, payload))
    edge_payloads.sort(
        key=lambda item: (
            node_labels.get(item[0], ""),
            node_labels.get(item[1], ""),
            item[2].kind,
            item[2].name,
        )
    )
    subgraph = rx.PyDiGraph(
        multigraph=False,
        check_cycle=False,
        attrs=graph.graph.attrs,
        node_count_hint=len(nodes_to_add),
        edge_count_hint=len(edge_payloads),
    )
    node_indices = subgraph.add_nodes_from([node for _, node in nodes_to_add])
    node_map = dict(zip([node_idx for node_idx, _ in nodes_to_add], node_indices, strict=True))
    subgraph.add_edges_from(
        [
            (node_map[source], node_map[target], payload)
            for source, target, payload in edge_payloads
        ]
    )
    evidence_idx: dict[str, int] = {}
    task_idx: dict[str, int] = {}
    for idx in subgraph.node_indices():
        node = subgraph[idx]
        if not isinstance(node, GraphNode):
            continue
        payload = node.payload
        if isinstance(payload, EvidenceNode):
            evidence_idx[payload.name] = idx
        elif isinstance(payload, TaskNode):
            task_idx[payload.name] = idx
    return TaskGraph(
        graph=subgraph,
        evidence_idx=evidence_idx,
        task_idx=task_idx,
        output_policy=graph.output_policy,
    )


def task_graph_impact_subgraph(
    graph: TaskGraph,
    *,
    task_names: Iterable[str],
) -> TaskGraph:
    """Return the impact subgraph for a set of task names.

    Returns
    -------
    TaskGraph
        Subgraph containing impacted tasks and related evidence nodes.
    """
    impacted: set[int] = set()
    for name in task_names:
        idx = graph.task_idx.get(name)
        if idx is None:
            continue
        impacted.add(idx)
        impacted.update(rx.ancestors(graph.graph, idx))
        impacted.update(rx.descendants(graph.graph, idx))
    return task_graph_subgraph(graph, node_ids=impacted)


def _build_task_graph_inferred(
    tasks: Sequence[TaskNode],
    *,
    output_policy: OutputPolicy,
    fingerprints: Mapping[str, str],
    requirements: TaskEdgeRequirements,
) -> TaskGraph:
    evidence_names = _collect_evidence_names(tasks)
    graph, evidence_idx, task_idx, tasks_sorted = _seed_inferred_task_graph(
        tasks,
        evidence_names=evidence_names,
        output_policy=output_policy,
    )
    _add_inferred_task_edges(
        graph,
        tasks_sorted=tasks_sorted,
        context=InferredEdgeContext(
            evidence_idx=evidence_idx,
            task_idx=task_idx,
            fingerprints=fingerprints,
            requirements=requirements,
        ),
    )
    return TaskGraph(
        graph=graph,
        evidence_idx=evidence_idx,
        task_idx=task_idx,
        output_policy=output_policy,
    )


def _seed_inferred_task_graph(
    tasks: Sequence[TaskNode],
    *,
    evidence_names: set[str],
    output_policy: OutputPolicy,
) -> tuple[rx.PyDiGraph, dict[str, int], dict[str, int], list[TaskNode]]:
    if output_policy != "all_producers":
        msg = f"Unsupported output policy: {output_policy!r}."
        raise ValueError(msg)
    _validate_task_names(tasks)
    evidence_names_sorted = sorted(evidence_names)
    tasks_sorted = sorted(tasks, key=lambda item: item.name)
    node_count_hint = len(evidence_names_sorted) + len(tasks_sorted)
    edge_count_hint = sum(len(task.sources) + 1 for task in tasks_sorted)
    graph = rx.PyDiGraph(
        multigraph=False,
        check_cycle=False,
        attrs={"label": "relspec"},
        node_count_hint=node_count_hint,
        edge_count_hint=edge_count_hint,
    )
    evidence_payloads = [
        GraphNode("evidence", EvidenceNode(name)) for name in evidence_names_sorted
    ]
    task_payloads = [GraphNode("task", task) for task in tasks_sorted]
    evidence_indices = graph.add_nodes_from(evidence_payloads)
    task_indices = graph.add_nodes_from(task_payloads)
    evidence_idx = dict(zip(evidence_names_sorted, evidence_indices, strict=True))
    task_idx = dict(zip([task.name for task in tasks_sorted], task_indices, strict=True))
    return graph, evidence_idx, task_idx, tasks_sorted


@dataclass(frozen=True)
class InferredEdgeContext:
    """Shared context for inferred edge construction."""

    evidence_idx: Mapping[str, int]
    task_idx: Mapping[str, int]
    fingerprints: Mapping[str, str]
    requirements: TaskEdgeRequirements


def _add_inferred_task_edges(
    graph: rx.PyDiGraph,
    *,
    tasks_sorted: Sequence[TaskNode],
    context: InferredEdgeContext,
) -> None:
    edge_payloads: list[tuple[int, int, GraphEdge]] = []
    for task in tasks_sorted:
        edge_payloads.extend(
            _task_edge_payloads(task, context=context)
        )
    graph.add_edges_from(edge_payloads)


def _task_edge_payloads(
    task: TaskNode,
    *,
    context: InferredEdgeContext,
) -> list[tuple[int, int, GraphEdge]]:
    task_node_idx = context.task_idx[task.name]
    task_required = context.requirements.columns.get(task.name, {})
    task_required_types = context.requirements.types.get(task.name, {})
    task_required_metadata = context.requirements.metadata.get(task.name, {})
    payloads: list[tuple[int, int, GraphEdge]] = []
    for source in sorted(task.sources):
        source_idx = context.evidence_idx[source]
        payloads.append(
            (
                source_idx,
                task_node_idx,
                GraphEdge(
                    kind="requires",
                    name=source,
                    required_columns=tuple(task_required.get(source, ())),
                    required_types=tuple(task_required_types.get(source, ())),
                    required_metadata=tuple(task_required_metadata.get(source, ())),
                    inferred=True,
                    plan_fingerprint=context.fingerprints.get(task.name),
                ),
            )
        )
    output_idx = context.evidence_idx[task.output]
    payloads.append(
        (
            task_node_idx,
            output_idx,
            GraphEdge(
                kind="produces",
                name=task.output,
                inferred=True,
                plan_fingerprint=context.fingerprints.get(task.name),
            ),
        )
    )
    return payloads


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


def _node_link_graph_attrs(attrs: object) -> dict[str, str]:
    if isinstance(attrs, Mapping):
        return {str(key): str(value) for key, value in attrs.items()}
    if attrs is None:
        return {}
    return {"value": str(attrs)}


def _node_link_node_attrs(payload: object) -> dict[str, str]:
    if isinstance(payload, GraphNode):
        if payload.kind == "evidence" and isinstance(payload.payload, EvidenceNode):
            return {
                "kind": payload.kind,
                "name": payload.payload.name,
            }
        if payload.kind == "task" and isinstance(payload.payload, TaskNode):
            task = payload.payload
            return {
                "kind": payload.kind,
                "name": task.name,
                "output": task.output,
                "priority": str(task.priority),
                "inputs": json.dumps(list(task.inputs)),
                "sources": json.dumps(list(task.sources)),
            }
    if isinstance(payload, TaskNode):
        return {
            "kind": "task",
            "name": payload.name,
            "output": payload.output,
            "priority": str(payload.priority),
            "inputs": json.dumps(list(payload.inputs)),
            "sources": json.dumps(list(payload.sources)),
        }
    if isinstance(payload, EvidenceNode):
        return {"kind": "evidence", "name": payload.name}
    return {}


def _node_link_edge_attrs(payload: object) -> dict[str, str]:
    if not isinstance(payload, GraphEdge):
        return {}
    metadata = [
        {"key": key.hex(), "value": value.hex()} for key, value in payload.required_metadata
    ]
    return {
        "kind": payload.kind,
        "name": payload.name,
        "required_columns": json.dumps(list(payload.required_columns)),
        "required_types": json.dumps(
            [{"name": name, "type": dtype} for name, dtype in payload.required_types],
            sort_keys=True,
        ),
        "required_metadata": json.dumps(metadata, sort_keys=True),
        "inferred": json.dumps(payload.inferred),
        "plan_fingerprint": payload.plan_fingerprint or "",
    }


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
    "task_graph_impact_subgraph",
    "task_graph_isolate_labels",
    "task_graph_node_label",
    "task_graph_node_link_json",
    "task_graph_signature",
    "task_graph_snapshot",
    "task_graph_subgraph",
]
