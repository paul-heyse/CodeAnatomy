"""Rustworkx-backed task graph helpers for inference-driven scheduling."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import pyarrow as pa
import rustworkx as rx

from relspec.errors import RelspecValidationError
from relspec.inferred_deps import InferredDeps, infer_deps_from_view_nodes
from storage.ipc import payload_hash

if TYPE_CHECKING:
    from datafusion_engine.scan_planner import ScanUnit
    from datafusion_engine.view_graph_registry import ViewNode
    from schema_spec.system import DatasetSpec

NodeKind = Literal["evidence", "task"]
EdgeKind = Literal["requires", "produces"]
OutputPolicy = Literal["all_producers"]

TASK_GRAPH_SNAPSHOT_VERSION = 2
TASK_DEPENDENCY_SNAPSHOT_VERSION = 1
_CYCLE_EDGE_MIN_LEN = 2
_TASK_GRAPH_NODE_SCHEMA = pa.struct(
    [
        pa.field("id", pa.int32(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
        pa.field("output", pa.string(), nullable=True),
        pa.field("priority", pa.int32(), nullable=True),
        pa.field("signature", pa.string(), nullable=True),
        pa.field("scan_dataset_name", pa.string(), nullable=True),
        pa.field("scan_delta_version", pa.int64(), nullable=True),
        pa.field("scan_candidate_file_count", pa.int64(), nullable=True),
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
_TASK_DEPENDENCY_NODE_SCHEMA = pa.struct(
    [
        pa.field("id", pa.int32(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
        pa.field("priority", pa.int32(), nullable=False),
        pa.field("signature", pa.string(), nullable=True),
    ]
)
_TASK_DEPENDENCY_EDGE_SCHEMA = pa.struct(
    [
        pa.field("source", pa.int32(), nullable=False),
        pa.field("target", pa.int32(), nullable=False),
        pa.field("evidence_names", pa.list_(pa.string()), nullable=False),
    ]
)
_TASK_DEPENDENCY_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32(), nullable=False),
        pa.field("label", pa.string(), nullable=False),
        pa.field("nodes", pa.list_(_TASK_DEPENDENCY_NODE_SCHEMA), nullable=False),
        pa.field("edges", pa.list_(_TASK_DEPENDENCY_EDGE_SCHEMA), nullable=False),
    ]
)


@dataclass(frozen=True)
class EvidenceNode:
    """Evidence dataset node payload."""

    name: str
    scan_unit_key: str | None = None
    scan_dataset_name: str | None = None
    scan_delta_version: int | None = None
    scan_candidate_file_count: int | None = None


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
class TaskDependencySnapshot:
    """Deterministic snapshot of a task dependency graph."""

    version: int
    label: str
    nodes: tuple[dict[str, object], ...]
    edges: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class TaskDependencyReduction:
    """Reduction artifacts for task dependency graphs."""

    full_graph: rx.PyDiGraph
    reduced_graph: rx.PyDiGraph
    node_map: Mapping[int, int]
    full_signature: str
    reduced_signature: str
    edge_count: int
    reduced_edge_count: int
    removed_edge_count: int


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
    full_graph_signature: str | None = None
    reduced_graph_signature: str | None = None
    reduction_node_map: dict[int, int] | None = None
    critical_path_task_names: tuple[str, ...] = ()
    critical_path_length_weighted: float | None = None
    dependency_edge_count: int | None = None
    reduced_dependency_edge_count: int | None = None
    dependency_removed_edge_count: int | None = None


@dataclass(frozen=True)
class TaskGraphBuildOptions:
    """Options for building inferred task graphs."""

    output_policy: OutputPolicy = "all_producers"
    priority: int = 100
    priorities: Mapping[str, int] | None = None
    extra_evidence: Iterable[str] | None = None
    scan_units: Sequence[ScanUnit] = ()
    scan_keys_by_task: Mapping[str, tuple[str, ...]] | None = None


@dataclass(frozen=True)
class InferredGraphConfig:
    """Configuration for inferred graph assembly."""

    output_policy: OutputPolicy
    fingerprints: Mapping[str, str]
    requirements: TaskEdgeRequirements
    extra_evidence: Iterable[str] | None = None
    scan_units: Mapping[str, ScanUnit] | None = None


def build_task_graph_from_inferred_deps(
    deps: Sequence[InferredDeps],
    *,
    options: TaskGraphBuildOptions | None = None,
) -> TaskGraph:
    """Build a task graph from inferred dependencies.

    Returns
    -------
    TaskGraph
        Graph constructed from inferred dependencies.
    """
    resolved = options or TaskGraphBuildOptions()
    priority_map = resolved.priorities or {}
    scan_unit_map = {unit.key: unit for unit in resolved.scan_units}
    scan_key_map = dict(resolved.scan_keys_by_task or {})
    task_nodes = tuple(
        TaskNode(
            name=dep.task_name,
            output=dep.output,
            inputs=dep.inputs,
            sources=tuple(
                dict.fromkeys(
                    (
                        *dep.inputs,
                        *scan_key_map.get(dep.task_name, ()),
                    )
                )
            ),
            priority=priority_map.get(dep.task_name, resolved.priority),
        )
        for dep in deps
    )
    fingerprints = {dep.task_name: dep.plan_fingerprint for dep in deps}
    requirements = TaskEdgeRequirements(
        columns={dep.task_name: dep.required_columns for dep in deps},
        types={dep.task_name: dep.required_types for dep in deps},
        metadata={dep.task_name: dep.required_metadata for dep in deps},
    )
    scan_keys_all = tuple(sorted(scan_unit_map))
    extra_evidence_tokens = tuple(resolved.extra_evidence or ())
    extra_evidence_all = tuple(dict.fromkeys((*extra_evidence_tokens, *scan_keys_all)))
    return _build_task_graph_inferred(
        task_nodes,
        config=InferredGraphConfig(
            output_policy=resolved.output_policy,
            fingerprints=fingerprints,
            requirements=requirements,
            extra_evidence=extra_evidence_all,
            scan_units=scan_unit_map,
        ),
    )


def build_task_graph_from_views(
    nodes: Sequence[ViewNode],
    *,
    output_policy: OutputPolicy = "all_producers",
    priority: int = 100,
    priorities: Mapping[str, int] | None = None,
    extra_evidence: Iterable[str] | Sequence[DatasetSpec] | None = None,
) -> TaskGraph:
    """Build a task graph from view nodes and their SQLGlot ASTs.

    Returns
    -------
    TaskGraph
        Graph constructed from view node dependencies.
    """
    inferred = infer_deps_from_view_nodes(nodes)
    extra_evidence_names = _extra_evidence_names(extra_evidence)
    return build_task_graph_from_inferred_deps(
        inferred,
        options=TaskGraphBuildOptions(
            output_policy=output_policy,
            priority=priority,
            priorities=priorities,
            extra_evidence=extra_evidence_names,
        ),
    )


def _extra_evidence_names(
    extra_evidence: Iterable[str] | Sequence[DatasetSpec] | None,
) -> tuple[str, ...]:
    if extra_evidence is None:
        return ()
    names: set[str] = set()
    for item in extra_evidence:
        if isinstance(item, str):
            if item:
                names.add(item)
            continue
        name = getattr(item, "name", None)
        if isinstance(name, str) and name:
            names.add(name)
            continue
        msg = "Extra evidence must be strings or dataset specs with a name."
        raise TypeError(msg)
    return tuple(sorted(names))


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
    node_map = dict(
        zip(
            [node_idx for node_idx, _node in nodes_to_add],
            node_indices,
            strict=True,
        )
    )
    edges_to_add = [
        (node_map[source], node_map[target], payload) for source, target, payload in edge_payloads
    ]
    subgraph.add_edges_from(edges_to_add)
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


def task_dependency_graph(graph: TaskGraph) -> rx.PyDiGraph:
    """Return a task-only dependency graph derived from evidence edges.

    Returns
    -------
    rx.PyDiGraph
        Task-only dependency graph.

    Raises
    ------
    TypeError
        Raised when a task node payload is not a TaskNode.
    """
    tasks: list[TaskNode] = []
    for name in sorted(graph.task_idx):
        node_idx = graph.task_idx[name]
        node = graph.graph[node_idx]
        if node.kind != "task":
            continue
        payload = node.payload
        if not isinstance(payload, TaskNode):
            msg = "Expected TaskNode payload for task graph node."
            raise TypeError(msg)
        tasks.append(payload)
    dependency_edges = _task_dependency_edges(graph)
    dep_graph = rx.PyDiGraph(
        multigraph=False,
        check_cycle=False,
        node_count_hint=len(tasks),
        edge_count_hint=len(dependency_edges),
    )
    node_indices = dep_graph.add_nodes_from(tasks)
    task_idx = dict(zip([task.name for task in tasks], node_indices, strict=True))
    edge_payloads: list[tuple[int, int, tuple[str, ...]]] = []
    for (source, target), evidence_names in dependency_edges.items():
        source_idx = task_idx.get(source)
        target_idx = task_idx.get(target)
        if source_idx is None or target_idx is None:
            continue
        edge_payloads.append((source_idx, target_idx, tuple(sorted(evidence_names))))
    dep_graph.add_edges_from(edge_payloads)
    return dep_graph


def task_dependency_reduction(
    graph: TaskGraph,
    *,
    task_signatures: Mapping[str, str] | None = None,
    label: str = "task_dependency",
) -> TaskDependencyReduction:
    """Return full and reduced task dependency graphs with signatures.

    Returns
    -------
    TaskDependencyReduction
        Reduction artifacts and signatures for dependency graphs.

    Raises
    ------
    RelspecValidationError
        Raised when the dependency graph contains a cycle.
    """
    full_graph = task_dependency_graph(graph)
    if not rx.is_directed_acyclic_graph(full_graph):
        msg = "Task dependency graph contains a cycle."
        raise RelspecValidationError(msg)
    edge_count = full_graph.num_edges()
    reduced_graph, node_map = rx.transitive_reduction(full_graph)
    reduced_edge_count = reduced_graph.num_edges()
    removed_edge_count = edge_count - reduced_edge_count
    full_snapshot = _task_dependency_snapshot(
        full_graph,
        label=label,
        task_signatures=task_signatures,
    )
    reduced_snapshot = _task_dependency_snapshot(
        reduced_graph,
        label=label,
        task_signatures=task_signatures,
    )
    full_signature = task_dependency_signature(full_snapshot)
    reduced_signature = task_dependency_signature(reduced_snapshot)
    return TaskDependencyReduction(
        full_graph=full_graph,
        reduced_graph=reduced_graph,
        node_map=dict(node_map),
        full_signature=full_signature,
        reduced_signature=reduced_signature,
        edge_count=edge_count,
        reduced_edge_count=reduced_edge_count,
        removed_edge_count=removed_edge_count,
    )


def task_dependency_signature(snapshot: TaskDependencySnapshot) -> str:
    """Return a stable signature for a task dependency snapshot.

    Returns
    -------
    str
        Stable hash signature for the snapshot.
    """
    payload = {
        "version": snapshot.version,
        "label": snapshot.label,
        "nodes": list(snapshot.nodes),
        "edges": list(snapshot.edges),
    }
    return payload_hash(payload, _TASK_DEPENDENCY_SCHEMA)


def task_dependency_critical_path(graph: rx.PyDiGraph) -> tuple[int, ...]:
    """Return the weighted critical path node indices for a dependency graph.

    Returns
    -------
    tuple[int, ...]
        Node indices along the weighted critical path.
    """

    def _edge_weight(_source: int, target: int, _edge: object) -> float:
        return _task_node_weight(graph[target])

    return tuple(rx.dag_weighted_longest_path(graph, _edge_weight))


def task_dependency_critical_path_length(graph: rx.PyDiGraph) -> float:
    """Return the weighted critical path length for a dependency graph.

    Returns
    -------
    float
        Weighted critical path length.
    """

    def _edge_weight(_source: int, target: int, _edge: object) -> float:
        return _task_node_weight(graph[target])

    length = rx.dag_weighted_longest_path_length(graph, _edge_weight)
    return float(length)


def task_dependency_critical_path_tasks(graph: rx.PyDiGraph) -> tuple[str, ...]:
    """Return the critical path as task names.

    Returns
    -------
    tuple[str, ...]
        Task names along the critical path.
    """
    names: list[str] = []
    for node_idx in task_dependency_critical_path(graph):
        node = graph[node_idx]
        if isinstance(node, TaskNode):
            names.append(node.name)
    return tuple(names)


def _task_dependency_edges(graph: TaskGraph) -> dict[tuple[str, str], set[str]]:
    edges: dict[tuple[str, str], set[str]] = {}
    for evidence_name, evidence_idx in graph.evidence_idx.items():
        producers = _task_neighbors(
            graph,
            graph.graph.predecessor_indices(evidence_idx),
        )
        consumers = _task_neighbors(
            graph,
            graph.graph.successor_indices(evidence_idx),
        )
        if not producers or not consumers:
            continue
        for producer in producers:
            for consumer in consumers:
                if producer == consumer:
                    continue
                edges.setdefault((producer, consumer), set()).add(evidence_name)
    return edges


def _task_neighbors(graph: TaskGraph, nodes: Iterable[int]) -> list[str]:
    names: list[str] = []
    for idx in nodes:
        node = graph.graph[idx]
        if node.kind != "task":
            continue
        payload = node.payload
        if not isinstance(payload, TaskNode):
            msg = "Expected TaskNode payload for task graph node."
            raise TypeError(msg)
        names.append(payload.name)
    return sorted(set(names))


def _task_dependency_snapshot(
    graph: rx.PyDiGraph,
    *,
    label: str,
    task_signatures: Mapping[str, str] | None,
) -> TaskDependencySnapshot:
    signatures = dict(task_signatures or {})
    node_map = {id(graph[idx]): idx for idx in graph.node_indices()}
    try:
        ordered_nodes = rx.lexicographical_topological_sort(
            graph,
            key=_dependency_node_sort_key,
        )
        ordered_pairs = [(node_map[id(node)], node) for node in ordered_nodes]
    except ValueError:
        ordered_pairs = [
            (idx, graph[idx])
            for idx in sorted(
                graph.node_indices(),
                key=lambda item: _dependency_node_sort_key(graph[item]),
            )
        ]
    nodes = tuple(
        _dependency_node_payload(node_idx, node, signatures=signatures)
        for node_idx, node in ordered_pairs
    )
    edges = tuple(_task_dependency_edge_payloads(graph))
    return TaskDependencySnapshot(
        version=TASK_DEPENDENCY_SNAPSHOT_VERSION,
        label=label,
        nodes=nodes,
        edges=edges,
    )


def _task_dependency_edge_payloads(graph: rx.PyDiGraph) -> Sequence[dict[str, object]]:
    node_names = {
        idx: graph[idx].name for idx in graph.node_indices() if isinstance(graph[idx], TaskNode)
    }
    edges: list[dict[str, object]] = []
    for source, target, payload in graph.weighted_edge_list():
        evidence_names = _evidence_name_list(payload)
        edges.append(
            {
                "source": source,
                "target": target,
                "evidence_names": evidence_names,
            }
        )

    def _edge_sort_key(item: Mapping[str, object]) -> tuple[str, str, str]:
        source = item.get("source")
        target = item.get("target")
        source_name = node_names.get(source, "") if isinstance(source, int) else ""
        target_name = node_names.get(target, "") if isinstance(target, int) else ""
        evidence_names = item.get("evidence_names")
        if isinstance(evidence_names, Sequence) and not isinstance(
            evidence_names,
            (str, bytes),
        ):
            evidence_label = ",".join(str(name) for name in evidence_names)
        else:
            evidence_label = ""
        return source_name, target_name, evidence_label

    edges.sort(key=_edge_sort_key)
    return edges


def _evidence_name_list(payload: object) -> list[str]:
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)):
        names = [name for name in payload if isinstance(name, str)]
        return sorted(set(names))
    if isinstance(payload, str):
        return [payload]
    return []


def _dependency_node_sort_key(node: object) -> str:
    if isinstance(node, TaskNode):
        return node.name
    return ""


def _dependency_node_payload(
    node_id: int,
    node: object,
    *,
    signatures: Mapping[str, str],
) -> dict[str, object]:
    if not isinstance(node, TaskNode):
        msg = "Expected TaskNode payload for dependency graph node."
        raise TypeError(msg)
    return {
        "id": node_id,
        "name": node.name,
        "priority": int(node.priority),
        "signature": signatures.get(node.name),
    }


def _task_node_weight(node: object) -> float:
    if isinstance(node, TaskNode):
        return float(max(node.priority, 1))
    payload = getattr(node, "payload", None)
    priority = getattr(payload, "priority", 1)
    if isinstance(priority, int):
        return float(max(priority, 1))
    return 1.0


def _build_task_graph_inferred(
    tasks: Sequence[TaskNode],
    *,
    config: InferredGraphConfig,
) -> TaskGraph:
    evidence_names = _collect_evidence_names(tasks, extra=config.extra_evidence)
    graph, evidence_idx, task_idx, tasks_sorted = _seed_inferred_task_graph(
        tasks,
        evidence_names=evidence_names,
        output_policy=config.output_policy,
        scan_units=config.scan_units,
    )
    _add_inferred_task_edges(
        graph,
        tasks_sorted=tasks_sorted,
        context=InferredEdgeContext(
            evidence_idx=evidence_idx,
            task_idx=task_idx,
            fingerprints=config.fingerprints,
            requirements=config.requirements,
        ),
    )
    return TaskGraph(
        graph=graph,
        evidence_idx=evidence_idx,
        task_idx=task_idx,
        output_policy=config.output_policy,
    )


def _seed_inferred_task_graph(
    tasks: Sequence[TaskNode],
    *,
    evidence_names: set[str],
    output_policy: OutputPolicy,
    scan_units: Mapping[str, ScanUnit] | None = None,
) -> tuple[rx.PyDiGraph, dict[str, int], dict[str, int], list[TaskNode]]:
    if output_policy != "all_producers":
        msg = f"Unsupported output policy: {output_policy!r}."
        raise ValueError(msg)
    _validate_task_names(tasks)
    evidence_names_sorted = sorted(evidence_names)
    tasks_sorted = sorted(tasks, key=lambda item: item.name)
    scan_unit_map = dict(scan_units or {})
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
        GraphNode("evidence", _evidence_node(name, scan_units=scan_unit_map))
        for name in evidence_names_sorted
    ]
    task_payloads = [GraphNode("task", task) for task in tasks_sorted]
    evidence_indices = graph.add_nodes_from(evidence_payloads)
    task_indices = graph.add_nodes_from(task_payloads)
    evidence_idx = dict(zip(evidence_names_sorted, evidence_indices, strict=True))
    task_idx = dict(
        zip(
            [task.name for task in tasks_sorted],
            task_indices,
            strict=True,
        )
    )
    return graph, evidence_idx, task_idx, tasks_sorted


def _evidence_node(name: str, *, scan_units: Mapping[str, ScanUnit]) -> EvidenceNode:
    unit = scan_units.get(name)
    if unit is None:
        return EvidenceNode(name=name)
    candidate_count = len(unit.candidate_files)
    delta_version = unit.delta_version
    return EvidenceNode(
        name=name,
        scan_unit_key=unit.key,
        scan_dataset_name=unit.dataset_name,
        scan_delta_version=delta_version,
        scan_candidate_file_count=candidate_count,
    )


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
        edge_payloads.extend(_task_edge_payloads(task, context=context))
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


def _collect_evidence_names(
    tasks: Sequence[TaskNode],
    *,
    extra: Iterable[str] | None = None,
) -> set[str]:
    names: set[str] = set()
    for task in tasks:
        names.add(task.output)
        names.update(task.sources)
    if extra is not None:
        names.update(extra)
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
            "scan_dataset_name": payload.scan_dataset_name,
            "scan_delta_version": payload.scan_delta_version,
            "scan_candidate_file_count": payload.scan_candidate_file_count,
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
        "scan_dataset_name": None,
        "scan_delta_version": None,
        "scan_candidate_file_count": None,
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


def _evidence_node_attrs(evidence: EvidenceNode) -> dict[str, str]:
    attrs: dict[str, str] = {
        "kind": "evidence",
        "name": evidence.name,
    }
    if evidence.scan_dataset_name is not None:
        attrs["scan_dataset_name"] = evidence.scan_dataset_name
    if evidence.scan_delta_version is not None:
        attrs["scan_delta_version"] = str(evidence.scan_delta_version)
    if evidence.scan_candidate_file_count is not None:
        attrs["scan_candidate_file_count"] = str(evidence.scan_candidate_file_count)
    return attrs


def _task_node_attrs(task: TaskNode) -> dict[str, str]:
    return {
        "kind": "task",
        "name": task.name,
        "output": task.output,
        "priority": str(task.priority),
        "inputs": json.dumps(list(task.inputs)),
        "sources": json.dumps(list(task.sources)),
    }


def _node_link_node_attrs(payload: object) -> dict[str, str]:
    if isinstance(payload, GraphNode):
        if payload.kind == "evidence" and isinstance(payload.payload, EvidenceNode):
            return _evidence_node_attrs(payload.payload)
        if payload.kind == "task" and isinstance(payload.payload, TaskNode):
            return _task_node_attrs(payload.payload)
    if isinstance(payload, TaskNode):
        return _task_node_attrs(payload)
    if isinstance(payload, EvidenceNode):
        return _evidence_node_attrs(payload)
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
    "TaskDependencyReduction",
    "TaskDependencySnapshot",
    "TaskEdgeRequirements",
    "TaskGraph",
    "TaskGraphBuildOptions",
    "TaskGraphSnapshot",
    "TaskNode",
    "build_task_graph_from_inferred_deps",
    "build_task_graph_from_views",
    "task_dependency_critical_path",
    "task_dependency_critical_path_length",
    "task_dependency_critical_path_tasks",
    "task_dependency_graph",
    "task_dependency_reduction",
    "task_dependency_signature",
    "task_graph_diagnostics",
    "task_graph_impact_subgraph",
    "task_graph_isolate_labels",
    "task_graph_node_label",
    "task_graph_node_link_json",
    "task_graph_signature",
    "task_graph_snapshot",
    "task_graph_subgraph",
]
