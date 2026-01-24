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

NodeKind = Literal["evidence", "rule"]
EdgeKind = Literal["requires", "produces"]
OutputPolicy = Literal["all_producers"]

RULE_GRAPH_SNAPSHOT_VERSION = 1
_RULE_GRAPH_NODE_SCHEMA = pa.struct(
    [
        pa.field("id", pa.int32(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
        pa.field("output", pa.string(), nullable=True),
        pa.field("priority", pa.int32(), nullable=True),
        pa.field("signature", pa.string(), nullable=True),
    ]
)
_RULE_GRAPH_EDGE_SCHEMA = pa.struct(
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
_RULE_GRAPH_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32(), nullable=False),
        pa.field("label", pa.string(), nullable=False),
        pa.field("output_policy", pa.string(), nullable=False),
        pa.field("nodes", pa.list_(_RULE_GRAPH_NODE_SCHEMA), nullable=False),
        pa.field("edges", pa.list_(_RULE_GRAPH_EDGE_SCHEMA), nullable=False),
    ]
)


@dataclass(frozen=True)
class EvidenceNode:
    """Evidence dataset node payload."""

    name: str


@dataclass(frozen=True)
class RuleNode:
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
    payload: EvidenceNode | RuleNode


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
class RuleGraph:
    """Rustworkx graph plus lookup indices."""

    graph: rx.PyDiGraph
    evidence_idx: Mapping[str, int]
    rule_idx: Mapping[str, int]
    output_policy: OutputPolicy


@dataclass(frozen=True)
class RuleGraphSnapshot:
    """Deterministic snapshot of a rule graph."""

    version: int
    label: str
    output_policy: OutputPolicy
    nodes: tuple[dict[str, object], ...]
    edges: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class GraphDiagnostics:
    """Diagnostics for rule graphs."""

    status: Literal["ok", "cycle"]
    cycles: tuple[tuple[int, ...], ...] = ()
    scc: tuple[tuple[int, ...], ...] = ()
    critical_path_length: int | None = None
    critical_path: tuple[int, ...] | None = None
    dot: str | None = None
    node_map: dict[int, int] | None = None


def build_rule_graph_from_inferred_deps(
    deps: Sequence[InferredDeps],
    *,
    output_policy: OutputPolicy = "all_producers",
    priority: int = 100,
) -> RuleGraph:
    """Build a rule graph from inferred dependencies.

    Returns
    -------
    RuleGraph
        Graph constructed from inferred dependencies.
    """
    rule_nodes = tuple(
        RuleNode(
            name=dep.rule_name,
            output=dep.output,
            inputs=dep.inputs,
            sources=dep.inputs,
            priority=priority,
        )
        for dep in deps
    )
    fingerprints = {dep.rule_name: dep.plan_fingerprint for dep in deps}
    required_columns = {dep.rule_name: dep.required_columns for dep in deps}
    return _build_rule_graph_inferred(
        rule_nodes,
        output_policy=output_policy,
        fingerprints=fingerprints,
        required_columns=required_columns,
    )


def rule_graph_snapshot(
    graph: RuleGraph,
    *,
    label: str,
    rule_signatures: Mapping[str, str] | None = None,
) -> RuleGraphSnapshot:
    """Return a deterministic snapshot of a rule graph.

    Returns
    -------
    RuleGraphSnapshot
        Deterministic snapshot for hashing or diagnostics.
    """
    signatures = dict(rule_signatures or {})
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
    return RuleGraphSnapshot(
        version=RULE_GRAPH_SNAPSHOT_VERSION,
        label=label,
        output_policy=graph.output_policy,
        nodes=nodes,
        edges=edges,
    )


def rule_graph_signature(snapshot: RuleGraphSnapshot) -> str:
    """Return a stable signature for a rule graph snapshot.

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
    return payload_hash(payload, _RULE_GRAPH_SCHEMA)


def rule_graph_diagnostics(graph: RuleGraph) -> GraphDiagnostics:
    """Return cycle and visualization diagnostics for a rule graph.

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


def _build_rule_graph_inferred(
    rules: Sequence[RuleNode],
    *,
    output_policy: OutputPolicy,
    fingerprints: Mapping[str, str],
    required_columns: Mapping[str, Mapping[str, tuple[str, ...]]],
) -> RuleGraph:
    if output_policy != "all_producers":
        msg = f"Unsupported output policy: {output_policy!r}."
        raise ValueError(msg)
    _validate_rule_names(rules)
    evidence_names = _collect_evidence_names(rules)
    graph = rx.PyDiGraph(multigraph=False, check_cycle=False, attrs={"label": "relspec"})
    evidence_idx: dict[str, int] = {}
    rule_idx: dict[str, int] = {}
    for name in sorted(evidence_names):
        evidence_idx[name] = graph.add_node(GraphNode("evidence", EvidenceNode(name)))
    for rule in sorted(rules, key=lambda item: item.name):
        rule_idx[rule.name] = graph.add_node(GraphNode("rule", rule))
    for rule in rules:
        rule_node_idx = rule_idx[rule.name]
        rule_required = required_columns.get(rule.name, {})
        for source in rule.sources:
            source_idx = evidence_idx[source]
            graph.add_edge(
                source_idx,
                rule_node_idx,
                GraphEdge(
                    kind="requires",
                    name=source,
                    required_columns=tuple(rule_required.get(source, ())),
                    inferred=True,
                    plan_fingerprint=fingerprints.get(rule.name),
                ),
            )
        output_idx = evidence_idx[rule.output]
        graph.add_edge(
            rule_node_idx,
            output_idx,
            GraphEdge(
                kind="produces",
                name=rule.output,
                inferred=True,
                plan_fingerprint=fingerprints.get(rule.name),
            ),
        )
    return RuleGraph(
        graph=graph,
        evidence_idx=evidence_idx,
        rule_idx=rule_idx,
        output_policy=output_policy,
    )


def _collect_evidence_names(rules: Sequence[RuleNode]) -> set[str]:
    names: set[str] = set()
    for rule in rules:
        names.add(rule.output)
        names.update(rule.inputs)
    return names


def _validate_rule_names(rules: Sequence[RuleNode]) -> None:
    seen: set[str] = set()
    for rule in rules:
        if not rule.name:
            msg = "Rule names must be non-empty."
            raise RelspecValidationError(msg)
        if rule.name in seen:
            msg = f"Duplicate rule name: {rule.name!r}."
            raise RelspecValidationError(msg)
        seen.add(rule.name)


def _node_sort_key(node: GraphNode) -> str:
    if node.kind == "evidence":
        return f"0:{node.payload.name}"
    payload = node.payload
    if isinstance(payload, RuleNode):
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
    if not isinstance(payload, RuleNode):
        msg = "Expected RuleNode payload for rule graph node."
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
    "RuleGraph",
    "RuleGraphSnapshot",
    "RuleNode",
    "build_rule_graph_from_inferred_deps",
    "rule_graph_diagnostics",
    "rule_graph_signature",
    "rule_graph_snapshot",
]
