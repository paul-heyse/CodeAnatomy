"""Rustworkx-backed rule graph helpers for relspec scheduling."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import pyarrow as pa
import rustworkx as rx

from arrowdsl.io.ipc import payload_hash
from relspec.errors import RelspecValidationError
from relspec.rules.definitions import EvidenceSpec as RuleEvidenceSpec

if TYPE_CHECKING:
    from normalize.runner import ResolvedNormalizeRule
    from relspec.model import EvidenceSpec as RelationshipEvidenceSpec
    from relspec.model import RelationshipRule
    from relspec.rules.definitions import RuleDefinition

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
    """Rule node payload."""

    name: str
    output: str
    inputs: tuple[str, ...]
    sources: tuple[str, ...]
    priority: int
    evidence: RuleEvidenceSpec | None = None


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


def build_rule_graph_from_definitions(
    rules: Sequence[RuleDefinition],
    *,
    output_policy: OutputPolicy = "all_producers",
) -> RuleGraph:
    """Build a rule graph from centralized rule definitions.

    Returns
    -------
    RuleGraph
        Graph with rule and evidence nodes.
    """
    rule_nodes = tuple(_rule_node_from_definition(rule) for rule in rules)
    return _build_rule_graph(rule_nodes, output_policy=output_policy)


def build_rule_graph_from_relationship_rules(
    rules: Sequence[RelationshipRule],
    *,
    output_policy: OutputPolicy = "all_producers",
) -> RuleGraph:
    """Build a rule graph from relationship rules.

    Returns
    -------
    RuleGraph
        Graph with rule and evidence nodes.
    """
    rule_nodes = tuple(_rule_node_from_relationship(rule) for rule in rules)
    return _build_rule_graph(rule_nodes, output_policy=output_policy)


def build_rule_graph_from_normalize_rules(
    rules: Sequence[ResolvedNormalizeRule],
    *,
    output_policy: OutputPolicy = "all_producers",
) -> RuleGraph:
    """Build a rule graph from resolved normalize rules.

    Returns
    -------
    RuleGraph
        Graph with rule and evidence nodes.
    """
    rule_nodes = tuple(_rule_node_from_normalize(rule) for rule in rules)
    return _build_rule_graph(rule_nodes, output_policy=output_policy)


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
        Snapshot payload for the rule graph.
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
        Stable signature string.
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
        Cycle detection and visualization payload.
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


def rule_graph_signature_from_definitions(
    rules: Sequence[RuleDefinition],
    *,
    label: str,
    output_policy: OutputPolicy = "all_producers",
    rule_signatures: Mapping[str, str] | None = None,
) -> str:
    """Return a stable rule graph signature for rule definitions.

    Returns
    -------
    str
        Stable signature string.
    """
    graph = build_rule_graph_from_definitions(rules, output_policy=output_policy)
    snapshot = rule_graph_snapshot(graph, label=label, rule_signatures=rule_signatures)
    return rule_graph_signature(snapshot)


def rule_graph_snapshot_from_definitions(
    rules: Sequence[RuleDefinition],
    *,
    label: str,
    output_policy: OutputPolicy = "all_producers",
    rule_signatures: Mapping[str, str] | None = None,
) -> RuleGraphSnapshot:
    """Return a rule graph snapshot for rule definitions.

    Returns
    -------
    RuleGraphSnapshot
        Snapshot payload for the rule graph.
    """
    graph = build_rule_graph_from_definitions(rules, output_policy=output_policy)
    return rule_graph_snapshot(graph, label=label, rule_signatures=rule_signatures)


def _build_rule_graph(
    rules: Sequence[RuleNode],
    *,
    output_policy: OutputPolicy,
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
    edges: list[tuple[int, int, GraphEdge]] = []
    for rule in rules:
        rule_node = rule_idx[rule.name]
        requirements = _edge_requirements(rule.evidence)
        edges.extend(
            [
                (
                    evidence_idx[source],
                    rule_node,
                    GraphEdge(
                        kind="requires",
                        name=source,
                        required_columns=requirements[0],
                        required_types=requirements[1],
                        required_metadata=requirements[2],
                    ),
                )
                for source in rule.sources
            ]
        )
        edges.append(
            (
                rule_node,
                evidence_idx[rule.output],
                GraphEdge(kind="produces", name=rule.output),
            )
        )
    graph.add_edges_from(edges)
    return RuleGraph(
        graph=graph,
        evidence_idx=evidence_idx,
        rule_idx=rule_idx,
        output_policy=output_policy,
    )


def _rule_node_from_definition(rule: RuleDefinition) -> RuleNode:
    inputs = tuple(rule.inputs)
    evidence = rule.evidence
    sources = evidence.sources if evidence is not None and evidence.sources else inputs
    return RuleNode(
        name=rule.name,
        output=rule.output,
        inputs=inputs,
        sources=sources,
        priority=rule.priority,
        evidence=evidence,
    )


def _rule_node_from_relationship(rule: RelationshipRule) -> RuleNode:
    inputs = tuple(ref.name for ref in rule.inputs)
    evidence = _relationship_evidence_spec(rule.evidence)
    sources = evidence.sources if evidence is not None and evidence.sources else inputs
    return RuleNode(
        name=rule.name,
        output=rule.output_dataset,
        inputs=inputs,
        sources=sources,
        priority=rule.priority,
        evidence=evidence,
    )


def _rule_node_from_normalize(rule: ResolvedNormalizeRule) -> RuleNode:
    inputs = tuple(rule.inputs)
    evidence = rule.evidence
    sources = evidence.sources if evidence is not None and evidence.sources else inputs
    return RuleNode(
        name=rule.name,
        output=rule.output,
        inputs=inputs,
        sources=sources,
        priority=rule.priority,
        evidence=evidence,
    )


def _relationship_evidence_spec(
    spec: RelationshipEvidenceSpec | None,
) -> RuleEvidenceSpec | None:
    if spec is None:
        return None
    return RuleEvidenceSpec(
        sources=spec.sources,
        required_columns=spec.required_columns,
        required_types=spec.required_types,
        required_metadata=spec.required_metadata,
    )


def _collect_evidence_names(rules: Sequence[RuleNode]) -> set[str]:
    names: set[str] = set()
    for rule in rules:
        names.update(rule.sources)
        names.add(rule.output)
    return names


def _edge_requirements(
    spec: RuleEvidenceSpec | None,
) -> tuple[tuple[str, ...], tuple[tuple[str, str], ...], tuple[tuple[bytes, bytes], ...]]:
    if spec is None:
        return (), (), ()
    required_columns = tuple(spec.required_columns)
    required_types = tuple(sorted(spec.required_types.items()))
    required_metadata = tuple(sorted(spec.required_metadata.items()))
    return required_columns, required_types, required_metadata


def _validate_rule_names(rules: Sequence[RuleNode]) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for rule in rules:
        if rule.name in seen:
            duplicates.add(rule.name)
        seen.add(rule.name)
    if duplicates:
        msg = f"Duplicate rule names in graph: {sorted(duplicates)}."
        raise RelspecValidationError(msg)


def _node_payload(
    node_id: int,
    node: GraphNode,
    *,
    signatures: Mapping[str, str],
) -> dict[str, object]:
    if node.kind == "evidence":
        evidence = node.payload
        if not isinstance(evidence, EvidenceNode):
            msg = "Expected EvidenceNode payload for evidence graph node."
            raise TypeError(msg)
        return {
            "id": node_id,
            "kind": node.kind,
            "name": evidence.name,
            "output": None,
            "priority": None,
            "signature": None,
        }
    rule = node.payload
    if not isinstance(rule, RuleNode):
        msg = "Expected RuleNode payload for rule graph node."
        raise TypeError(msg)
    return {
        "id": node_id,
        "kind": node.kind,
        "name": rule.name,
        "output": rule.output,
        "priority": rule.priority,
        "signature": signatures.get(rule.name),
    }


def _edge_payloads(graph: rx.PyDiGraph) -> list[dict[str, object]]:
    edges: list[dict[str, object]] = []
    for source, target, payload in graph.weighted_edge_list():
        if not isinstance(payload, GraphEdge):
            msg = "Expected GraphEdge payload for graph edge."
            raise TypeError(msg)
        edges.append(
            {
                "source": source,
                "target": target,
                "kind": payload.kind,
                "name": payload.name,
                "required_columns": list(payload.required_columns),
                "required_types": _edge_type_entries(payload.required_types),
                "required_metadata": _edge_metadata_entries(payload.required_metadata),
            }
        )
    edges.sort(key=lambda item: (item["source"], item["target"], item["kind"], item["name"]))
    return edges


def _edge_type_entries(
    entries: tuple[tuple[str, str], ...],
) -> list[dict[str, str]]:
    return [{"name": name, "type": dtype} for name, dtype in entries]


def _edge_metadata_entries(
    entries: tuple[tuple[bytes, bytes], ...],
) -> list[dict[str, bytes]]:
    return [{"key": key, "value": value} for key, value in entries]


def _node_sort_key(node: GraphNode) -> str:
    payload = node.payload
    if isinstance(payload, EvidenceNode):
        return f"evidence:{payload.name}"
    if isinstance(payload, RuleNode):
        return f"rule:{payload.name}"
    return node.kind


__all__ = [
    "RULE_GRAPH_SNAPSHOT_VERSION",
    "GraphDiagnostics",
    "GraphEdge",
    "GraphNode",
    "NodeKind",
    "OutputPolicy",
    "RuleGraph",
    "RuleGraphSnapshot",
    "RuleNode",
    "build_rule_graph_from_definitions",
    "build_rule_graph_from_normalize_rules",
    "build_rule_graph_from_relationship_rules",
    "rule_graph_diagnostics",
    "rule_graph_signature",
    "rule_graph_signature_from_definitions",
    "rule_graph_snapshot",
    "rule_graph_snapshot_from_definitions",
]
