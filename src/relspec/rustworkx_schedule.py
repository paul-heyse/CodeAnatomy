"""Rustworkx scheduling helpers for rule graphs."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import rustworkx as rx

from relspec.errors import RelspecValidationError
from relspec.rustworkx_graph import EvidenceNode, RuleGraph, RuleNode
from relspec.rules.evidence import EvidenceCatalog

if TYPE_CHECKING:
    from arrowdsl.core.interop import SchemaLike


@dataclass(frozen=True)
class RuleSchedule:
    """Deterministic schedule for rule execution."""

    ordered_rules: tuple[str, ...]
    generations: tuple[tuple[str, ...], ...]
    missing_rules: tuple[str, ...] = ()


def schedule_rules(
    graph: RuleGraph,
    *,
    evidence: EvidenceCatalog,
    output_schema_for: Callable[[str], SchemaLike | None] | None = None,
    allow_partial: bool = False,
) -> RuleSchedule:
    """Return a deterministic rule schedule driven by a rule graph."""
    working = evidence.clone()
    seed_nodes = _seed_nodes(graph, working.sources)
    sorter = _make_sorter(graph, seed_nodes=seed_nodes)
    ordered: list[str] = []
    generations: list[tuple[str, ...]] = []
    visited_rules: set[str] = set()
    while sorter.is_active():
        ready = list(sorter.get_ready())
        if not ready:
            break
        ready_rules = _sorted_ready_rules(graph, ready)
        ready_evidence = _ready_evidence_nodes(graph, ready)
        for rule in ready_rules:
            if not working.satisfies(rule.evidence, inputs=rule.inputs):
                msg = f"Rule graph cannot resolve evidence for: {rule.name!r}."
                raise RelspecValidationError(msg)
            ordered.append(rule.name)
            visited_rules.add(rule.name)
        _register_ready_evidence(
            graph,
            ready_evidence,
            evidence=working,
            output_schema_for=output_schema_for,
        )
        sorter.done([*ready_evidence, *[graph.rule_idx[rule.name] for rule in ready_rules]])
        if ready_rules:
            generations.append(tuple(rule.name for rule in ready_rules))
    missing = tuple(sorted(set(graph.rule_idx) - visited_rules))
    if missing and not allow_partial:
        msg = f"Rule graph cannot resolve evidence for: {list(missing)}."
        raise RelspecValidationError(msg)
    return RuleSchedule(
        ordered_rules=tuple(ordered),
        generations=tuple(generations),
        missing_rules=missing,
    )


def impacted_rules(
    graph: RuleGraph,
    *,
    evidence_name: str,
) -> tuple[str, ...]:
    """Return rule names impacted by a changed evidence dataset."""
    node = graph.evidence_idx.get(evidence_name)
    if node is None:
        return ()
    impacted = rx.descendants(graph.graph, node)
    names = [
        graph.graph[idx].payload.name
        for idx in impacted
        if graph.graph[idx].kind == "rule"
    ]
    return tuple(sorted(set(names)))


def provenance_for_rule(
    graph: RuleGraph,
    *,
    rule_name: str,
) -> tuple[str, ...]:
    """Return evidence names that contribute to a rule."""
    node = graph.rule_idx.get(rule_name)
    if node is None:
        return ()
    ancestors = rx.ancestors(graph.graph, node)
    names = [
        graph.graph[idx].payload.name
        for idx in ancestors
        if graph.graph[idx].kind == "evidence"
    ]
    return tuple(sorted(set(names)))


def _seed_nodes(graph: RuleGraph, seed_evidence: Iterable[str]) -> list[int]:
    nodes: list[int] = []
    for name in seed_evidence:
        idx = graph.evidence_idx.get(name)
        if idx is not None:
            nodes.append(idx)
    return nodes


def _make_sorter(graph: RuleGraph, *, seed_nodes: list[int]) -> rx.TopologicalSorter:
    try:
        return rx.TopologicalSorter(graph.graph, check_cycle=True, initial=seed_nodes)
    except (TypeError, ValueError) as exc:
        msg = "Rule graph contains a cycle or invalid dependency."
        raise RelspecValidationError(msg) from exc


def _sorted_ready_rules(graph: RuleGraph, ready: Iterable[int]) -> list[RuleNode]:
    rules: list[RuleNode] = []
    for idx in ready:
        node = graph.graph[idx]
        if node.kind != "rule":
            continue
        payload = node.payload
        if not isinstance(payload, RuleNode):
            msg = "Expected RuleNode payload for rule graph node."
            raise TypeError(msg)
        rules.append(payload)
    rules.sort(key=lambda rule: (rule.priority, rule.name))
    return rules


def _ready_evidence_nodes(graph: RuleGraph, ready: Iterable[int]) -> list[int]:
    nodes: list[int] = []
    for idx in ready:
        node = graph.graph[idx]
        if node.kind != "evidence":
            continue
        payload = node.payload
        if not isinstance(payload, EvidenceNode):
            msg = "Expected EvidenceNode payload for evidence graph node."
            raise TypeError(msg)
        nodes.append(idx)
    return nodes


def _register_ready_evidence(
    graph: RuleGraph,
    ready: Iterable[int],
    *,
    evidence: EvidenceCatalog,
    output_schema_for: Callable[[str], SchemaLike | None] | None,
) -> None:
    for idx in ready:
        node = graph.graph[idx]
        if node.kind != "evidence":
            continue
        payload = node.payload
        if not isinstance(payload, EvidenceNode):
            msg = "Expected EvidenceNode payload for evidence graph node."
            raise TypeError(msg)
        name = payload.name
        if name in evidence.sources:
            continue
        if not _has_rule_predecessor(graph, idx):
            continue
        schema = output_schema_for(name) if output_schema_for is not None else None
        if schema is None:
            evidence.sources.add(name)
            continue
        evidence.register(name, schema)


def _has_rule_predecessor(graph: RuleGraph, node_idx: int) -> bool:
    for pred_idx in graph.graph.predecessor_indices(node_idx):
        pred = graph.graph[pred_idx]
        if pred.kind == "rule":
            return True
    return False


__all__ = [
    "RuleSchedule",
    "impacted_rules",
    "provenance_for_rule",
    "schedule_rules",
]
