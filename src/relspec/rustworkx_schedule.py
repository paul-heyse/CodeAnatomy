"""Rustworkx scheduling helpers for rule graphs."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import rustworkx as rx

from relspec.errors import RelspecValidationError
from relspec.evidence import EvidenceCatalog
from relspec.graph_edge_validation import validate_edge_requirements
from relspec.rustworkx_graph import EvidenceNode, RuleGraph, RuleNode
from relspec.schedule_events import RuleScheduleMetadata

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
    """Return a deterministic rule schedule driven by a rule graph.

    Returns
    -------
    RuleSchedule
        Ordered rule names and generation waves.

    Raises
    ------
    RelspecValidationError
        Raised when evidence requirements cannot be satisfied.
    """
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
        valid_rules: list[RuleNode] = []
        for rule in ready_rules:
            rule_idx = graph.rule_idx.get(rule.name)
            if rule_idx is None:
                continue
            if not validate_edge_requirements(graph, rule_idx, catalog=working):
                continue
            ordered.append(rule.name)
            visited_rules.add(rule.name)
            valid_rules.append(rule)
        _register_ready_evidence(
            graph,
            ready_evidence,
            evidence=working,
            output_schema_for=output_schema_for,
        )
        sorter.done([*ready_evidence, *[graph.rule_idx[rule.name] for rule in valid_rules]])
        if valid_rules:
            generations.append(tuple(rule.name for rule in valid_rules))
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
    """Return rule names impacted by a changed evidence dataset.

    Returns
    -------
    tuple[str, ...]
        Sorted impacted rule names.
    """
    node = graph.evidence_idx.get(evidence_name)
    if node is None:
        return ()
    impacted = rx.descendants(graph.graph, node)
    names = [graph.graph[idx].payload.name for idx in impacted if graph.graph[idx].kind == "rule"]
    return tuple(sorted(set(names)))


def impacted_rules_for_evidence(
    graph: RuleGraph,
    *,
    evidence_names: Iterable[str],
) -> tuple[str, ...]:
    """Return impacted rules for multiple evidence datasets.

    Returns
    -------
    tuple[str, ...]
        Sorted impacted rule names.
    """
    impacted: set[str] = set()
    for name in evidence_names:
        impacted.update(impacted_rules(graph, evidence_name=name))
    return tuple(sorted(impacted))


def provenance_for_rule(
    graph: RuleGraph,
    *,
    rule_name: str,
) -> tuple[str, ...]:
    """Return evidence names that contribute to a rule.

    Returns
    -------
    tuple[str, ...]
        Sorted evidence names contributing to the rule.
    """
    node = graph.rule_idx.get(rule_name)
    if node is None:
        return ()
    ancestors = rx.ancestors(graph.graph, node)
    names = [
        graph.graph[idx].payload.name for idx in ancestors if graph.graph[idx].kind == "evidence"
    ]
    return tuple(sorted(set(names)))


def rule_schedule_metadata(
    schedule: RuleSchedule,
) -> dict[str, RuleScheduleMetadata]:
    """Return per-rule schedule metadata for execution events.

    Returns
    -------
    dict[str, RuleScheduleMetadata]
        Mapping of rule names to schedule metadata.
    """
    order_index = {name: idx for idx, name in enumerate(schedule.ordered_rules)}
    mapping: dict[str, RuleScheduleMetadata] = {}
    for gen_idx, generation in enumerate(schedule.generations):
        generation_size = len(generation)
        for gen_order, name in enumerate(generation):
            schedule_index = order_index.get(name)
            if schedule_index is None:
                continue
            mapping[name] = RuleScheduleMetadata(
                schedule_index=schedule_index,
                generation_index=gen_idx,
                generation_order=gen_order,
                generation_size=generation_size,
            )
    return mapping


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
    "impacted_rules_for_evidence",
    "provenance_for_rule",
    "rule_schedule_metadata",
    "schedule_rules",
]
