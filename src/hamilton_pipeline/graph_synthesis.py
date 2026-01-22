"""Helpers for synthesizing Hamilton-compatible dependency maps."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from relspec.rustworkx_graph import EvidenceNode, RuleGraph, RuleNode


@dataclass(frozen=True)
class DependencyDiff:
    """Difference summary between two dependency maps."""

    missing: Mapping[str, tuple[str, ...]]
    extra: Mapping[str, tuple[str, ...]]
    mismatched: Mapping[str, tuple[tuple[str, ...], tuple[str, ...]]]


def dependency_map(graph: RuleGraph) -> Mapping[str, tuple[str, ...]]:
    """Return rule -> evidence dependency map derived from a rule graph."""
    mapping: dict[str, tuple[str, ...]] = {}
    for rule_name, node_idx in graph.rule_idx.items():
        deps: list[str] = []
        for pred_idx in graph.graph.predecessor_indices(node_idx):
            pred = graph.graph[pred_idx]
            if pred.kind != "evidence":
                continue
            payload = pred.payload
            if not isinstance(payload, EvidenceNode):
                msg = "Expected EvidenceNode payload for evidence graph node."
                raise TypeError(msg)
            deps.append(payload.name)
        mapping[rule_name] = tuple(sorted(set(deps)))
    return mapping


def rule_output_map(graph: RuleGraph) -> Mapping[str, str]:
    """Return rule -> output dataset map derived from a rule graph."""
    mapping: dict[str, str] = {}
    for rule_name, node_idx in graph.rule_idx.items():
        node = graph.graph[node_idx]
        payload = node.payload
        if not isinstance(payload, RuleNode):
            msg = "Expected RuleNode payload for rule graph node."
            raise TypeError(msg)
        mapping[rule_name] = payload.output
    return mapping


def compare_dependency_maps(
    expected: Mapping[str, Sequence[str]],
    actual: Mapping[str, Sequence[str]],
) -> DependencyDiff:
    """Return a diff between two dependency maps."""
    missing = {
        name: _sorted_deps(expected[name])
        for name in sorted(expected)
        if name not in actual
    }
    extra = {name: _sorted_deps(actual[name]) for name in sorted(actual) if name not in expected}
    mismatched: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {}
    for name in sorted(set(expected) & set(actual)):
        exp = _sorted_deps(expected[name])
        act = _sorted_deps(actual[name])
        if exp != act:
            mismatched[name] = (exp, act)
    return DependencyDiff(missing=missing, extra=extra, mismatched=mismatched)


def _sorted_deps(values: Sequence[str]) -> tuple[str, ...]:
    return tuple(sorted(set(values)))


__all__ = ["DependencyDiff", "compare_dependency_maps", "dependency_map", "rule_output_map"]
