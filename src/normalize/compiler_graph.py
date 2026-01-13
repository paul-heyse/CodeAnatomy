"""Normalize rule graph compilation and evidence gating."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from normalize.evidence_catalog import EvidenceCatalog
from normalize.rule_model import NormalizeRule


@dataclass(frozen=True)
class RuleNode:
    """Node in the normalize rule graph."""

    name: str
    rule: NormalizeRule
    requires: tuple[str, ...] = ()


def order_rules(
    rules: Sequence[NormalizeRule],
    *,
    evidence: EvidenceCatalog,
    allow_fallback: bool = True,
) -> list[NormalizeRule]:
    """Return rules ordered by dependency and priority.

    Returns
    -------
    list[NormalizeRule]
        Ordered, evidence-eligible rules.

    Raises
    ------
    ValueError
        Raised when the rule dependency graph contains cycles.
    """
    eligible = [rule for rule in rules if evidence.satisfies(rule.evidence, inputs=rule.inputs)]
    if allow_fallback:
        eligible = _select_by_output(eligible)

    output_names = {rule.output for rule in eligible}
    nodes: list[RuleNode] = []
    for rule in eligible:
        deps = sorted({name for name in rule.inputs if name in output_names})
        nodes.append(RuleNode(name=rule.name, rule=rule, requires=tuple(deps)))

    remaining = {node.name: node for node in nodes}
    resolved: list[NormalizeRule] = []
    while remaining:
        ready = [
            node
            for node in remaining.values()
            if all(dep not in remaining for dep in node.requires)
        ]
        if not ready:
            msg = f"Normalize rule graph contains cycles: {sorted(remaining)}"
            raise ValueError(msg)
        ready_sorted = sorted(ready, key=lambda node: (node.rule.priority, node.rule.name))
        for node in ready_sorted:
            resolved.append(node.rule)
            remaining.pop(node.name, None)
    return resolved


def _select_by_output(rules: Sequence[NormalizeRule]) -> list[NormalizeRule]:
    selected: dict[str, NormalizeRule] = {}
    for rule in rules:
        existing = selected.get(rule.output)
        if existing is None:
            selected[rule.output] = rule
            continue
        if (rule.priority, rule.name) < (existing.priority, existing.name):
            selected[rule.output] = rule
    return list(selected.values())


__all__ = ["RuleNode", "order_rules"]
