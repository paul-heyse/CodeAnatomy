"""Normalize rule graph compilation and evidence gating."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from arrowdsl.core.interop import SchemaLike
from arrowdsl.plan.plan import Plan, union_all_plans
from normalize.evidence_catalog import EvidenceCatalog
from normalize.registry_specs import dataset_schema
from normalize.rule_model import NormalizeRule


@dataclass(frozen=True)
class RuleNode:
    """Node in the normalize rule graph."""

    name: str
    rule: NormalizeRule
    requires: tuple[str, ...] = ()


@dataclass(frozen=True)
class NormalizeGraphPlan:
    """Compiled normalize plan graph."""

    plan: Plan
    outputs: dict[str, Plan]


def compile_graph_plan(
    rules: Sequence[NormalizeRule],
    *,
    plans: dict[str, Plan],
) -> NormalizeGraphPlan:
    """Compile a graph-level plan from rule outputs.

    Returns
    -------
    NormalizeGraphPlan
        Graph-level plan and per-output subplans.
    """
    ordered_outputs = [rule.output for rule in rules if rule.output in plans]
    outputs = {name: plans[name] for name in ordered_outputs}
    union = union_all_plans(tuple(outputs.values()), label="normalize_graph")
    return NormalizeGraphPlan(plan=union, outputs=outputs)


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
    work = evidence.clone()
    pending = list(rules)
    chosen_outputs: set[str] = set()
    resolved: list[NormalizeRule] = []
    while pending:
        ready = [
            rule
            for rule in pending
            if rule.output not in chosen_outputs
            and work.satisfies(rule.evidence, inputs=rule.inputs)
        ]
        if allow_fallback:
            ready = _select_by_output(ready)
        if not ready:
            missing = sorted(rule.name for rule in pending)
            msg = f"Normalize rule graph cannot resolve evidence for: {missing}"
            raise ValueError(msg)
        ready_sorted = sorted(ready, key=lambda rule: (rule.priority, rule.name))
        for rule in ready_sorted:
            resolved.append(rule)
            chosen_outputs.add(rule.output)
            _register_rule_output(work, rule)
        pending = [
            rule
            for rule in pending
            if rule not in ready_sorted and rule.output not in chosen_outputs
        ]
    return resolved


def _register_rule_output(evidence: EvidenceCatalog, rule: NormalizeRule) -> None:
    output_schema = _virtual_output_schema(rule)
    if output_schema is None:
        evidence.sources.add(rule.output)
        return
    evidence.register(rule.output, output_schema)


def _virtual_output_schema(rule: NormalizeRule) -> SchemaLike | None:
    try:
        return dataset_schema(rule.output)
    except KeyError:
        return None


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


__all__ = ["NormalizeGraphPlan", "RuleNode", "compile_graph_plan", "order_rules"]
