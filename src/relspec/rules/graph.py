"""Graph plan helpers for centralized rule execution."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from arrowdsl.core.interop import SchemaLike
from arrowdsl.plan.plan import Plan, union_all_plans
from relspec.rules.definitions import EvidenceSpec
from relspec.rules.evidence import EvidenceCatalog


@dataclass(frozen=True)
class GraphPlan:
    """Graph-level plan with per-output subplans."""

    plan: Plan
    outputs: dict[str, Plan]


@dataclass(frozen=True)
class RuleSelectors[RuleT]:
    """Selection helpers for generic rule ordering."""

    inputs_for: Callable[[RuleT], Sequence[str]]
    output_for: Callable[[RuleT], str]
    name_for: Callable[[RuleT], str]
    priority_for: Callable[[RuleT], int]
    evidence_for: Callable[[RuleT], EvidenceSpec | None]
    output_schema_for: Callable[[RuleT], SchemaLike | None] | None = None


def compile_graph_plan[RuleT](
    rules: Sequence[RuleT],
    *,
    plans: dict[str, Plan],
    output_for: Callable[[RuleT], str],
    label: str,
) -> GraphPlan:
    """Compile a graph-level plan from ordered rules.

    Returns
    -------
    GraphPlan
        Graph-level plan with per-output subplans.
    """
    ordered_outputs = [output_for(rule) for rule in rules if output_for(rule) in plans]
    outputs = {name: plans[name] for name in ordered_outputs}
    union = union_all_plans(tuple(outputs.values()), label=label)
    return GraphPlan(plan=union, outputs=outputs)


def order_rules_by_evidence[RuleT](
    rules: Sequence[RuleT],
    *,
    evidence: EvidenceCatalog,
    selectors: RuleSelectors[RuleT],
    allow_fallback: bool = True,
    label: str = "Rule",
) -> list[RuleT]:
    """Return rules ordered by dependency and priority.

    Returns
    -------
    list[RuleT]
        Ordered, evidence-eligible rules.

    Raises
    ------
    ValueError
        Raised when the rule dependency graph contains cycles.
    """
    schema_for = selectors.output_schema_for or (lambda _: None)
    work = evidence.clone()
    pending = list(rules)
    chosen_outputs: set[str] = set()
    resolved: list[RuleT] = []
    while pending:
        ready = [
            rule
            for rule in pending
            if selectors.output_for(rule) not in chosen_outputs
            and work.satisfies(
                selectors.evidence_for(rule),
                inputs=selectors.inputs_for(rule),
            )
        ]
        if allow_fallback:
            ready = _select_by_output(
                ready,
                selectors.output_for,
                selectors.priority_for,
                selectors.name_for,
            )
        if not ready:
            missing = sorted(selectors.name_for(rule) for rule in pending)
            msg = f"{label} graph cannot resolve evidence for: {missing}"
            raise ValueError(msg)
        ready_sorted = sorted(
            ready,
            key=lambda rule: (
                selectors.priority_for(rule),
                selectors.name_for(rule),
            ),
        )
        for rule in ready_sorted:
            resolved.append(rule)
            output_name = selectors.output_for(rule)
            chosen_outputs.add(output_name)
            _register_output(work, output_name, schema_for(rule))
        pending = [
            rule
            for rule in pending
            if rule not in ready_sorted and selectors.output_for(rule) not in chosen_outputs
        ]
    return resolved


def rule_graph_signature[RuleT](
    rules: Sequence[RuleT],
    *,
    name_for: Callable[[RuleT], str],
    signature_for: Callable[[RuleT], str],
    label: str,
) -> str:
    """Return a stable signature for a rule graph.

    Returns
    -------
    str
        Deterministic hash for the rule graph.
    """
    entries = sorted((name_for(rule), signature_for(rule)) for rule in rules)
    payload = {"label": label, "rules": entries}
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _register_output(evidence: EvidenceCatalog, name: str, schema: SchemaLike | None) -> None:
    """Register a rule output in the evidence catalog.

    Parameters
    ----------
    evidence
        Evidence catalog to update.
    name
        Output dataset name.
    schema
        Optional schema for the output dataset.
    """
    if schema is None:
        evidence.sources.add(name)
        return
    evidence.register(name, schema)


def _select_by_output[RuleT](
    rules: Sequence[RuleT],
    output_for: Callable[[RuleT], str],
    priority_for: Callable[[RuleT], int],
    name_for: Callable[[RuleT], str],
) -> list[RuleT]:
    """Select one rule per output based on priority and name.

    Parameters
    ----------
    rules
        Rules to select from.
    output_for
        Function mapping a rule to its output name.
    priority_for
        Function mapping a rule to its priority.
    name_for
        Function mapping a rule to its name.

    Returns
    -------
    list[RuleT]
        Selected rules, one per output.
    """
    selected: dict[str, RuleT] = {}
    for rule in rules:
        output = output_for(rule)
        existing = selected.get(output)
        if existing is None:
            selected[output] = rule
            continue
        if (priority_for(rule), name_for(rule)) < (
            priority_for(existing),
            name_for(existing),
        ):
            selected[output] = rule
    return list(selected.values())


__all__ = [
    "GraphPlan",
    "RuleSelectors",
    "compile_graph_plan",
    "order_rules_by_evidence",
    "rule_graph_signature",
]
