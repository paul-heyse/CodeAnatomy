"""Graph plan helpers for centralized rule execution."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.core.context import Ordering
from arrowdsl.core.interop import SchemaLike
from ibis_engine.expr_compiler import union_tables
from ibis_engine.plan import IbisPlan
from registry_common.arrow_payloads import payload_hash
from relspec.rules.definitions import EvidenceSpec
from relspec.rules.evidence import EvidenceCatalog

RULE_GRAPH_SIGNATURE_VERSION = 1
_RULE_GRAPH_RULE_SCHEMA = pa.struct(
    [
        pa.field("name", pa.string()),
        pa.field("signature", pa.string()),
    ]
)
_RULE_GRAPH_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("label", pa.string()),
        pa.field("rules", pa.list_(_RULE_GRAPH_RULE_SCHEMA)),
    ]
)


@dataclass(frozen=True)
class GraphPlan:
    """Graph-level plan with per-output subplans."""

    plan: IbisPlan
    outputs: dict[str, IbisPlan]


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
    plans: dict[str, IbisPlan],
    output_for: Callable[[RuleT], str],
    label: str,
) -> GraphPlan:
    """Compile a graph-level plan from ordered rules.

    Returns
    -------
    GraphPlan
        Graph-level plan with per-output subplans.

    Raises
    ------
    ValueError
        Raised when there are no output plans available.
    """
    ordered_outputs = [output_for(rule) for rule in rules if output_for(rule) in plans]
    outputs = {name: plans[name] for name in ordered_outputs}
    if not outputs:
        msg = f"{label} requires at least one output plan."
        raise ValueError(msg)
    expr = union_tables([plan.expr for plan in outputs.values()], distinct=False)
    union = IbisPlan(expr=expr, ordering=Ordering.unordered())
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
    entries = [
        {"name": name, "signature": signature}
        for name, signature in sorted((name_for(rule), signature_for(rule)) for rule in rules)
    ]
    payload = {
        "version": RULE_GRAPH_SIGNATURE_VERSION,
        "label": label,
        "rules": entries,
    }
    return payload_hash(payload, _RULE_GRAPH_SCHEMA)


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
