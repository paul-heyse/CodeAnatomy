"""Relationship rule graph compilation and evidence gating."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike
from arrowdsl.plan.plan import Plan, union_all_plans
from arrowdsl.schema.build import ConstExpr, FieldExpr
from relspec.compiler import RelationshipRuleCompiler
from relspec.contracts import RELATION_OUTPUT_NAME, relation_output_schema
from relspec.model import EvidenceSpec as RelationshipEvidenceSpec
from relspec.model import RelationshipRule
from relspec.rules.definitions import EvidenceSpec as CentralEvidenceSpec
from relspec.rules.evidence import EvidenceCatalog
from schema_spec.system import GLOBAL_SCHEMA_REGISTRY


@dataclass(frozen=True)
class RuleNode:
    """Node in the relationship rule graph."""

    name: str
    rule: RelationshipRule
    requires: tuple[str, ...] = ()


@dataclass(frozen=True)
class GraphPlan:
    """Compiled plan graph for relationship rules."""

    plan: Plan
    outputs: dict[str, Plan]


def compile_graph_plan(
    rules: Sequence[RelationshipRule],
    *,
    ctx: ExecutionContext,
    compiler: RelationshipRuleCompiler,
    evidence: EvidenceCatalog,
) -> GraphPlan:
    """Compile relationship rules into a graph-level plan.

    Parameters
    ----------
    rules:
        Relationship rules to compile.
    ctx:
        Execution context for plan compilation.
    compiler:
        Rule compiler with plan resolver.
    evidence:
        Evidence catalog used to order rules.

    Returns
    -------
    GraphPlan
        Graph-level plan and per-output subplans.
    """
    work = evidence.clone()
    outputs: dict[str, list[Plan]] = {}
    for rule in order_rules(rules, evidence=work):
        compiled = compiler.compile_rule(rule, ctx=ctx)
        if compiled.plan is not None and not compiled.post_kernels:
            plan = _apply_rule_meta(compiled.plan, rule=rule, ctx=ctx)
        else:
            table = compiled.execute(ctx=ctx, resolver=compiler.resolver)
            plan = Plan.table_source(table, label=rule.name)
        outputs.setdefault(rule.output_dataset, []).append(plan)
        work.register(rule.output_dataset, plan.schema(ctx=ctx))
    merged: dict[str, Plan] = {}
    for output, plans in outputs.items():
        if len(plans) == 1:
            merged[output] = plans[0]
            continue
        merged[output] = union_all_plans(plans, label=output)
    union = union_all_plans(list(merged.values()), label="relspec_graph")
    return GraphPlan(plan=union, outputs=merged)


def _apply_rule_meta(plan: Plan, *, rule: RelationshipRule, ctx: ExecutionContext) -> Plan:
    if not rule.emit_rule_meta:
        return plan
    schema = plan.schema(ctx=ctx)
    names = list(schema.names)
    exprs = [FieldExpr(name=name).to_expression() for name in names]
    if rule.rule_name_col not in names:
        exprs.append(ConstExpr(value=rule.name).to_expression())
        names.append(rule.rule_name_col)
    if rule.rule_priority_col not in names:
        exprs.append(ConstExpr(value=int(rule.priority)).to_expression())
        names.append(rule.rule_priority_col)
    return plan.project(exprs, names, label=plan.label or rule.name)


def order_rules(
    rules: Sequence[RelationshipRule],
    *,
    evidence: EvidenceCatalog,
) -> list[RelationshipRule]:
    """Return rules ordered by dependency and priority.

    Returns
    -------
    list[RelationshipRule]
        Ordered, evidence-eligible rules.

    Raises
    ------
    ValueError
        Raised when the rule dependency graph contains cycles.
    """
    work = evidence.clone()
    pending = list(rules)
    resolved: list[RelationshipRule] = []
    while pending:
        ready = _ready_rules(pending, work)
        if not ready:
            missing = sorted({rule.name for rule in pending})
            msg = f"Relationship rule graph cannot resolve evidence for: {missing}"
            raise ValueError(msg)

        ready_sorted = sorted(ready, key=lambda rule: (rule.priority, rule.name))
        for rule in ready_sorted:
            resolved.append(rule)
            pending.remove(rule)
            _register_rule_output(work, rule)
    return resolved


def _ready_rules(
    pending: Sequence[RelationshipRule], evidence: EvidenceCatalog
) -> list[RelationshipRule]:
    ready: list[RelationshipRule] = []
    for rule in pending:
        inputs = tuple(ref.name for ref in rule.inputs)
        if evidence.satisfies(_central_evidence(rule.evidence), inputs=inputs):
            ready.append(rule)
    return ready


def _register_rule_output(evidence: EvidenceCatalog, rule: RelationshipRule) -> None:
    output_schema = _virtual_output_schema(rule)
    if output_schema is not None:
        evidence.register(rule.output_dataset, output_schema)
    else:
        evidence.sources.add(rule.output_dataset)


def _virtual_output_schema(rule: RelationshipRule) -> SchemaLike | None:
    if rule.contract_name:
        dataset_spec = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(rule.contract_name)
        if dataset_spec is not None:
            return dataset_spec.schema()
        if rule.contract_name == RELATION_OUTPUT_NAME:
            return relation_output_schema()
    return None


def _central_evidence(
    spec: RelationshipEvidenceSpec | None,
) -> CentralEvidenceSpec | None:
    if spec is None:
        return None
    return CentralEvidenceSpec(
        sources=spec.sources,
        required_columns=spec.required_columns,
        required_types=spec.required_types,
    )


__all__ = ["EvidenceCatalog", "GraphPlan", "RuleNode", "compile_graph_plan", "order_rules"]
