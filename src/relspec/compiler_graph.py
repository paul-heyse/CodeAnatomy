"""Relationship rule graph compilation and evidence gating."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import cast

import ibis
import pyarrow as pa
from ibis.expr.datatypes import DataType
from ibis.expr.types import Table as IbisTable
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.context import ExecutionContext, Ordering
from arrowdsl.core.interop import SchemaLike
from ibis_engine.plan import IbisPlan
from relspec.compiler import RelationshipRuleCompiler
from relspec.contracts import RELATION_OUTPUT_NAME, relation_output_schema
from relspec.engine import IbisRelPlanCompiler
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

    plan: IbisPlan
    outputs: dict[str, IbisPlan]


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
    plan_compiler = compiler.plan_compiler or IbisRelPlanCompiler()
    outputs: dict[str, list[IbisPlan]] = {}
    for rule in order_rules(rules, evidence=work):
        compiled = compiler.compile_rule(rule, ctx=ctx)
        if compiled.rel_plan is not None and not compiled.post_kernels:
            plan = plan_compiler.compile(compiled.rel_plan, ctx=ctx, resolver=compiler.resolver)
        else:
            table = compiled.execute(ctx=ctx, resolver=compiler.resolver, compiler=plan_compiler)
            plan = IbisPlan(expr=ibis.memtable(table), ordering=Ordering.unordered())
        outputs.setdefault(rule.output_dataset, []).append(plan)
        work.register(rule.output_dataset, plan.expr.schema().to_pyarrow())
    merged: dict[str, IbisPlan] = {}
    for output, plans in outputs.items():
        if len(plans) == 1:
            merged[output] = plans[0]
            continue
        merged[output] = _union_plans(plans, label=output)
    union = _union_plans(list(merged.values()), label="relspec_graph")
    return GraphPlan(plan=union, outputs=merged)


def _union_plans(plans: Sequence[IbisPlan], *, label: str) -> IbisPlan:
    if not plans:
        _ = label
        empty = ibis.memtable(pa.table({}))
        return IbisPlan(expr=empty, ordering=Ordering.unordered())
    aligned = _align_union_tables([plan.expr for plan in plans])
    unioned = aligned[0]
    for table in aligned[1:]:
        unioned = unioned.union(table)
    return IbisPlan(expr=unioned, ordering=Ordering.unordered())


def _align_union_tables(tables: Sequence[IbisTable]) -> list[IbisTable]:
    names: list[str] = []
    types: dict[str, DataType] = {}
    for table in tables:
        schema = table.schema()
        schema_names = cast("Sequence[str]", schema.names)
        schema_types = cast("Sequence[DataType]", schema.types)
        for name, dtype in zip(schema_names, schema_types, strict=True):
            if name in types:
                continue
            names.append(name)
            types[name] = dtype
    aligned: list[IbisTable] = []
    for table in tables:
        cols: list[IbisValue] = []
        for name in names:
            if name in table.columns:
                cols.append(table[name])
            else:
                cols.append(ibis.literal(None, type=types[name]).name(name))
        aligned.append(table.select(cols))
    return aligned


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
