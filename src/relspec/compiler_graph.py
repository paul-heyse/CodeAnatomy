"""Relationship rule graph compilation and evidence gating."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import ibis
import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import Table as IbisTable
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.context import ExecutionContext, Ordering, OrderingLevel
from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.plan.schema_utils import plan_schema
from datafusion_engine.runtime import AdapterExecutionPolicy, ExecutionLabel
from ibis_engine.execution import IbisExecutionContext, materialize_ibis_plan
from ibis_engine.expr_compiler import align_set_op_tables, union_tables
from ibis_engine.plan import IbisPlan
from relspec.compiler import RelationshipRuleCompiler, RuleExecutionOptions
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


@dataclass(frozen=True)
class GraphExecutionOptions:
    """Execution options for graph-level plan materialization."""

    execution_policy: AdapterExecutionPolicy | None = None
    ibis_backend: BaseBackend | None = None
    params: Mapping[IbisValue, object] | None = None


def compile_graph_plan(
    rules: Sequence[RelationshipRule],
    *,
    ctx: ExecutionContext,
    compiler: RelationshipRuleCompiler,
    evidence: EvidenceCatalog,
    execution: GraphExecutionOptions | None = None,
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
    execution:
        Execution options for adapterized plan materialization.

    Returns
    -------
    GraphPlan
        Graph-level plan and per-output subplans.
    """
    work = evidence.clone()
    plan_compiler = compiler.plan_compiler or IbisRelPlanCompiler()
    execution = execution or GraphExecutionOptions()

    def _plan_executor(
        plan: IbisPlan,
        exec_ctx: ExecutionContext,
        exec_params: Mapping[IbisValue, object] | None,
        execution_label: ExecutionLabel | None = None,
    ) -> TableLike:
        resolved_params = exec_params if exec_params is not None else execution.params
        ibis_execution = IbisExecutionContext(
            ctx=exec_ctx,
            execution_policy=execution.execution_policy,
            execution_label=execution_label,
            ibis_backend=execution.ibis_backend,
            params=resolved_params,
        )
        return materialize_ibis_plan(plan, execution=ibis_execution)

    outputs: dict[str, list[IbisPlan]] = {}
    for rule in order_rules(rules, evidence=work):
        compiled = compiler.compile_rule(rule, ctx=ctx)
        if compiled.rel_plan is not None and not compiled.post_kernels:
            plan = plan_compiler.compile(
                compiled.rel_plan,
                ctx=ctx,
                resolver=compiler.resolver,
            )
        else:
            table = compiled.execute(
                ctx=ctx,
                resolver=compiler.resolver,
                compiler=plan_compiler,
                options=RuleExecutionOptions(
                    params=execution.params,
                    plan_executor=_plan_executor,
                    execution_policy=execution.execution_policy,
                    ibis_backend=execution.ibis_backend,
                ),
            )
            plan = IbisPlan(expr=ibis.memtable(table), ordering=Ordering.unordered())
        outputs.setdefault(rule.output_dataset, []).append(plan)
        work.register(rule.output_dataset, plan_schema(plan, ctx=ctx))
    merged: dict[str, IbisPlan] = {}
    for output, plans in outputs.items():
        if len(plans) == 1:
            merged[output] = plans[0]
            continue
        merged[output] = _union_plans(plans, label=output)
    union = _union_plans(list(merged.values()), label="relspec_graph")
    return GraphPlan(plan=union, outputs=merged)


def union_plans(plans: Sequence[IbisPlan], *, label: str) -> IbisPlan:
    """Union a sequence of Ibis plans into a single plan.

    Returns
    -------
    IbisPlan
        Unioned plan with unordered ordering metadata.
    """
    return _union_plans(plans, label=label)


def _union_plans(plans: Sequence[IbisPlan], *, label: str) -> IbisPlan:
    """Union a sequence of Ibis plans into a single plan.

    Parameters
    ----------
    plans
        Plans to union.
    label
        Label used for empty plan fallbacks.

    Returns
    -------
    IbisPlan
        Unioned plan with unordered ordering metadata.
    """
    if not plans:
        _ = label
        empty = ibis.memtable(pa.table({}))
        return IbisPlan(expr=empty, ordering=Ordering.unordered())
    _validate_set_op_ordering([plan.ordering for plan in plans], op_label="union")
    unioned = union_tables([plan.expr for plan in plans], distinct=False)
    return IbisPlan(expr=unioned, ordering=Ordering.unordered())


def _align_union_tables(tables: Sequence[IbisTable]) -> list[IbisTable]:
    """Align tables to a shared schema order for union.

    Parameters
    ----------
    tables
        Tables to align.

    Returns
    -------
    list[IbisTable]
        Tables with matching column order and types.
    """
    return align_set_op_tables(tables)


def _validate_set_op_ordering(orderings: Sequence[Ordering], *, op_label: str) -> None:
    levels = {ordering.level for ordering in orderings}
    if not levels or levels == {OrderingLevel.UNORDERED}:
        return
    if len(levels) != 1:
        msg = f"Set op {op_label} requires consistent ordering levels."
        raise ValueError(msg)
    reference = orderings[0]
    if any(ordering != reference for ordering in orderings[1:]):
        msg = f"Set op {op_label} requires consistent ordering keys."
        raise ValueError(msg)


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
    """Return rules whose evidence dependencies are satisfied.

    Parameters
    ----------
    pending
        Rules not yet scheduled.
    evidence
        Evidence catalog used for dependency checks.

    Returns
    -------
    list[RelationshipRule]
        Rules ready to be scheduled.
    """
    ready: list[RelationshipRule] = []
    for rule in pending:
        inputs = tuple(ref.name for ref in rule.inputs)
        if evidence.satisfies(_central_evidence(rule.evidence), inputs=inputs):
            ready.append(rule)
    return ready


def _register_rule_output(evidence: EvidenceCatalog, rule: RelationshipRule) -> None:
    """Register a rule's output schema or source in evidence.

    Parameters
    ----------
    evidence
        Evidence catalog to update.
    rule
        Relationship rule whose output is registered.
    """
    output_schema = _virtual_output_schema(rule)
    if output_schema is not None:
        evidence.register(rule.output_dataset, output_schema)
    else:
        evidence.sources.add(rule.output_dataset)


def _virtual_output_schema(rule: RelationshipRule) -> SchemaLike | None:
    """Resolve a virtual output schema for a relationship rule.

    Parameters
    ----------
    rule
        Relationship rule with optional contract metadata.

    Returns
    -------
    SchemaLike | None
        Schema for the rule output when available.
    """
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
    """Convert relationship evidence spec to central evidence spec.

    Parameters
    ----------
    spec
        Relationship-level evidence specification.

    Returns
    -------
    CentralEvidenceSpec | None
        Converted evidence spec when provided.
    """
    if spec is None:
        return None
    return CentralEvidenceSpec(
        sources=spec.sources,
        required_columns=spec.required_columns,
        required_types=spec.required_types,
    )


__all__ = [
    "EvidenceCatalog",
    "GraphExecutionOptions",
    "GraphPlan",
    "RuleNode",
    "compile_graph_plan",
    "order_rules",
    "union_plans",
]
