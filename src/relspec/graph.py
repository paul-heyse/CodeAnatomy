"""Graph helpers for relspec rule execution and signatures."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import Table as IbisTable
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.core.ordering import Ordering, OrderingLevel
from datafusion_engine.runtime import dataset_schema_from_context
from ibis_engine.execution import IbisExecutionContext, materialize_ibis_plan
from ibis_engine.expr_compiler import align_set_op_tables, union_tables
from ibis_engine.plan import IbisPlan
from ibis_engine.sources import SourceToIbisOptions, register_ibis_table
from registry_common.arrow_payloads import payload_hash
from relspec.compiler import RelationshipRuleCompiler, RuleExecutionOptions
from relspec.contracts import RELATION_OUTPUT_NAME, relation_output_schema
from relspec.engine import IbisRelPlanCompiler
from relspec.model import EvidenceSpec as RelationshipEvidenceSpec
from relspec.model import RelationshipRule
from relspec.rules.definitions import EvidenceSpec as RuleEvidenceSpec
from relspec.rules.evidence import EvidenceCatalog

if TYPE_CHECKING:
    from datafusion_engine.runtime import AdapterExecutionPolicy, ExecutionLabel

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
class RuleNode:
    """Node in the relationship rule graph."""

    name: str
    rule: RelationshipRule
    requires: tuple[str, ...] = ()


@dataclass(frozen=True)
class GraphPlan:
    """Graph-level plan with per-output subplans."""

    plan: IbisPlan
    outputs: dict[str, IbisPlan]


@dataclass(frozen=True)
class GraphExecutionOptions:
    """Execution options for graph-level plan materialization."""

    execution_policy: AdapterExecutionPolicy | None = None
    ibis_backend: BaseBackend | None = None
    params: Mapping[IbisValue, object] | None = None


@dataclass(frozen=True)
class RuleSelectors[RuleT]:
    """Selection helpers for generic rule ordering."""

    inputs_for: Callable[[RuleT], Sequence[str]]
    output_for: Callable[[RuleT], str]
    name_for: Callable[[RuleT], str]
    priority_for: Callable[[RuleT], int]
    evidence_for: Callable[[RuleT], RuleEvidenceSpec | None]
    output_schema_for: Callable[[RuleT], SchemaLike | None] | None = None


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

    Raises
    ------
    ValueError
        Raised when no Ibis backend is configured.

    Returns
    -------
    GraphPlan
        Graph-level plan and per-output subplans.
    """
    work = evidence.clone()
    plan_compiler = compiler.plan_compiler or IbisRelPlanCompiler()
    execution = execution or GraphExecutionOptions()
    if execution.ibis_backend is None:
        msg = "Ibis backend is required for graph plan compilation."
        raise ValueError(msg)
    ibis_backend = execution.ibis_backend

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
            ibis_backend=ibis_backend,
            params=resolved_params,
        )
        return materialize_ibis_plan(plan, execution=ibis_execution)

    outputs: dict[str, list[IbisPlan]] = {}
    for rule in order_rules(rules, evidence=work):
        compiled = compiler.compile_rule(rule, ctx=ctx)
        if compiled.rel_plan is not None:
            plan = plan_compiler.compile(
                compiled.rel_plan,
                ctx=ctx,
                resolver=compiler.resolver,
            )
            plan = compiled.apply_plan_transforms(plan, ctx=ctx)
        else:
            table = compiled.execute(
                ctx=ctx,
                resolver=compiler.resolver,
                compiler=plan_compiler,
                options=RuleExecutionOptions(
                    params=execution.params,
                    plan_executor=_plan_executor,
                    execution_policy=execution.execution_policy,
                    ibis_backend=ibis_backend,
                ),
            )
            plan = register_ibis_table(
                table,
                options=SourceToIbisOptions(
                    backend=ibis_backend,
                    name=None,
                    ordering=Ordering.unordered(),
                ),
            )
        outputs.setdefault(rule.output_dataset, []).append(plan)
        work.register(rule.output_dataset, _plan_schema(plan))
    merged: dict[str, IbisPlan] = {}
    for output, plans in outputs.items():
        if len(plans) == 1:
            merged[output] = plans[0]
            continue
        merged[output] = _union_plans(plans, label=output)
    union = _union_plans(list(merged.values()), label="relspec_graph")
    return GraphPlan(plan=union, outputs=merged)


def compile_union_graph_plan[RuleT](
    rules: Sequence[RuleT],
    *,
    plans: Mapping[str, IbisPlan],
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


def union_plans(plans: Sequence[IbisPlan], *, label: str) -> IbisPlan:
    """Union a sequence of Ibis plans into a single plan.

    Returns
    -------
    IbisPlan
        Unioned plan with unordered ordering metadata.
    """
    return _union_plans(plans, label=label)


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


def _union_plans(plans: Sequence[IbisPlan], *, label: str) -> IbisPlan:
    """Union a sequence of Ibis plans into a single plan.

    Parameters
    ----------
    plans
        Plans to union.
    label
        Label used for empty plan fallbacks.

    Raises
    ------
    ValueError
        Raised when no plans are provided.

    Returns
    -------
    IbisPlan
        Unioned plan with unordered ordering metadata.
    """
    if not plans:
        msg = f"{label} requires at least one plan to union."
        raise ValueError(msg)
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


def _plan_schema(plan: IbisPlan) -> SchemaLike:
    return pa.schema(plan.expr.schema().to_pyarrow())


def _ready_rules(
    pending: Sequence[RelationshipRule],
    evidence: EvidenceCatalog,
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
        if rule.contract_name == RELATION_OUTPUT_NAME:
            return relation_output_schema()
        try:
            return dataset_schema_from_context(rule.contract_name)
        except KeyError:
            return None
    return None


def _central_evidence(
    spec: RelationshipEvidenceSpec | None,
) -> RuleEvidenceSpec | None:
    """Convert relationship evidence spec to central evidence spec.

    Parameters
    ----------
    spec
        Relationship-level evidence specification.

    Returns
    -------
    RuleEvidenceSpec | None
        Converted evidence spec when provided.
    """
    if spec is None:
        return None
    return RuleEvidenceSpec(
        sources=spec.sources,
        required_columns=spec.required_columns,
        required_types=spec.required_types,
    )


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
        if priority_for(rule) < priority_for(existing):
            selected[output] = rule
            continue
        if priority_for(rule) == priority_for(existing) and name_for(rule) < name_for(existing):
            selected[output] = rule
    return list(selected.values())


__all__ = [
    "GraphExecutionOptions",
    "GraphPlan",
    "RuleNode",
    "RuleSelectors",
    "compile_graph_plan",
    "compile_union_graph_plan",
    "order_rules",
    "order_rules_by_evidence",
    "rule_graph_signature",
    "union_plans",
]
