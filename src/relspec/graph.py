"""Graph helpers for relspec rule execution and plan compilation."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import Table as IbisTable
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.core.ordering import Ordering, OrderingLevel
from ibis_engine.execution import materialize_ibis_plan
from ibis_engine.execution_factory import ibis_execution_from_ctx
from ibis_engine.expr_compiler import align_set_op_tables, union_tables
from ibis_engine.plan import IbisPlan
from ibis_engine.sources import SourceToIbisOptions, register_ibis_table
from relspec.compiler import RelationshipRuleCompiler, RuleExecutionOptions
from relspec.contracts import RELATION_OUTPUT_NAME, relation_output_schema
from relspec.engine import IbisRelPlanCompiler
from relspec.errors import (
    RelspecCompilationError,
    RelspecExecutionError,
    RelspecValidationError,
)
from relspec.rustworkx_graph import build_rule_graph_from_relationship_rules
from relspec.rustworkx_schedule import schedule_rules
from relspec.execution_lanes import (
    DataFusionLaneInputs,
    DataFusionLaneOptions,
    ExecutionLaneRecord,
    execute_plan_datafusion,
    record_execution_lane,
    safe_backend,
)
from relspec.model import RelationshipRule
from relspec.rules.evidence import EvidenceCatalog

if TYPE_CHECKING:
    from datafusion_engine.runtime import AdapterExecutionPolicy, ExecutionLabel

logger = logging.getLogger(__name__)


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
    RelspecExecutionError
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
        raise RelspecExecutionError(msg)
    ibis_backend = execution.ibis_backend

    def _plan_executor(
        plan: IbisPlan,
        exec_ctx: ExecutionContext,
        exec_params: Mapping[IbisValue, object] | None,
        execution_label: ExecutionLabel | None = None,
    ) -> TableLike:
        resolved_params = exec_params if exec_params is not None else execution.params
        runtime_profile = exec_ctx.runtime.datafusion
        sqlglot_backend = safe_backend(ibis_backend)
        if runtime_profile is not None and sqlglot_backend is not None:
            try:
                return execute_plan_datafusion(
                    plan,
                    inputs=DataFusionLaneInputs(
                        ctx=exec_ctx,
                        backend=sqlglot_backend,
                        params=resolved_params,
                        execution_policy=execution.execution_policy,
                        execution_label=execution_label,
                        runtime_profile=runtime_profile,
                        options=DataFusionLaneOptions(allow_full_materialization=True),
                    ),
                )
            except (TypeError, ValueError, RuntimeError) as exc:
                record_execution_lane(
                    diagnostics_sink=runtime_profile.diagnostics_sink,
                    record=ExecutionLaneRecord(
                        engine="ibis",
                        lane="fallback",
                        fallback_reason=str(exc),
                        execution_label=execution_label,
                        options=DataFusionLaneOptions(allow_full_materialization=True),
                    ),
                )
                logger.exception("DataFusion lane failed; falling back to Ibis execution.")
        ibis_execution = ibis_execution_from_ctx(
            exec_ctx,
            backend=ibis_backend,
            params=resolved_params,
            execution_policy=execution.execution_policy,
            execution_label=execution_label,
        )
        return materialize_ibis_plan(plan, execution=ibis_execution)

    outputs: dict[str, list[IbisPlan]] = {}
    graph = build_rule_graph_from_relationship_rules(rules)
    output_schemas = _output_schema_map(rules)
    schedule = schedule_rules(
        graph,
        evidence=work,
        output_schema_for=output_schemas.get,
    )
    rules_by_name = {rule.name: rule for rule in rules}
    for name in schedule.ordered_rules:
        rule = rules_by_name[name]
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
    RelspecCompilationError
        Raised when there are no output plans available.
    """
    ordered_outputs = [output_for(rule) for rule in rules if output_for(rule) in plans]
    outputs = {name: plans[name] for name in ordered_outputs}
    if not outputs:
        msg = f"{label} requires at least one output plan."
        raise RelspecCompilationError(msg)
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
    RelspecCompilationError
        Raised when no plans are provided.

    Returns
    -------
    IbisPlan
        Unioned plan with unordered ordering metadata.
    """
    if not plans:
        msg = f"{label} requires at least one plan to union."
        raise RelspecCompilationError(msg)
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
        raise RelspecValidationError(msg)
    reference = orderings[0]
    if any(ordering != reference for ordering in orderings[1:]):
        msg = f"Set op {op_label} requires consistent ordering keys."
        raise RelspecValidationError(msg)


def _plan_schema(plan: IbisPlan) -> SchemaLike:
    return pa.schema(plan.expr.schema().to_pyarrow())


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
            from datafusion_engine.runtime import (
                dataset_schema_from_context,
            )

            return dataset_schema_from_context(rule.contract_name)
        except KeyError:
            return None
    return None


def _output_schema_map(
    rules: Sequence[RelationshipRule],
) -> dict[str, SchemaLike | None]:
    schemas: dict[str, SchemaLike | None] = {}
    for rule in rules:
        output = rule.output_dataset
        if output in schemas and schemas[output] is not None:
            continue
        schemas[output] = _virtual_output_schema(rule)
    return schemas


__all__ = [
    "GraphExecutionOptions",
    "GraphPlan",
    "compile_graph_plan",
    "compile_union_graph_plan",
    "union_plans",
]
