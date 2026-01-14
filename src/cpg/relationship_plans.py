"""Compile CPG relationship rules into plan outputs."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from typing import cast

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import QuerySpec, ScanTelemetry
from arrowdsl.plan.runner import run_plan
from arrowdsl.schema.build import ConstExpr, FieldExpr
from arrowdsl.schema.ops import align_plan, align_table
from cpg.catalog import PlanCatalog, resolve_plan_source
from cpg.plan_specs import ensure_plan
from relspec.compiler import (
    RelationshipRuleCompiler,
    apply_policy_defaults,
    validate_policy_requirements,
)
from relspec.compiler_graph import EvidenceCatalog, order_rules
from relspec.contracts import relation_output_schema
from relspec.model import DatasetRef, RelationshipRule
from relspec.policies import evidence_spec_from_schema
from relspec.rules.cache import rule_definitions_cached
from relspec.rules.compiler import RuleCompiler
from relspec.rules.handlers.cpg import RelationshipRuleHandler
from relspec.rules.spec_tables import rule_definitions_from_table


class CatalogPlanResolver:
    """Resolve dataset refs using the CPG plan catalog."""

    def __init__(self, catalog: PlanCatalog) -> None:
        self._catalog = catalog

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> Plan:
        """Resolve a DatasetRef into a Plan.

        Returns
        -------
        Plan
            Plan for the dataset reference.

        Raises
        ------
        KeyError
            Raised when the dataset reference is not found in the catalog.
        """
        source = resolve_plan_source(self._catalog, ref.name, ctx=ctx)
        if source is None:
            msg = f"Unknown dataset reference: {ref.name!r}."
            raise KeyError(msg)
        plan = ensure_plan(source, label=ref.label or ref.name, ctx=ctx)
        if ref.query is None:
            return plan
        return _apply_query_spec(plan, ref.query, ctx=ctx)

    @staticmethod
    def telemetry(ref: DatasetRef, *, ctx: ExecutionContext) -> ScanTelemetry | None:
        """Return scan telemetry for catalog-resolved plans (unavailable).

        Returns
        -------
        ScanTelemetry | None
            ``None`` because catalog-resolved plans do not expose telemetry.
        """
        _ = ref
        _ = ctx
        return None


def compile_relation_plans(
    catalog: PlanCatalog,
    *,
    ctx: ExecutionContext,
    rule_table: pa.Table | None = None,
    materialize_debug: bool | None = None,
    required_sources: Sequence[str] | None = None,
) -> dict[str, Plan]:
    """Compile relationship rules into plans keyed by output dataset name.

    Returns
    -------
    dict[str, Plan]
        Mapping of output dataset names to relation plans.
    """
    rules, output_schema = _prepare_relation_rules(
        ctx,
        rule_table=rule_table,
        required_sources=required_sources,
    )
    materialize = ctx.debug if materialize_debug is None else materialize_debug
    work_catalog = PlanCatalog(catalog.snapshot())
    evidence = EvidenceCatalog.from_plan_catalog(work_catalog, ctx=ctx)
    resolver = CatalogPlanResolver(work_catalog)
    compiler = RelationshipRuleCompiler(resolver=resolver)
    plans: dict[str, Plan] = {}
    for rule in order_rules(rules, evidence=evidence):
        compiled = compiler.compile_rule(rule, ctx=ctx)
        if compiled.plan is not None and not compiled.post_kernels:
            plan = _apply_rule_meta(compiled.plan, rule, ctx=ctx)
            aligned = align_plan(plan, schema=output_schema, ctx=ctx)
            force_materialize = materialize or rule.execution_mode == "table"
            if force_materialize:
                materialized = _materialize_plan(aligned, ctx=ctx, label=rule.name)
                plans[rule.output_dataset] = materialized
                work_catalog.add(rule.output_dataset, materialized)
                evidence.register(rule.output_dataset, materialized.schema(ctx=ctx))
            else:
                plans[rule.output_dataset] = aligned
                work_catalog.add(rule.output_dataset, aligned)
                evidence.register(rule.output_dataset, aligned.schema(ctx=ctx))
            continue
        table = compiled.execute(ctx=ctx, resolver=resolver)
        aligned = align_table(table, schema=output_schema, ctx=ctx)
        plan = Plan.table_source(aligned, label=rule.name)
        plans[rule.output_dataset] = plan
        work_catalog.add(rule.output_dataset, plan)
        evidence.register(rule.output_dataset, aligned.schema)
    return plans


def _resolve_relation_rules(
    ctx: ExecutionContext,
    *,
    rule_table: pa.Table | None,
) -> tuple[RelationshipRule, ...]:
    if rule_table is None:
        definitions = rule_definitions_cached("cpg")
    else:
        definitions = rule_definitions_from_table(rule_table)
    compiler = RuleCompiler(handlers={"cpg": RelationshipRuleHandler()})
    compiled = compiler.compile_rules(definitions, ctx=ctx)
    return cast("tuple[RelationshipRule, ...]", compiled)


def _prepare_relation_rules(
    ctx: ExecutionContext,
    *,
    rule_table: pa.Table | None,
    required_sources: Sequence[str] | None,
) -> tuple[tuple[RelationshipRule, ...], pa.Schema]:
    rules = _resolve_relation_rules(ctx, rule_table=rule_table)
    if required_sources:
        required_set = set(required_sources)
        rules = tuple(rule for rule in rules if set(_rule_sources(rule)).issubset(required_set))
    output_schema = relation_output_schema()
    rules = _apply_rule_policy_defaults(rules, output_schema)
    return rules, output_schema


def _apply_rule_policy_defaults(
    rules: Sequence[RelationshipRule],
    output_schema: pa.Schema,
) -> tuple[RelationshipRule, ...]:
    rules = tuple(apply_policy_defaults(rule, output_schema) for rule in rules)
    inferred = evidence_spec_from_schema(output_schema)
    if inferred is not None:
        rules = tuple(
            replace(rule, evidence=inferred) if rule.evidence is None else rule for rule in rules
        )
    for rule in rules:
        validate_policy_requirements(rule, output_schema)
    return rules


def _apply_query_spec(plan: Plan, spec: QuerySpec, *, ctx: ExecutionContext) -> Plan:
    exprs = [FieldExpr(name=col).to_expression() for col in spec.projection.base]
    names = list(spec.projection.base)
    for name, expr_spec in spec.projection.derived.items():
        exprs.append(expr_spec.to_expression())
        names.append(name)
    if exprs:
        plan = plan.project(exprs, names, label=plan.label or "")
    predicate = spec.predicate_expression()
    if predicate is not None:
        plan = plan.filter(predicate, ctx=ctx)
    pushdown = spec.pushdown_expression()
    if pushdown is not None:
        plan = plan.filter(pushdown, ctx=ctx)
    return plan


def _apply_rule_meta(plan: Plan, rule: RelationshipRule, *, ctx: ExecutionContext) -> Plan:
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


def _materialize_plan(plan: Plan, *, ctx: ExecutionContext, label: str) -> Plan:
    result = run_plan(
        plan,
        ctx=ctx,
        prefer_reader=False,
        attach_ordering_metadata=True,
    )
    table = cast("TableLike", result.value)
    return Plan.table_source(table, label=label)


def _rule_sources(rule: RelationshipRule) -> tuple[str, ...]:
    return tuple(ref.name for ref in rule.inputs)


__all__ = ["CatalogPlanResolver", "compile_relation_plans"]
