"""Relationship rule registry for CPG edge construction."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from functools import cache
from typing import cast

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.plan.plan import Plan, union_all_plans
from arrowdsl.plan.query import QuerySpec
from arrowdsl.plan.runner import run_plan
from arrowdsl.plan.scan_io import DatasetSource
from arrowdsl.schema.build import ConstExpr, FieldExpr
from arrowdsl.schema.ops import align_plan, align_table
from arrowdsl.spec.tables.cpg import EDGE_EMIT_STRUCT
from arrowdsl.spec.tables.relspec import (
    evidence_row,
    hash_join_row,
    interval_align_row,
    kernel_spec_row,
    project_row,
    relationship_rule_definition_table,
    relationship_rules_from_table,
    winner_select_row,
)
from cpg.catalog import PlanCatalog, resolve_plan_source
from cpg.kinds_ultimate import EdgeKind
from cpg.plan_specs import ensure_plan
from cpg.relation_registry_specs import rule_definition_specs
from cpg.relation_template_specs import RuleDefinitionSpec
from cpg.specs import EdgeEmitSpec, EdgePlanSpec
from relspec.compiler import (
    RelationshipRuleCompiler,
    apply_policy_defaults,
    validate_policy_requirements,
)
from relspec.compiler_graph import (
    EvidenceCatalog,
    GraphPlan,
    compile_graph_plan,
    order_rules,
)
from relspec.contracts import relation_output_schema
from relspec.model import DatasetRef, RelationshipRule
from relspec.policies import evidence_spec_from_schema


@cache
def relation_rule_table_cached() -> pa.Table:
    """Return the relationship rule table for CPG edges.

    Returns
    -------
    pa.Table
        Arrow table of relationship rules.
    """
    definitions = rule_definition_specs()
    return _relation_rule_edge_table(definitions)


def _rule_definition_row(spec: RuleDefinitionSpec) -> dict[str, object]:
    project = project_row(spec.project)
    predicate_expr = spec.predicate.to_json() if spec.predicate is not None else None
    post_kernels = [kernel_spec_row(kernel) for kernel in spec.post_kernels] or None
    return {
        "name": spec.name,
        "kind": spec.kind.value,
        "output_dataset": spec.output_dataset,
        "contract_name": spec.contract_name,
        "inputs": list(spec.inputs) or None,
        "hash_join": hash_join_row(spec.hash_join),
        "interval_align": interval_align_row(spec.interval_align),
        "winner_select": winner_select_row(spec.winner_select),
        "predicate_expr": predicate_expr,
        "project": project,
        "post_kernels": post_kernels,
        "evidence": evidence_row(spec.evidence),
        "confidence_policy": spec.confidence_policy,
        "ambiguity_policy": spec.ambiguity_policy,
        "priority": int(spec.priority),
        "emit_rule_meta": spec.emit_rule_meta,
        "rule_name_col": spec.rule_name_col,
        "rule_priority_col": spec.rule_priority_col,
        "execution_mode": spec.execution_mode,
    }


def _relation_rule_definition_table(definitions: Sequence[RuleDefinitionSpec]) -> pa.Table:
    rows = [_rule_definition_row(spec) for spec in definitions]
    return relationship_rule_definition_table(rows)


def _relation_rule_edge_table(definitions: Sequence[RuleDefinitionSpec]) -> pa.Table:
    table = _relation_rule_definition_table(definitions)
    metadata = table.schema.metadata
    edge_rows: list[dict[str, object] | None] = []
    edge_flags: list[str | None] = []
    for spec in definitions:
        edge = spec.edge
        if edge is None:
            edge_rows.append(None)
            edge_flags.append(None)
            continue
        edge_rows.append(
            {
                "edge_kind": edge.edge_kind.value,
                "src_cols": list(edge.src_cols),
                "dst_cols": list(edge.dst_cols),
                "path_cols": list(edge.path_cols),
                "bstart_cols": list(edge.bstart_cols),
                "bend_cols": list(edge.bend_cols),
                "origin": edge.origin,
                "default_resolution_method": edge.resolution_method,
            }
        )
        edge_flags.append(edge.option_flag)
    table = table.append_column("edge_option_flag", pa.array(edge_flags, type=pa.string()))
    table = table.append_column("edge_emit", pa.array(edge_rows, type=EDGE_EMIT_STRUCT))
    if metadata:
        table = table.replace_schema_metadata(metadata)
    return table


def relation_rules() -> tuple[RelationshipRule, ...]:
    """Return relationship rules parsed from the rule table.

    Returns
    -------
    tuple[RelationshipRule, ...]
        Relationship rule definitions.
    """
    return relationship_rules_from_table(relation_rule_table_cached())


def edge_plan_specs() -> tuple[EdgePlanSpec, ...]:
    """Return edge plan specs for CPG edge emission.

    Returns
    -------
    tuple[EdgePlanSpec, ...]
        Edge plan specs for edge emission.
    """
    return edge_plan_specs_from_table(relation_rule_table_cached())


def edge_plan_specs_from_table(table: pa.Table) -> tuple[EdgePlanSpec, ...]:
    specs: list[EdgePlanSpec] = []
    for row in table.to_pylist():
        emit_row = row.get("edge_emit")
        option_flag = row.get("edge_option_flag")
        if emit_row is None or option_flag is None:
            continue
        emit = EdgeEmitSpec(
            edge_kind=EdgeKind(str(emit_row["edge_kind"])),
            src_cols=tuple(emit_row.get("src_cols") or ()),
            dst_cols=tuple(emit_row.get("dst_cols") or ()),
            path_cols=tuple(emit_row.get("path_cols") or ()),
            bstart_cols=tuple(emit_row.get("bstart_cols") or ()),
            bend_cols=tuple(emit_row.get("bend_cols") or ()),
            origin=str(emit_row["origin"]),
            default_resolution_method=str(emit_row["default_resolution_method"]),
        )
        specs.append(
            EdgePlanSpec(
                name=str(row["name"]),
                option_flag=str(option_flag),
                relation_ref=str(row["name"]),
                emit=emit,
            )
        )
    return tuple(specs)


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


def _rule_sources(rule: RelationshipRule) -> tuple[str, ...]:
    if rule.evidence is None:
        return tuple(ref.name for ref in rule.inputs)
    return rule.evidence.sources or tuple(ref.name for ref in rule.inputs)


def _schema_from_source(source: object, *, ctx: ExecutionContext) -> SchemaLike | None:
    if isinstance(source, Plan):
        return source.schema(ctx=ctx)
    if isinstance(source, DatasetSource):
        return source.dataset.schema
    schema = getattr(source, "schema", None)
    if schema is not None and hasattr(schema, "names"):
        return schema
    return None


def _default_schema_for_rule(
    rule: RelationshipRule,
    catalog: PlanCatalog,
    *,
    ctx: ExecutionContext,
    fallback: SchemaLike,
) -> SchemaLike:
    sources = _rule_sources(rule)
    if len(sources) == 1:
        source = catalog.tables.get(sources[0])
        if source is not None:
            schema = _schema_from_source(source, ctx=ctx)
            if schema is not None:
                return schema
    return fallback


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


def _align_relation_plan(plan: Plan, *, ctx: ExecutionContext) -> Plan:
    return align_plan(plan, schema=relation_output_schema(), ctx=ctx)


def _align_relation_table(table: TableLike, *, ctx: ExecutionContext) -> TableLike:
    return align_table(table, schema=relation_output_schema(), ctx=ctx)


def _materialize_plan(plan: Plan, *, ctx: ExecutionContext, label: str) -> Plan:
    result = run_plan(
        plan,
        ctx=ctx,
        prefer_reader=False,
        attach_ordering_metadata=True,
    )
    table = cast("TableLike", result.value)
    return Plan.table_source(table, label=label)


def compile_relation_plans(
    catalog: PlanCatalog,
    *,
    ctx: ExecutionContext,
    rule_table: pa.Table | None = None,
    materialize_debug: bool | None = None,
    required_sources: Sequence[str] | None = None,
) -> dict[str, Plan]:
    """Compile relation rules into plans keyed by output dataset name.

    Returns
    -------
    dict[str, Plan]
        Mapping of output dataset names to relation plans.
    """
    rules = relationship_rules_from_table(rule_table or relation_rule_table_cached())
    if required_sources:
        required_set = set(required_sources)
        rules = tuple(rule for rule in rules if set(_rule_sources(rule)).issubset(required_set))
    output_schema = relation_output_schema()
    rules = tuple(
        apply_policy_defaults(
            rule,
            _default_schema_for_rule(rule, catalog, ctx=ctx, fallback=output_schema),
        )
        for rule in rules
    )
    for rule in rules:
        validate_policy_requirements(rule, output_schema)
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
            aligned = _align_relation_plan(plan, ctx=ctx)
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
        aligned = _align_relation_table(table, ctx=ctx)
        plan = Plan.table_source(aligned, label=rule.name)
        plans[rule.output_dataset] = plan
        work_catalog.add(rule.output_dataset, plan)
        evidence.register(rule.output_dataset, aligned.schema)
    return plans


def compile_relation_graph_plan(
    catalog: PlanCatalog,
    *,
    ctx: ExecutionContext,
    rule_table: pa.Table | None = None,
) -> GraphPlan:
    """Compile relation rules into a unified graph plan.

    Returns
    -------
    GraphPlan
        Graph plan containing a union plan and per-output subplans.
    """
    rules = relationship_rules_from_table(rule_table or relation_rule_table_cached())
    output_schema = relation_output_schema()
    rules = tuple(apply_policy_defaults(rule, output_schema) for rule in rules)
    inferred_evidence = evidence_spec_from_schema(output_schema)
    if inferred_evidence is not None:
        rules = tuple(
            replace(rule, evidence=inferred_evidence) if rule.evidence is None else rule
            for rule in rules
        )
    for rule in rules:
        validate_policy_requirements(rule, output_schema)
    work_catalog = PlanCatalog(catalog.snapshot())
    evidence = EvidenceCatalog.from_plan_catalog(work_catalog, ctx=ctx)
    resolver = CatalogPlanResolver(work_catalog)
    compiler = RelationshipRuleCompiler(resolver=resolver)
    graph = compile_graph_plan(rules, ctx=ctx, compiler=compiler, evidence=evidence)
    aligned_outputs: dict[str, Plan] = {}
    for name, plan in graph.outputs.items():
        aligned = align_plan(plan, schema=output_schema, ctx=ctx)
        aligned_outputs[name] = aligned
    union = union_all_plans(tuple(aligned_outputs.values()), label="relspec_graph")
    return GraphPlan(plan=union, outputs=aligned_outputs)


__all__ = [
    "CatalogPlanResolver",
    "compile_relation_graph_plan",
    "compile_relation_plans",
    "edge_plan_specs",
    "relation_rule_table_cached",
    "relation_rules",
]
