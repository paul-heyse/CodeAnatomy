"""Compile CPG relationship rules into plan outputs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, cast

import ibis
import pyarrow as pa
from ibis.backends import BaseBackend

from arrowdsl.core.context import ExecutionContext
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import ScanTelemetry
from arrowdsl.schema.ops import align_table
from cpg.catalog import PlanCatalog, resolve_plan_source
from cpg.ibis_bridge import plan_bundle_to_ibis
from cpg.plan_specs import ensure_plan
from ibis_engine.plan import IbisPlan
from ibis_engine.plan_bridge import table_to_ibis
from ibis_engine.query_compiler import apply_query_spec
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

if TYPE_CHECKING:
    from ibis.expr.types import Value as IbisValue


@dataclass(frozen=True)
class RelationPlanBundle:
    """Compiled relation plans plus scan telemetry."""

    plans: dict[str, Plan]
    telemetry: Mapping[str, ScanTelemetry] = field(default_factory=dict)


@dataclass(frozen=True)
class RelationPlanCompileOptions:
    """Options for compiling relationship plans."""

    rule_table: pa.Table | None = None
    materialize_debug: bool | None = None
    required_sources: Sequence[str] | None = None
    backend: BaseBackend | None = None
    param_bindings: Mapping[IbisValue, object] | None = None


class CatalogPlanResolver:
    """Resolve dataset refs using the CPG plan catalog."""

    def __init__(self, catalog: PlanCatalog, *, backend: BaseBackend | None = None) -> None:
        self._catalog = catalog
        self._backend = backend

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> IbisPlan:
        """Resolve a DatasetRef into an Ibis plan.

        Returns
        -------
        IbisPlan
            Ibis plan for the dataset reference.

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
        table = plan.to_table(ctx=ctx)
        if self._backend is None:
            expr = ibis.memtable(table)
            ibis_plan = IbisPlan(expr=expr, ordering=plan.ordering)
        else:
            ibis_plan = table_to_ibis(
                table,
                backend=self._backend,
                name=ref.label or ref.name,
                ordering=plan.ordering,
            )
        if ref.query is None:
            return ibis_plan
        expr = apply_query_spec(ibis_plan.expr, spec=ref.query)
        return IbisPlan(expr=expr, ordering=ibis_plan.ordering)

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
    options: RelationPlanCompileOptions | None = None,
) -> RelationPlanBundle:
    """Compile relationship rules into plans keyed by output dataset name.

    Returns
    -------
    RelationPlanBundle
        Plan bundle with output plans and scan telemetry.
    """
    options = options or RelationPlanCompileOptions()
    rules, output_schema = _prepare_relation_rules(
        ctx,
        rule_table=options.rule_table,
        required_sources=options.required_sources,
    )
    ctx_exec = ctx if options.materialize_debug is None else replace(
        ctx,
        debug=options.materialize_debug,
    )
    work_catalog = PlanCatalog(catalog.snapshot())
    evidence = EvidenceCatalog.from_plan_catalog(work_catalog, ctx=ctx)
    resolver = CatalogPlanResolver(work_catalog, backend=options.backend)
    compiler = RelationshipRuleCompiler(resolver=resolver)
    plans: dict[str, Plan] = {}
    telemetry = compiler.collect_scan_telemetry(rules, ctx=ctx)
    for rule in order_rules(rules, evidence=evidence):
        compiled = compiler.compile_rule(rule, ctx=ctx_exec)
        table = compiled.execute(
            ctx=ctx_exec,
            resolver=resolver,
            compiler=compiler.plan_compiler,
            params=options.param_bindings,
        )
        aligned = align_table(table, schema=output_schema, ctx=ctx_exec)
        plan = Plan.table_source(aligned, label=rule.name)
        plans[rule.output_dataset] = plan
        work_catalog.add(rule.output_dataset, plan)
        evidence.register(rule.output_dataset, aligned.schema)
    return RelationPlanBundle(plans=plans, telemetry=telemetry)


def compile_relation_plans_ibis(
    catalog: PlanCatalog,
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
    options: RelationPlanCompileOptions | None = None,
    name_prefix: str = "cpg_rel",
) -> dict[str, IbisPlan]:
    """Compile relationship rules into Ibis plans keyed by output dataset name.

    Returns
    -------
    dict[str, IbisPlan]
        Ibis plans keyed by output dataset name.
    """
    options = options or RelationPlanCompileOptions()
    if options.backend is None:
        options = replace(options, backend=backend)
    bundle = compile_relation_plans(
        catalog,
        ctx=ctx,
        options=options,
    )
    return plan_bundle_to_ibis(
        bundle.plans,
        ctx=ctx,
        backend=backend,
        name_prefix=name_prefix,
    )


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


def _rule_sources(rule: RelationshipRule) -> tuple[str, ...]:
    return tuple(ref.name for ref in rule.inputs)


__all__ = [
    "CatalogPlanResolver",
    "RelationPlanBundle",
    "RelationPlanCompileOptions",
    "compile_relation_plans",
    "compile_relation_plans_ibis",
]
