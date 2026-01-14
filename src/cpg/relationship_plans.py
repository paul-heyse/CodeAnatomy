"""Compile CPG relationship rules into plan outputs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, cast

import ibis
import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import BooleanValue, Table

from arrowdsl.core.context import ExecutionContext, Ordering
from arrowdsl.core.interop import Scalar
from arrowdsl.plan.ops import DedupeSpec, SortKey
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import ScanTelemetry
from arrowdsl.schema.ops import align_table
from cpg.catalog import PlanCatalog, PlanSource, resolve_plan_source
from cpg.plan_specs import ensure_plan
from ibis_engine.expr_compiler import (
    IbisExprRegistry,
    default_expr_registry,
    expr_ir_to_ibis,
)
from ibis_engine.plan import IbisPlan
from ibis_engine.plan_bridge import SourceToIbisOptions, source_to_ibis, table_to_ibis
from ibis_engine.query_compiler import apply_query_spec
from ibis_engine.schema_utils import align_table_to_schema
from relspec.compiler import (
    RelationshipRuleCompiler,
    apply_policy_defaults,
    validate_policy_requirements,
)
from relspec.compiler_graph import EvidenceCatalog, order_rules
from relspec.contracts import relation_output_schema
from relspec.engine import IbisRelPlanCompiler
from relspec.model import (
    AddLiteralSpec,
    DatasetRef,
    DedupeKernelSpec,
    DropColumnsSpec,
    ExplodeListSpec,
    FilterKernelSpec,
    KernelSpecT,
    RelationshipRule,
    RenameColumnsSpec,
)
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

    plans: Mapping[str, Plan | IbisPlan]
    telemetry: Mapping[str, ScanTelemetry] = field(default_factory=dict)


@dataclass(frozen=True)
class RelationPlanCompileOptions:
    """Options for compiling relationship plans."""

    rule_table: pa.Table | None = None
    materialize_debug: bool | None = None
    required_sources: Sequence[str] | None = None
    backend: BaseBackend | None = None
    param_bindings: Mapping[IbisValue, object] | None = None


IbisPlanSource = PlanSource | IbisPlan | Table


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
    ctx_exec = (
        ctx
        if options.materialize_debug is None
        else replace(
            ctx,
            debug=options.materialize_debug,
        )
    )
    work_catalog = PlanCatalog(catalog.snapshot())
    evidence = EvidenceCatalog.from_plan_catalog(work_catalog, ctx=ctx)
    resolver = CatalogPlanResolver(work_catalog, backend=options.backend)
    compiler = RelationshipRuleCompiler(resolver=resolver)
    plans: dict[str, Plan | IbisPlan] = {}
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


@dataclass
class IbisPlanCatalog:
    """Catalog wrapper for resolving Ibis plan inputs."""

    backend: BaseBackend
    tables: dict[str, IbisPlanSource] = field(default_factory=dict)

    def resolve(
        self,
        name: str,
        *,
        ctx: ExecutionContext,
        label: str | None = None,
    ) -> IbisPlan | None:
        source = self.tables.get(name)
        if source is None:
            return None
        if isinstance(source, IbisPlan):
            return source
        if isinstance(source, Table):
            return IbisPlan(expr=source, ordering=Ordering.unordered())
        plan_source = ensure_plan(cast("PlanSource", source), label=label or name, ctx=ctx)
        plan = source_to_ibis(
            plan_source,
            options=SourceToIbisOptions(
                ctx=ctx,
                backend=self.backend,
                name=label or name,
            ),
        )
        self.tables[name] = plan
        return plan


class CatalogIbisResolver:
    """Resolve dataset refs into Ibis plans from an Ibis catalog."""

    def __init__(self, catalog: IbisPlanCatalog) -> None:
        self._catalog = catalog

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> IbisPlan:
        plan = self._catalog.resolve(ref.name, ctx=ctx, label=ref.label or ref.name)
        if plan is None:
            msg = f"Unknown dataset reference: {ref.name!r}."
            raise KeyError(msg)
        expr = plan.expr
        if ref.query is not None:
            expr = apply_query_spec(expr, spec=ref.query)
        return IbisPlan(expr=expr, ordering=plan.ordering)

    @staticmethod
    def telemetry(ref: DatasetRef, *, ctx: ExecutionContext) -> ScanTelemetry | None:
        _ = ref
        _ = ctx
        return None


@dataclass(frozen=True)
class _IbisRelationContext:
    ctx_exec: ExecutionContext
    rules: tuple[RelationshipRule, ...]
    output_schema: pa.Schema
    evidence: EvidenceCatalog
    ibis_catalog: IbisPlanCatalog
    resolver: CatalogIbisResolver
    registry: IbisExprRegistry
    plan_compiler: IbisRelPlanCompiler
    compiler: RelationshipRuleCompiler
    telemetry: Mapping[str, ScanTelemetry]
    param_bindings: Mapping[IbisValue, object] | None


def _apply_rule_meta_ibis(table: Table, rule: RelationshipRule) -> Table:
    expr = table
    if rule.rule_name_col not in expr.columns:
        expr = expr.mutate(**{rule.rule_name_col: ibis.literal(rule.name)})
    if rule.rule_priority_col not in expr.columns:
        expr = expr.mutate(**{rule.rule_priority_col: ibis.literal(int(rule.priority))})
    return expr


def _apply_kernel_specs_ibis(
    table: Table,
    specs: Sequence[KernelSpecT],
    *,
    registry: IbisExprRegistry,
) -> Table:
    expr = table
    for spec in specs:
        if isinstance(spec, AddLiteralSpec):
            expr = _apply_add_literal_ibis(expr, spec)
        elif isinstance(spec, DropColumnsSpec):
            expr = _apply_drop_columns_ibis(expr, spec)
        elif isinstance(spec, FilterKernelSpec):
            expr = _apply_filter_ibis(expr, spec, registry=registry)
        elif isinstance(spec, RenameColumnsSpec):
            expr = _apply_rename_columns_ibis(expr, spec)
        elif isinstance(spec, ExplodeListSpec):
            expr = _apply_explode_list_ibis(expr, spec)
        elif isinstance(spec, DedupeKernelSpec):
            expr = _apply_dedupe_ibis(expr, spec)
        else:
            msg = f"Unsupported kernel spec for Ibis: {type(spec).__name__}."
            raise TypeError(msg)
    return expr


def _apply_add_literal_ibis(table: Table, spec: AddLiteralSpec) -> Table:
    if spec.name in table.columns:
        return table
    value = spec.value
    if isinstance(value, Scalar):
        value = value.as_py()
    return table.mutate(**{spec.name: ibis.literal(value)})


def _apply_drop_columns_ibis(table: Table, spec: DropColumnsSpec) -> Table:
    cols = [col for col in spec.columns if col in table.columns]
    return table.drop(*cols) if cols else table


def _apply_filter_ibis(
    table: Table,
    spec: FilterKernelSpec,
    *,
    registry: IbisExprRegistry,
) -> Table:
    predicate = expr_ir_to_ibis(spec.predicate, table, registry=registry)
    return table.filter(cast("BooleanValue", predicate))


def _apply_rename_columns_ibis(table: Table, spec: RenameColumnsSpec) -> Table:
    mapping = {key: val for key, val in spec.mapping.items() if key in table.columns}
    if not mapping:
        return table
    return table.rename(mapping)


def _apply_explode_list_ibis(table: Table, spec: ExplodeListSpec) -> Table:
    if spec.list_col not in table.columns:
        return table
    exploded = table.unnest(spec.list_col)
    renames: dict[str, str] = {}
    if spec.list_col != spec.out_value_col:
        renames[spec.list_col] = spec.out_value_col
    if spec.parent_id_col != spec.out_parent_col and spec.parent_id_col in exploded.columns:
        renames[spec.parent_id_col] = spec.out_parent_col
    return exploded.rename(renames) if renames else exploded


def _apply_dedupe_ibis(table: Table, spec: DedupeKernelSpec) -> Table:
    dedupe = spec.spec
    if not dedupe.keys:
        return table
    if dedupe.strategy not in {
        "KEEP_FIRST_AFTER_SORT",
        "KEEP_BEST_BY_SCORE",
        "KEEP_ARBITRARY",
    }:
        msg = f"Unsupported dedupe strategy for Ibis: {dedupe.strategy!r}."
        raise ValueError(msg)
    order_by = _dedupe_order_by(table, spec=dedupe)
    window = ibis.window(
        group_by=[table[key] for key in dedupe.keys],
        order_by=order_by,
    )
    ranked = table.mutate(_row_number=ibis.row_number().over(window))
    keep_first = ranked["_row_number"] == ibis.literal(1)
    return ranked.filter(keep_first).drop("_row_number")


def _ibis_relation_context(
    catalog: PlanCatalog,
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
    options: RelationPlanCompileOptions,
) -> _IbisRelationContext:
    resolved = options
    if resolved.backend is None:
        resolved = replace(resolved, backend=backend)
    rules, output_schema = _prepare_relation_rules(
        ctx,
        rule_table=resolved.rule_table,
        required_sources=resolved.required_sources,
    )
    ctx_exec = (
        ctx
        if resolved.materialize_debug is None
        else replace(
            ctx,
            debug=resolved.materialize_debug,
        )
    )
    work_catalog = PlanCatalog(catalog.snapshot())
    evidence = EvidenceCatalog.from_plan_catalog(work_catalog, ctx=ctx)
    tables: dict[str, IbisPlanSource] = {}
    tables.update(work_catalog.snapshot())
    ibis_catalog = IbisPlanCatalog(backend=backend, tables=tables)
    resolver = CatalogIbisResolver(ibis_catalog)
    registry = default_expr_registry()
    plan_compiler = IbisRelPlanCompiler(registry=registry)
    compiler = RelationshipRuleCompiler(resolver=resolver, plan_compiler=plan_compiler)
    telemetry = compiler.collect_scan_telemetry(rules, ctx=ctx)
    return _IbisRelationContext(
        ctx_exec=ctx_exec,
        rules=rules,
        output_schema=output_schema,
        evidence=evidence,
        ibis_catalog=ibis_catalog,
        resolver=resolver,
        registry=registry,
        plan_compiler=plan_compiler,
        compiler=compiler,
        telemetry=telemetry,
        param_bindings=resolved.param_bindings,
    )


def _compile_relation_plans_ibis(
    context: _IbisRelationContext,
    *,
    backend: BaseBackend,
    name_prefix: str,
) -> dict[str, IbisPlan]:
    plans: dict[str, IbisPlan] = {}
    for rule in order_rules(context.rules, evidence=context.evidence):
        compiled = context.compiler.compile_rule(rule, ctx=context.ctx_exec)
        if compiled.rel_plan is None:
            table = compiled.execute(
                ctx=context.ctx_exec,
                resolver=context.resolver,
                compiler=context.plan_compiler,
                params=context.param_bindings,
            )
            aligned = align_table(table, schema=context.output_schema, ctx=context.ctx_exec)
            plan = table_to_ibis(
                aligned,
                backend=backend,
                name=f"{name_prefix}_{rule.name}",
            )
        else:
            plan = context.plan_compiler.compile(
                compiled.rel_plan,
                ctx=context.ctx_exec,
                resolver=context.resolver,
            )
            expr = plan.expr
            if compiled.emit_rule_meta:
                expr = _apply_rule_meta_ibis(expr, rule)
            expr = _apply_kernel_specs_ibis(expr, rule.post_kernels, registry=context.registry)
            expr = align_table_to_schema(expr, schema=context.output_schema)
            plan = IbisPlan(expr=expr, ordering=plan.ordering)
        plans[rule.output_dataset] = plan
        context.ibis_catalog.tables[rule.output_dataset] = plan
        context.evidence.register(rule.output_dataset, context.output_schema)
    return plans


def _dedupe_order_by(table: Table, *, spec: DedupeSpec) -> list[IbisValue]:
    if spec.tie_breakers:
        return [_sort_expr(table, key) for key in spec.tie_breakers]
    return [table[key].asc() for key in spec.keys]


def _sort_expr(table: Table, key: SortKey) -> IbisValue:
    column = table[key.column]
    if key.order == "descending":
        return column.desc()
    return column.asc()


def compile_relation_plans_ibis(
    catalog: PlanCatalog,
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
    options: RelationPlanCompileOptions | None = None,
    name_prefix: str = "cpg_rel",
) -> RelationPlanBundle:
    """Compile relationship rules into Ibis plans keyed by output dataset name.

    Returns
    -------
    RelationPlanBundle
        Ibis plan bundle keyed by output dataset name plus telemetry.
    """
    options = options or RelationPlanCompileOptions()
    context = _ibis_relation_context(
        catalog,
        ctx=ctx,
        backend=backend,
        options=options,
    )
    plans = _compile_relation_plans_ibis(
        context,
        backend=backend,
        name_prefix=name_prefix,
    )
    return RelationPlanBundle(plans=plans, telemetry=context.telemetry)


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
