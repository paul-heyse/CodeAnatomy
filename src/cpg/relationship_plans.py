"""Compile CPG relationship rules into plan outputs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Literal, cast

import ibis
import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import BooleanValue, NumericValue, Table, Value

from arrowdsl.core.context import ExecutionContext, Ordering
from arrowdsl.core.interop import Scalar, TableLike
from arrowdsl.plan.ops import DedupeSpec, SortKey
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import ScanTelemetry
from arrowdsl.plan.runner import AdapterRunOptions, run_plan_adapter
from arrowdsl.schema.schema import align_table
from config import AdapterMode
from cpg.catalog import PlanCatalog, PlanSource, resolve_plan_source
from cpg.plan_specs import ensure_plan
from datafusion_engine.runtime import AdapterExecutionPolicy, ExecutionLabel
from ibis_engine.expr_compiler import (
    IbisExprRegistry,
    default_expr_registry,
    expr_ir_to_ibis,
)
from ibis_engine.plan import IbisPlan
from ibis_engine.plan_bridge import (
    SourceToIbisOptions,
    register_ibis_view,
    source_to_ibis,
    table_to_ibis,
)
from ibis_engine.query_compiler import apply_query_spec
from ibis_engine.schema_utils import align_table_to_schema
from relspec.compiler import (
    PlanExecutor,
    RelationshipRuleCompiler,
    RuleExecutionOptions,
    apply_policy_defaults,
    validate_policy_requirements,
)
from relspec.compiler_graph import EvidenceCatalog, order_rules
from relspec.contracts import relation_output_schema
from relspec.engine import IbisRelPlanCompiler
from relspec.model import (
    AddLiteralSpec,
    CanonicalSortKernelSpec,
    DatasetRef,
    DedupeKernelSpec,
    DropColumnsSpec,
    ExplodeListSpec,
    FilterKernelSpec,
    IntervalAlignConfig,
    KernelSpecT,
    RelationshipRule,
    RenameColumnsSpec,
    WinnerSelectConfig,
)
from relspec.policies import evidence_spec_from_schema
from relspec.rules.cache import rule_definitions_cached
from relspec.rules.compiler import RuleCompiler
from relspec.rules.handlers.cpg import RelationshipRuleHandler
from relspec.rules.spec_tables import rule_definitions_from_table

if TYPE_CHECKING:
    from ibis.expr.types import Column, Deferred, Selector
    from ibis.expr.types import Value as IbisValue


@dataclass(frozen=True)
class RelationPlanBundle:
    """Compiled relation plans plus scan telemetry."""

    plans: Mapping[str, Plan | IbisPlan]
    telemetry: Mapping[str, ScanTelemetry] = field(default_factory=dict)
    coverage: Mapping[str, IbisPlan] = field(default_factory=dict)


@dataclass(frozen=True)
class RelationPlanCompileOptions:
    """Options for compiling relationship plans."""

    rule_table: pa.Table | None = None
    materialize_debug: bool | None = None
    required_sources: Sequence[str] | None = None
    backend: BaseBackend | None = None
    param_bindings: Mapping[IbisValue, object] | None = None
    adapter_mode: AdapterMode | None = None
    execution_policy: AdapterExecutionPolicy | None = None


IbisPlanSource = PlanSource | IbisPlan | Table


class CatalogPlanResolver:
    """Resolve dataset refs using the CPG plan catalog."""

    def __init__(self, catalog: PlanCatalog, *, backend: BaseBackend | None = None) -> None:
        self._catalog = catalog
        self.backend: BaseBackend | None = backend

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
        if self.backend is None:
            expr = ibis.memtable(table)
            ibis_plan = IbisPlan(expr=expr, ordering=plan.ordering)
        else:
            ibis_plan = table_to_ibis(
                table,
                backend=self.backend,
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


def _plan_executor_factory(
    *,
    adapter_mode: AdapterMode | None,
    execution_policy: AdapterExecutionPolicy | None,
    ibis_backend: BaseBackend | None,
) -> PlanExecutor:
    resolved_adapter_mode = (
        adapter_mode if adapter_mode is None or adapter_mode.use_ibis_bridge else None
    )

    def _plan_executor(
        plan: IbisPlan,
        exec_ctx: ExecutionContext,
        exec_params: Mapping[IbisValue, object] | None,
        execution_label: ExecutionLabel | None = None,
    ) -> TableLike:
        result = run_plan_adapter(
            plan,
            ctx=exec_ctx,
            options=AdapterRunOptions(
                adapter_mode=resolved_adapter_mode,
                prefer_reader=False,
                execution_policy=execution_policy,
                execution_label=execution_label,
                ibis_backend=ibis_backend,
                ibis_params=exec_params,
            ),
        )
        return cast("TableLike", result.value)

    return _plan_executor


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

    Raises
    ------
    ValueError
        Raised when a relation plan cannot be compiled.
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
    plan_executor = _plan_executor_factory(
        adapter_mode=options.adapter_mode,
        execution_policy=options.execution_policy,
        ibis_backend=options.backend,
    )
    plans: dict[str, Plan | IbisPlan] = {}
    telemetry = compiler.collect_scan_telemetry(rules, ctx=ctx)
    for rule in order_rules(rules, evidence=evidence):
        compiled = compiler.compile_rule(rule, ctx=ctx_exec)
        table = compiled.execute(
            ctx=ctx_exec,
            resolver=resolver,
            compiler=compiler.plan_compiler,
            options=RuleExecutionOptions(
                params=options.param_bindings,
                plan_executor=plan_executor,
                adapter_mode=options.adapter_mode,
                execution_policy=options.execution_policy,
                ibis_backend=options.backend,
            ),
        )
        table = align_table(table, schema=output_schema)
        plan = Plan.table_source(table, label=rule.name)
        if plan is None:
            msg = f"Failed to compile relation plan for rule {rule.name!r}."
            raise ValueError(msg)
        plans[rule.output_dataset] = plan
        work_catalog.add(rule.output_dataset, plan)
        evidence.register(rule.output_dataset, table.schema)
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
        self.backend: BaseBackend | None = catalog.backend

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
    adapter_mode: AdapterMode | None
    execution_policy: AdapterExecutionPolicy | None


@dataclass(frozen=True)
class _IntervalAlignInputs:
    left_expr: Table
    right_expr: Table
    right_map: Mapping[str, str]
    left_id_col: str
    right_path: str
    right_start: str
    right_end: str
    left_keep: Sequence[str]
    right_keep: Sequence[str]


@dataclass(frozen=True)
class _IntervalMatchResult:
    matched: Table
    winners: Table
    score_col: str
    candidate_col: str


@dataclass(frozen=True)
class _WinnerByScoreSpec:
    group_key: str
    score_col: str
    score_order: Literal["ascending", "descending"]
    tie_breakers: Sequence[SortKey]


@dataclass(frozen=True)
class _IntervalAlignMetricsSpec:
    left_id_col: str
    candidate_col: str
    rule_name: str


@dataclass(frozen=True)
class _IntervalOutputSpec:
    output_names: Sequence[str]
    left_keep: set[str]
    right_keep: set[str]
    right_map: Mapping[str, str]
    cfg: IntervalAlignConfig
    score_col: str


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
        elif isinstance(spec, CanonicalSortKernelSpec):
            expr = _apply_canonical_sort_ibis(expr, spec)
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
    row_number_col = _unique_name("row_number", set(table.columns))
    ranked = table.mutate(**{row_number_col: ibis.row_number().over(window)})
    keep_first = ranked[row_number_col] == ibis.literal(1)
    return ranked.filter(keep_first).drop(row_number_col)


def _apply_canonical_sort_ibis(table: Table, spec: CanonicalSortKernelSpec) -> Table:
    order_by = [_sort_expr(table, key) for key in spec.sort_keys if key.column in table.columns]
    if not order_by:
        return table
    order_exprs = cast("Sequence[str | Column | Selector | Deferred]", order_by)
    return table.order_by(*order_exprs)


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
        adapter_mode=resolved.adapter_mode,
        execution_policy=resolved.execution_policy,
    )


def _compile_relation_plans_ibis(
    context: _IbisRelationContext,
    *,
    backend: BaseBackend,
    name_prefix: str,
) -> tuple[dict[str, IbisPlan], dict[str, IbisPlan]]:
    plans: dict[str, IbisPlan] = {}
    coverage: dict[str, IbisPlan] = {}
    plan_executor = _plan_executor_factory(
        adapter_mode=context.adapter_mode,
        execution_policy=context.execution_policy,
        ibis_backend=backend,
    )

    for rule in order_rules(context.rules, evidence=context.evidence):
        compiled = context.compiler.compile_rule(rule, ctx=context.ctx_exec)
        plan = None
        metrics_plan = None
        if compiled.rel_plan is None:
            plan, metrics_plan = _compile_kernel_rule_ibis(rule, context=context)
        if plan is None and compiled.rel_plan is None:
            table = compiled.execute(
                ctx=context.ctx_exec,
                resolver=context.resolver,
                compiler=context.plan_compiler,
                options=RuleExecutionOptions(
                    params=context.param_bindings,
                    plan_executor=plan_executor,
                    adapter_mode=context.adapter_mode,
                    execution_policy=context.execution_policy,
                    ibis_backend=backend,
                ),
            )
            aligned = align_table(table, schema=context.output_schema)
            plan = table_to_ibis(
                aligned,
                backend=backend,
                name=f"{name_prefix}_{rule.name}",
            )
        if plan is None and compiled.rel_plan is not None:
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
        if plan is None:
            msg = f"Failed to compile ibis relation plan for rule {rule.name!r}."
            raise ValueError(msg)
        view_name = f"{name_prefix}_{rule.output_dataset}" if name_prefix else rule.output_dataset
        plan = register_ibis_view(
            plan.expr,
            backend=backend,
            name=view_name,
            ordering=plan.ordering,
        )
        plans[rule.output_dataset] = plan
        context.ibis_catalog.tables[rule.output_dataset] = plan
        context.evidence.register(rule.output_dataset, context.output_schema)
        if metrics_plan is not None:
            coverage[rule.name] = metrics_plan
    return plans, coverage


def _dedupe_order_by(table: Table, *, spec: DedupeSpec) -> list[IbisValue]:
    if spec.tie_breakers:
        return [_sort_expr(table, key) for key in spec.tie_breakers]
    return [table[key].asc() for key in spec.keys]


def _normalize_sort_order(order: str) -> Literal["ascending", "descending"]:
    return "descending" if order == "descending" else "ascending"


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
    plans, coverage = _compile_relation_plans_ibis(
        context,
        backend=backend,
        name_prefix=name_prefix,
    )
    return RelationPlanBundle(
        plans=plans,
        telemetry=context.telemetry,
        coverage=coverage,
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


def _compile_kernel_rule_ibis(
    rule: RelationshipRule,
    *,
    context: _IbisRelationContext,
) -> tuple[IbisPlan | None, IbisPlan | None]:
    if rule.interval_align is not None:
        return _interval_align_plan_ibis(rule, context=context)
    if rule.winner_select is not None:
        return _winner_select_plan_ibis(rule, context=context)
    return None, None


def _interval_align_plan_ibis(
    rule: RelationshipRule,
    *,
    context: _IbisRelationContext,
) -> tuple[IbisPlan, IbisPlan | None]:
    cfg = rule.interval_align
    if cfg is None:
        msg = "INTERVAL_ALIGN rules require interval_align config."
        raise ValueError(msg)
    left_ref, right_ref = rule.inputs
    left_plan = context.resolver.resolve(left_ref, ctx=context.ctx_exec)
    right_plan = context.resolver.resolve(right_ref, ctx=context.ctx_exec)
    expr, metrics = _interval_align_expr(
        left_plan.expr,
        right_plan.expr,
        cfg=cfg,
        rule_name=rule.name,
    )
    if rule.emit_rule_meta:
        expr = _apply_rule_meta_ibis(expr, rule)
    expr = _apply_kernel_specs_ibis(expr, rule.post_kernels, registry=context.registry)
    expr = align_table_to_schema(expr, schema=context.output_schema)
    plan = IbisPlan(expr=expr, ordering=Ordering.unordered())
    metrics_plan = (
        None if metrics is None else IbisPlan(expr=metrics, ordering=Ordering.unordered())
    )
    return plan, metrics_plan


def _winner_select_plan_ibis(
    rule: RelationshipRule,
    *,
    context: _IbisRelationContext,
) -> tuple[IbisPlan, IbisPlan | None]:
    cfg = rule.winner_select
    if cfg is None:
        msg = "WINNER_SELECT rules require winner_select config."
        raise ValueError(msg)
    src_ref = rule.inputs[0]
    src_plan = context.resolver.resolve(src_ref, ctx=context.ctx_exec)
    expr, metrics = _winner_select_expr(
        src_plan.expr,
        cfg=cfg,
        rule_name=rule.name,
    )
    if rule.emit_rule_meta:
        expr = _apply_rule_meta_ibis(expr, rule)
    expr = _apply_kernel_specs_ibis(expr, rule.post_kernels, registry=context.registry)
    expr = align_table_to_schema(expr, schema=context.output_schema)
    plan = IbisPlan(expr=expr, ordering=Ordering.unordered())
    metrics_plan = (
        None if metrics is None else IbisPlan(expr=metrics, ordering=Ordering.unordered())
    )
    return plan, metrics_plan


def _interval_align_inputs(
    left: Table,
    right: Table,
    cfg: IntervalAlignConfig,
) -> _IntervalAlignInputs:
    left_keep = list(cfg.select_left) if cfg.select_left else list(left.columns)
    right_keep = list(cfg.select_right) if cfg.select_right else list(right.columns)
    left_expr = _ensure_span_columns(left, cfg.left_path_col, cfg.left_start_col, cfg.left_end_col)
    right_expr = _ensure_span_columns(
        right,
        cfg.right_path_col,
        cfg.right_start_col,
        cfg.right_end_col,
    )
    right_expr, right_map = _rename_right_columns(right_expr, set(left_expr.columns), "__r")
    left_id_col = _unique_name("left_id", set(left_expr.columns) | set(right_expr.columns))
    left_expr = left_expr.mutate(
        **{left_id_col: ibis.row_number().over(_left_id_window(left_expr, cfg))}
    )
    return _IntervalAlignInputs(
        left_expr=left_expr,
        right_expr=right_expr,
        right_map=right_map,
        left_id_col=left_id_col,
        right_path=_right_name(cfg.right_path_col, right_map),
        right_start=_right_name(cfg.right_start_col, right_map),
        right_end=_right_name(cfg.right_end_col, right_map),
        left_keep=left_keep,
        right_keep=right_keep,
    )


def _interval_align_matches(
    inputs: _IntervalAlignInputs,
    cfg: IntervalAlignConfig,
) -> _IntervalMatchResult:
    joined = inputs.left_expr.join(
        inputs.right_expr,
        predicates=[
            inputs.left_expr[cfg.left_path_col].cast("string")
            == inputs.right_expr[inputs.right_path].cast("string")
        ],
        how="inner",
    )
    match_mask = _interval_match_mask(
        cfg,
        left_start=_span_value(joined[cfg.left_start_col]),
        left_end=_span_value(joined[cfg.left_end_col]),
        right_start=_span_value(joined[inputs.right_start]),
        right_end=_span_value(joined[inputs.right_end]),
    )
    matched = joined.filter(match_mask)
    score_col = (
        cfg.match_score_col
        if cfg.emit_match_meta
        else _unique_name("match_score", set(matched.columns))
    )
    match_score = _interval_match_score(matched[inputs.right_start], matched[inputs.right_end])
    matched = matched.mutate(**{score_col: match_score})
    if cfg.emit_match_meta:
        matched = matched.mutate(**{cfg.match_kind_col: ibis.literal(cfg.mode)})
    candidate_col = _unique_name("candidate_count", set(matched.columns))
    count_window = ibis.window(group_by=[matched[inputs.left_id_col]])
    matched = matched.mutate(**{candidate_col: ibis.count().over(count_window)})
    winners = _winner_by_score(
        matched,
        spec=_WinnerByScoreSpec(
            group_key=inputs.left_id_col,
            score_col=score_col,
            score_order=_normalize_sort_order("descending"),
            tie_breakers=_interval_tie_breakers(cfg, inputs.right_map),
        ),
    )
    return _IntervalMatchResult(
        matched=matched,
        winners=winners,
        score_col=score_col,
        candidate_col=candidate_col,
    )


def _interval_align_expr(
    left: Table,
    right: Table,
    *,
    cfg: IntervalAlignConfig,
    rule_name: str,
) -> tuple[Table, Table | None]:
    inputs = _interval_align_inputs(left, right, cfg)
    matches = _interval_align_matches(inputs, cfg)
    output_names = _unique_columns([*inputs.left_keep, *inputs.right_keep])
    if cfg.emit_match_meta:
        output_names = _unique_columns(
            [
                *output_names,
                cfg.match_kind_col,
                cfg.match_score_col,
            ]
        )
    matched_output = _interval_output(
        matches.winners,
        spec=_IntervalOutputSpec(
            output_names=output_names,
            left_keep=set(inputs.left_keep),
            right_keep=set(inputs.right_keep),
            right_map=inputs.right_map,
            cfg=cfg,
            score_col=matches.score_col,
        ),
    )
    if cfg.how != "left":
        metrics = _interval_align_metrics(
            inputs.left_expr,
            matches.matched,
            matches.winners,
            spec=_IntervalAlignMetricsSpec(
                left_id_col=inputs.left_id_col,
                candidate_col=matches.candidate_col,
                rule_name=rule_name,
            ),
        )
        return matched_output, metrics
    matched_ids = matches.winners.select(
        _match_left_id=matches.winners[inputs.left_id_col]
    ).distinct()
    left_only = inputs.left_expr.filter(
        ~inputs.left_expr[inputs.left_id_col].isin(matched_ids["_match_left_id"])
    )
    if cfg.emit_match_meta:
        left_only = left_only.mutate(
            **{
                cfg.match_kind_col: _left_only_kind(left_only, cfg),
                cfg.match_score_col: ibis.null(),
            }
        )
    left_output = _interval_output(
        left_only,
        spec=_IntervalOutputSpec(
            output_names=output_names,
            left_keep=set(inputs.left_keep),
            right_keep=set(inputs.right_keep),
            right_map=inputs.right_map,
            cfg=cfg,
            score_col=matches.score_col,
        ),
    )
    combined = matched_output.union(left_output)
    metrics = _interval_align_metrics(
        inputs.left_expr,
        matches.matched,
        matches.winners,
        spec=_IntervalAlignMetricsSpec(
            left_id_col=inputs.left_id_col,
            candidate_col=matches.candidate_col,
            rule_name=rule_name,
        ),
    )
    return combined, metrics


def _winner_select_expr(
    table: Table,
    *,
    cfg: WinnerSelectConfig,
    rule_name: str,
) -> tuple[Table, Table | None]:
    if not cfg.keys:
        return table, None
    if cfg.score_col not in table.columns:
        msg = f"WinnerSelectConfig score column missing: {cfg.score_col!r}."
        raise ValueError(msg)
    missing_keys = [key for key in cfg.keys if key not in table.columns]
    if missing_keys:
        msg = f"WinnerSelectConfig keys missing from table: {missing_keys!r}."
        raise ValueError(msg)
    score_order = _normalize_sort_order(cfg.score_order)
    order_by = [_sort_expr(table, SortKey(cfg.score_col, score_order))]
    order_by.extend(
        _sort_expr(table, key) for key in cfg.tie_breakers if key.column in table.columns
    )
    window = ibis.window(group_by=[table[key] for key in cfg.keys], order_by=order_by)
    row_number_col = _unique_name("row_number", set(table.columns))
    ranked = table.mutate(**{row_number_col: ibis.row_number().over(window)})
    winners = ranked.filter(ranked[row_number_col] == ibis.literal(1)).drop(row_number_col)
    metrics = _winner_select_metrics(
        table,
        winners,
        keys=cfg.keys,
        rule_name=rule_name,
    )
    return winners, metrics


def _interval_match_mask(
    cfg: IntervalAlignConfig,
    *,
    left_start: Value,
    left_end: Value,
    right_start: Value,
    right_end: Value,
) -> BooleanValue:
    if cfg.mode == "EXACT":
        return (right_start == left_start) & (right_end == left_end)
    if cfg.mode == "CONTAINED_BEST":
        return (right_start >= left_start) & (right_end <= left_end)
    return (right_end > left_start) & (right_start < left_end)


def _interval_match_score(right_start: Value, right_end: Value) -> Value:
    end_val = cast("NumericValue", right_end.cast("int64"))
    start_val = cast("NumericValue", right_start.cast("int64"))
    span_len = end_val - start_val
    span_len = cast(
        "NumericValue",
        ibis.ifelse(span_len < ibis.literal(0), ibis.literal(0), span_len),
    )
    negative = cast("NumericValue", ibis.literal(-1, type="int64"))
    return (span_len * negative).cast("float64")


def _interval_tie_breakers(
    cfg: IntervalAlignConfig,
    right_map: Mapping[str, str],
) -> list[SortKey]:
    return [SortKey(_right_name(key.column, right_map), key.order) for key in cfg.tie_breakers]


def _winner_by_score(table: Table, *, spec: _WinnerByScoreSpec) -> Table:
    order_by = [_sort_expr(table, SortKey(spec.score_col, spec.score_order))]
    order_by.extend(
        _sort_expr(table, key) for key in spec.tie_breakers if key.column in table.columns
    )
    window = ibis.window(group_by=[table[spec.group_key]], order_by=order_by)
    row_number_col = _unique_name("row_number", set(table.columns))
    ranked = table.mutate(**{row_number_col: ibis.row_number().over(window)})
    return ranked.filter(ranked[row_number_col] == ibis.literal(1)).drop(row_number_col)


def _interval_output(table: Table, *, spec: _IntervalOutputSpec) -> Table:
    exprs: list[Value] = []
    for name in spec.output_names:
        if spec.cfg.emit_match_meta and name == spec.cfg.match_kind_col:
            exprs.append(_value_or_null(table, spec.cfg.match_kind_col).name(name))
            continue
        if spec.cfg.emit_match_meta and name == spec.cfg.match_score_col:
            exprs.append(_value_or_null(table, spec.score_col).name(name))
            continue
        if name in spec.right_keep and name not in spec.left_keep:
            exprs.append(_value_or_null(table, _right_name(name, spec.right_map)).name(name))
            continue
        exprs.append(_value_or_null(table, name).name(name))
    return table.select(exprs)


def _left_only_kind(left_only: Table, cfg: IntervalAlignConfig) -> Value:
    has_path = left_only[cfg.left_path_col].notnull()
    has_span = left_only[cfg.left_start_col].notnull() & left_only[cfg.left_end_col].notnull()
    return ibis.ifelse(
        has_path & has_span,
        ibis.literal("NO_MATCH"),
        ibis.literal("NO_PATH_OR_SPAN"),
    )


def _interval_align_metrics(
    left: Table,
    matched: Table,
    winners: Table,
    *,
    spec: _IntervalAlignMetricsSpec,
) -> Table:
    candidate_counts = matched.select(
        left_id=matched[spec.left_id_col],
        candidate_count=matched[spec.candidate_col],
    ).distinct()
    metrics = candidate_counts.aggregate(
        matched_left=candidate_counts.count(),
        ambiguous_left=ibis.sum(
            ibis.ifelse(
                candidate_counts.candidate_count > ibis.literal(1),
                ibis.literal(1),
                ibis.literal(0),
            )
        ),
        max_candidates=candidate_counts.candidate_count.max(),
        avg_candidates=ibis.mean(candidate_counts.candidate_count),
    )
    total_left = left.count()
    winner_rows = winners.count()
    candidate_rows = matched.count()
    return metrics.mutate(
        rule_name=ibis.literal(spec.rule_name),
        total_left=total_left,
        candidate_rows=candidate_rows,
        winner_rows=winner_rows,
        winner_rate=_safe_div(winner_rows, total_left),
        ambiguity_rate=_safe_div(metrics.ambiguous_left, metrics.matched_left),
    )


def _winner_select_metrics(
    table: Table,
    winners: Table,
    *,
    keys: Sequence[str],
    rule_name: str,
) -> Table:
    candidate_counts = table.group_by([table[key] for key in keys]).aggregate(
        candidate_count=table.count()
    )
    metrics = candidate_counts.aggregate(
        total_groups=candidate_counts.count(),
        ambiguous_groups=ibis.sum(
            ibis.ifelse(
                candidate_counts.candidate_count > ibis.literal(1),
                ibis.literal(1),
                ibis.literal(0),
            )
        ),
        max_candidates=candidate_counts.candidate_count.max(),
        avg_candidates=ibis.mean(candidate_counts.candidate_count),
    )
    winner_rows = winners.count()
    return metrics.mutate(
        rule_name=ibis.literal(rule_name),
        winner_rows=winner_rows,
        winner_rate=_safe_div(winner_rows, metrics.total_groups),
        ambiguity_rate=_safe_div(metrics.ambiguous_groups, metrics.total_groups),
    )


def _safe_div(numerator: Value, denominator: Value) -> Value:
    denom = cast("NumericValue", denominator.cast("float64"))
    num = cast("NumericValue", numerator.cast("float64"))
    return ibis.ifelse(
        denom == ibis.literal(0),
        ibis.literal(None, type="float64"),
        num / denom,
    )


def _left_id_window(table: Table, cfg: IntervalAlignConfig) -> object:
    order_by: list[IbisValue] = []
    if cfg.left_path_col in table.columns:
        order_by.append(table[cfg.left_path_col].asc())
    if cfg.left_start_col in table.columns:
        order_by.append(table[cfg.left_start_col].asc())
    if cfg.left_end_col in table.columns:
        order_by.append(table[cfg.left_end_col].asc())
    if not order_by:
        order_by.append(cast("IbisValue", ibis.literal(1)))
    return ibis.window(order_by=order_by)


def _ensure_span_columns(table: Table, path: str, start: str, end: str) -> Table:
    expr = _ensure_typed_column(table, path, "string")
    expr = _ensure_typed_column(expr, start, "int64")
    return _ensure_typed_column(expr, end, "int64")


def _ensure_typed_column(table: Table, name: str, dtype: str) -> Table:
    if name in table.columns:
        return table
    return table.mutate(**{name: ibis.literal(None, type=dtype)})


def _rename_right_columns(
    right: Table, left_columns: set[str], suffix: str
) -> tuple[Table, dict[str, str]]:
    rename: dict[str, str] = {}
    existing = set(right.columns) | set(left_columns)
    for name in right.columns:
        if name not in left_columns:
            continue
        candidate = f"{name}{suffix}"
        idx = 1
        while candidate in existing:
            candidate = f"{name}{suffix}_{idx}"
            idx += 1
        rename[name] = candidate
        existing.add(candidate)
    return (right.rename(rename) if rename else right, rename)


def _right_name(name: str, right_map: Mapping[str, str]) -> str:
    return right_map.get(name, name)


def _unique_columns(names: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for name in names:
        if name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def _unique_name(base: str, existing: set[str]) -> str:
    candidate = base
    idx = 1
    while candidate in existing:
        candidate = f"{base}_{idx}"
        idx += 1
    existing.add(candidate)
    return candidate


def _span_value(expr: Value) -> Value:
    return expr.cast("int64")


def _value_or_null(table: Table, name: str) -> Value:
    if name in table.columns:
        return table[name]
    return ibis.null()


__all__ = [
    "CatalogPlanResolver",
    "RelationPlanBundle",
    "RelationPlanCompileOptions",
    "compile_relation_plans",
    "compile_relation_plans_ibis",
]
