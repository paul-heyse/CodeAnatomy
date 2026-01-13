"""Compile relationship rules into executable plans and kernels."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa

from arrowdsl.compute.kernels import (
    IntervalAlignOptions,
    apply_dedupe,
    explode_list_column,
    interval_align_table,
)
from arrowdsl.core.context import DeterminismTier, ExecutionContext, Ordering
from arrowdsl.core.interop import ComputeExpression, TableLike
from arrowdsl.finalize.finalize import Contract, FinalizeResult
from arrowdsl.plan.ops import DedupeSpec, SortKey
from arrowdsl.plan.plan import Plan, hash_join, union_all_plans
from arrowdsl.plan.query import open_dataset
from arrowdsl.schema.arrays import ConstExpr, FieldExpr, const_array, set_or_append_column
from arrowdsl.schema.schema import SchemaEvolutionSpec, projection_for_schema
from relspec.edge_contract_validator import (
    EdgeContractValidationConfig,
    validate_relationship_output_contracts_for_edge_kinds,
)
from relspec.model import (
    AddLiteralSpec,
    CanonicalSortKernelSpec,
    DatasetRef,
    DedupeKernelSpec,
    DropColumnsSpec,
    ExplodeListSpec,
    KernelSpecT,
    ProjectConfig,
    RelationshipRule,
    RenameColumnsSpec,
    RuleKind,
)
from relspec.registry import ContractCatalog, DatasetCatalog
from schema_spec.system import (
    GLOBAL_SCHEMA_REGISTRY,
    dataset_spec_from_contract,
    make_dataset_spec,
    table_spec_from_schema,
)

type KernelFn = Callable[[TableLike, ExecutionContext], TableLike]


class PlanResolver(Protocol):
    """Resolve a ``DatasetRef`` to an executable plan."""

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> Plan:
        """Resolve a dataset reference into a plan.

        Parameters
        ----------
        ref:
            Dataset reference to resolve.
        ctx:
            Execution context.

        Returns
        -------
        Plan
            Executable plan for the dataset reference.
        """
        ...


def _scan_ordering_for_ctx(ctx: ExecutionContext) -> Ordering:
    """Return ordering metadata for scan output based on context.

    Parameters
    ----------
    ctx:
        Execution context.

    Returns
    -------
    Ordering
        Ordering marker for scan output.
    """
    if ctx.runtime.scan.implicit_ordering or ctx.runtime.scan.require_sequenced_output:
        return Ordering.implicit()
    return Ordering.unordered()


class FilesystemPlanResolver:
    """Resolve dataset names to filesystem-backed Arrow datasets."""

    def __init__(self, catalog: DatasetCatalog) -> None:
        self.catalog = catalog

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> Plan:
        """Resolve a dataset reference into a filesystem-backed plan.

        Parameters
        ----------
        ref:
            Dataset reference to resolve.
        ctx:
            Execution context.

        Returns
        -------
        Plan
            Executable plan for the dataset reference.
        """
        loc = self.catalog.get(ref.name)
        dataset = open_dataset(
            loc.path,
            dataset_format=loc.format,
            filesystem=loc.filesystem,
            partitioning=loc.partitioning,
        )

        dataset_spec = loc.dataset_spec
        if dataset_spec is None:
            if loc.table_spec is not None:
                table_spec = loc.table_spec
            else:
                table_spec = table_spec_from_schema(ref.name, dataset.schema)
            dataset_spec = make_dataset_spec(table_spec=table_spec, query_spec=ref.query)
        elif ref.query is not None:
            dataset_spec = make_dataset_spec(
                table_spec=dataset_spec.table_spec,
                contract_spec=dataset_spec.contract_spec,
                query_spec=ref.query,
                evolution_spec=dataset_spec.evolution_spec,
                metadata_spec=dataset_spec.metadata_spec,
                validation=dataset_spec.validation,
            )
        decl = dataset_spec.scan_context(dataset, ctx).acero_decl()
        label = ref.label or ref.name
        return Plan(decl=decl, label=label, ordering=_scan_ordering_for_ctx(ctx))


class InMemoryPlanResolver:
    """Resolve dataset names to in-memory tables or plans."""

    def __init__(self, mapping: Mapping[str, TableLike | Plan]) -> None:
        self.mapping = dict(mapping)

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> Plan:
        """Resolve a dataset reference into an in-memory plan.

        Parameters
        ----------
        ref:
            Dataset reference to resolve.
        ctx:
            Execution context (unused for in-memory resolution).

        Returns
        -------
        Plan
            Executable plan for the dataset reference.

        Raises
        ------
        KeyError
            Raised when the dataset reference is unknown.
        """
        _ = ctx
        obj = self.mapping.get(ref.name)
        if obj is None:
            msg = f"InMemoryPlanResolver: unknown dataset {ref.name!r}."
            raise KeyError(msg)
        if isinstance(obj, Plan):
            return obj
        return Plan.table_source(obj, label=ref.label or ref.name)


def _build_rule_meta_kernel(rule: RelationshipRule) -> KernelFn:
    def _add_rule_meta(table: TableLike, _ctx: ExecutionContext) -> TableLike:
        count = table.num_rows
        if rule.rule_name_col not in table.column_names:
            table = set_or_append_column(
                table,
                rule.rule_name_col,
                const_array(count, rule.name, dtype=pa.string()),
            )
        if rule.rule_priority_col not in table.column_names:
            table = set_or_append_column(
                table,
                rule.rule_priority_col,
                const_array(count, int(rule.priority), dtype=pa.int32()),
            )
        return table

    return _add_rule_meta


def _build_add_literal_kernel(spec: AddLiteralSpec) -> KernelFn:
    def _fn(table: TableLike, _ctx: ExecutionContext) -> TableLike:
        if spec.name in table.column_names:
            return table
        return set_or_append_column(table, spec.name, const_array(table.num_rows, spec.value))

    return _fn


def _build_drop_columns_kernel(spec: DropColumnsSpec) -> KernelFn:
    def _fn(table: TableLike, _ctx: ExecutionContext) -> TableLike:
        cols = [col for col in spec.columns if col in table.column_names]
        return table.drop(cols) if cols else table

    return _fn


def _build_rename_columns_kernel(spec: RenameColumnsSpec) -> KernelFn:
    def _fn(table: TableLike, _ctx: ExecutionContext) -> TableLike:
        if not spec.mapping:
            return table
        names = list(table.column_names)
        new_names = [spec.mapping.get(name, name) for name in names]
        return table.rename_columns(new_names)

    return _fn


def _build_explode_list_kernel(spec: ExplodeListSpec) -> KernelFn:
    def _fn(table: TableLike, _ctx: ExecutionContext) -> TableLike:
        return explode_list_column(
            table,
            parent_id_col=spec.parent_id_col,
            list_col=spec.list_col,
            out_parent_col=spec.out_parent_col,
            out_value_col=spec.out_value_col,
        )

    return _fn


def _build_dedupe_kernel(spec: DedupeKernelSpec) -> KernelFn:
    def _fn(table: TableLike, ctx: ExecutionContext) -> TableLike:
        return apply_dedupe(table, spec=spec.spec, _ctx=ctx)

    return _fn


def _kernel_from_spec(spec: object) -> KernelFn:
    if isinstance(spec, AddLiteralSpec):
        return _build_add_literal_kernel(spec)
    if isinstance(spec, DropColumnsSpec):
        return _build_drop_columns_kernel(spec)
    if isinstance(spec, RenameColumnsSpec):
        return _build_rename_columns_kernel(spec)
    if isinstance(spec, ExplodeListSpec):
        return _build_explode_list_kernel(spec)
    if isinstance(spec, DedupeKernelSpec):
        return _build_dedupe_kernel(spec)
    if isinstance(spec, CanonicalSortKernelSpec):
        msg = "CanonicalSortKernelSpec is deprecated; use contract canonical_sort in finalize."
        raise TypeError(msg)
    msg = f"Unknown KernelSpec type: {type(spec).__name__}."
    raise ValueError(msg)


def _compile_post_kernels(specs: Sequence[KernelSpecT]) -> tuple[KernelFn, ...]:
    """Compile post-kernel specifications into callables.

    Parameters
    ----------
    specs:
        Post-kernel specs to compile.

    Returns
    -------
    tuple[KernelFn, ...]
        Post-kernel functions.
    """
    return tuple(_kernel_from_spec(spec) for spec in specs)


def _apply_add_literal_to_plan(
    plan: Plan,
    spec: AddLiteralSpec,
    *,
    ctx: ExecutionContext,
    rule: RelationshipRule,
) -> Plan:
    schema = plan.schema(ctx=ctx)
    names = list(schema.names)
    if spec.name in names:
        return plan
    exprs = [FieldExpr(name=name).to_expression() for name in names]
    exprs.append(ConstExpr(value=spec.value).to_expression())
    names.append(spec.name)
    return plan.project(exprs, names, label=plan.label or rule.name)


def _apply_drop_columns_to_plan(
    plan: Plan,
    spec: DropColumnsSpec,
    *,
    ctx: ExecutionContext,
    rule: RelationshipRule,
) -> Plan:
    schema = plan.schema(ctx=ctx)
    keep = [name for name in schema.names if name not in spec.columns]
    if keep == list(schema.names):
        return plan
    exprs = [FieldExpr(name=name).to_expression() for name in keep]
    return plan.project(exprs, keep, label=plan.label or rule.name)


def _apply_rename_columns_to_plan(
    plan: Plan,
    spec: RenameColumnsSpec,
    *,
    ctx: ExecutionContext,
    rule: RelationshipRule,
) -> Plan:
    if not spec.mapping:
        return plan
    schema = plan.schema(ctx=ctx)
    names = list(schema.names)
    renamed = [spec.mapping.get(name, name) for name in names]
    if renamed == names:
        return plan
    exprs = [FieldExpr(name=name).to_expression() for name in names]
    return plan.project(exprs, renamed, label=plan.label or rule.name)


def _apply_dedupe_to_plan(
    plan: Plan,
    spec: DedupeKernelSpec,
    *,
    ctx: ExecutionContext,
    rule: RelationshipRule,
) -> tuple[Plan, bool]:
    strategy = spec.spec.strategy
    if strategy not in {"KEEP_ARBITRARY", "COLLAPSE_LIST"}:
        return plan, False
    if not spec.spec.keys:
        return plan, True
    schema = plan.schema(ctx=ctx)
    names = list(schema.names)
    if any(key not in names for key in spec.spec.keys):
        return plan, False
    non_keys = [name for name in names if name not in spec.spec.keys]
    if not non_keys:
        return plan, False
    agg_fn = "first" if strategy == "KEEP_ARBITRARY" else "list"
    plan = plan.aggregate(group_keys=list(spec.spec.keys), aggs=[(col, agg_fn) for col in non_keys])
    agg_names = [f"{col}_{agg_fn}" for col in non_keys]
    exprs = [FieldExpr(name=name).to_expression() for name in spec.spec.keys]
    exprs.extend(FieldExpr(name=name).to_expression() for name in agg_names)
    out_names = list(spec.spec.keys) + non_keys
    return plan.project(exprs, out_names, label=plan.label or rule.name), True


def _apply_plan_kernel_specs(
    plan: Plan,
    specs: Sequence[KernelSpecT],
    *,
    ctx: ExecutionContext,
    rule: RelationshipRule,
) -> tuple[Plan, tuple[KernelSpecT, ...]]:
    if plan.decl is None or not specs:
        return plan, tuple(specs)
    remaining: list[KernelSpecT] = []
    for idx, spec in enumerate(specs):
        if isinstance(spec, AddLiteralSpec):
            plan = _apply_add_literal_to_plan(plan, spec, ctx=ctx, rule=rule)
            continue
        if isinstance(spec, DropColumnsSpec):
            plan = _apply_drop_columns_to_plan(plan, spec, ctx=ctx, rule=rule)
            continue
        if isinstance(spec, RenameColumnsSpec):
            plan = _apply_rename_columns_to_plan(plan, spec, ctx=ctx, rule=rule)
            continue
        if isinstance(spec, DedupeKernelSpec):
            plan, applied = _apply_dedupe_to_plan(plan, spec, ctx=ctx, rule=rule)
            if applied:
                continue
        remaining = list(specs[idx:])
        break
    else:
        remaining = []
    return plan, tuple(remaining)


def _apply_project_to_plan(
    plan: Plan, project: ProjectConfig | None, *, rule: RelationshipRule
) -> Plan:
    """Apply a projection in the plan lane when configured.

    Parameters
    ----------
    plan:
        Input plan.
    project:
        Optional projection configuration.
    rule:
        Rule providing a fallback label.

    Returns
    -------
    Plan
        Projected plan when projection is configured; otherwise the original plan.
    """
    if project is None:
        return plan

    if not project.select and not project.exprs:
        return plan

    exprs: list[ComputeExpression] = []
    names: list[str] = []

    for col in project.select:
        exprs.append(FieldExpr(name=col).to_expression())
        names.append(col)

    for name, expr in project.exprs.items():
        exprs.append(expr.to_expression())
        names.append(name)

    if not exprs:
        return plan

    return plan.project(exprs, names, label=plan.label or rule.name)


def _apply_rule_meta_to_plan(
    plan: Plan,
    *,
    rule: RelationshipRule,
    ctx: ExecutionContext,
) -> Plan:
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


def _align_plan(plan: Plan, *, schema: pa.Schema, ctx: ExecutionContext) -> Plan:
    exprs, names = projection_for_schema(
        schema,
        available=plan.schema(ctx=ctx).names,
        safe_cast=ctx.safe_cast,
    )
    return plan.project(exprs, names, label=plan.label)


def _union_all_plans_aligned(plans: Sequence[Plan], *, ctx: ExecutionContext) -> Plan:
    schema = SchemaEvolutionSpec().unify_schema_from_schemas(
        [plan.schema(ctx=ctx) for plan in plans]
    )
    aligned = [_align_plan(plan, schema=schema, ctx=ctx) for plan in plans]
    return union_all_plans(aligned, label="relspec_union")


@dataclass(frozen=True)
class CompiledRule:
    """Executable compiled form of a rule."""

    rule: RelationshipRule
    plan: Plan | None
    execute_fn: Callable[[ExecutionContext, PlanResolver], TableLike] | None
    post_kernels: tuple[KernelFn, ...] = ()
    emit_rule_meta: bool = True

    def execute(self, *, ctx: ExecutionContext, resolver: PlanResolver) -> TableLike:
        """Execute the compiled rule into a table.

        Parameters
        ----------
        ctx:
            Execution context.
        resolver:
            Plan resolver for dataset references.

        Returns
        -------
        pyarrow.Table
            Rule output table.

        Raises
        ------
        RuntimeError
            Raised when no execution path is available.
        """
        if self.execute_fn is not None:
            table = self.execute_fn(ctx, resolver)
            if self.emit_rule_meta:
                table = _build_rule_meta_kernel(self.rule)(table, ctx)
            for fn in self.post_kernels:
                table = fn(table, ctx)
            return table
        if self.plan is None:
            msg = "CompiledRule has neither plan nor execute_fn."
            raise RuntimeError(msg)
        table = self.plan.to_table(ctx=ctx)
        if self.emit_rule_meta:
            table = _build_rule_meta_kernel(self.rule)(table, ctx)
        for fn in self.post_kernels:
            table = fn(table, ctx)
        return table


@dataclass(frozen=True)
class CompiledOutput:
    """Compiled output with contributing rules."""

    output_dataset: str
    contract_name: str | None
    contributors: tuple[CompiledRule, ...] = ()

    def execute(
        self, *, ctx: ExecutionContext, resolver: PlanResolver, contracts: ContractCatalog
    ) -> FinalizeResult:
        """Execute contributing rules and finalize against the output contract.

        Parameters
        ----------
        ctx:
            Execution context.
        resolver:
            Plan resolver for dataset references.
        contracts:
            Contract catalog for finalization.

        Returns
        -------
        FinalizeResult
            Finalized tables for the output dataset.

        Raises
        ------
        ValueError
            Raised when the output has no contributors.
        """
        ctx_exec = ctx
        if ctx.determinism == DeterminismTier.CANONICAL and not ctx.provenance:
            ctx_exec = ctx.with_provenance(provenance=True)

        if not self.contributors:
            msg = f"CompiledOutput {self.output_dataset!r} has no contributors."
            raise ValueError(msg)

        plan_parts: list[Plan] = []
        table_parts: list[TableLike] = []
        for cr in self.contributors:
            if cr.execute_fn is not None:
                table_parts.append(cr.execute(ctx=ctx_exec, resolver=resolver))
                continue
            if cr.plan is None or cr.plan.decl is None:
                table_parts.append(cr.execute(ctx=ctx_exec, resolver=resolver))
                continue
            if cr.post_kernels:
                table_parts.append(cr.execute(ctx=ctx_exec, resolver=resolver))
                continue
            plan_parts.append(_apply_rule_meta_to_plan(cr.plan, rule=cr.rule, ctx=ctx_exec))

        if plan_parts:
            union = union_all_plans(plan_parts, label=self.output_dataset)
            table_parts.append(union.to_table(ctx=ctx_exec))

        if self.contract_name is None:
            unioned = SchemaEvolutionSpec().unify_and_cast(
                table_parts,
                safe_cast=ctx_exec.safe_cast,
                on_error="unsafe" if ctx_exec.safe_cast else "raise",
                keep_extra_columns=ctx_exec.provenance,
            )
            dummy = Contract(name=f"{self.output_dataset}_NO_CONTRACT", schema=unioned.schema)
            finalize_ctx = dataset_spec_from_contract(dummy).finalize_context(ctx_exec)
            return finalize_ctx.run(unioned, ctx=ctx_exec)

        contract = contracts.get(self.contract_name)
        dataset_spec = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(contract.name)
        if dataset_spec is None:
            dataset_spec = dataset_spec_from_contract(contract)
        unioned = dataset_spec.unify_tables(table_parts, ctx=ctx_exec)
        return dataset_spec.finalize_context(ctx_exec).run(unioned, ctx=ctx_exec)


class RelationshipRuleCompiler:
    """Compile ``RelationshipRule`` instances into executable units."""

    def __init__(self, *, resolver: PlanResolver) -> None:
        self.resolver = resolver

    def _compile_filter_project(
        self,
        rule: RelationshipRule,
        *,
        ctx: ExecutionContext,
        post_kernels: tuple[KernelSpecT, ...],
    ) -> CompiledRule:
        src = rule.inputs[0]
        plan = self.resolver.resolve(src, ctx=ctx)
        plan = _apply_project_to_plan(plan, rule.project, rule=rule)
        plan, remaining = _apply_plan_kernel_specs(plan, post_kernels, ctx=ctx, rule=rule)
        return CompiledRule(
            rule=rule,
            plan=plan,
            execute_fn=None,
            post_kernels=_compile_post_kernels(remaining),
            emit_rule_meta=rule.emit_rule_meta,
        )

    def _compile_hash_join(
        self,
        rule: RelationshipRule,
        *,
        ctx: ExecutionContext,
        post_kernels: tuple[KernelSpecT, ...],
    ) -> CompiledRule:
        if rule.hash_join is None:
            msg = "HASH_JOIN rules require hash_join config."
            raise ValueError(msg)
        left_ref, right_ref = rule.inputs
        left_plan = self.resolver.resolve(left_ref, ctx=ctx)
        right_plan = self.resolver.resolve(right_ref, ctx=ctx)

        join_plan = hash_join(
            left=left_plan,
            right=right_plan,
            spec=rule.hash_join.to_join_spec(),
            label=rule.name,
        )
        join_plan = _apply_project_to_plan(join_plan, rule.project, rule=rule)
        join_plan, remaining = _apply_plan_kernel_specs(join_plan, post_kernels, ctx=ctx, rule=rule)
        return CompiledRule(
            rule=rule,
            plan=join_plan,
            execute_fn=None,
            post_kernels=_compile_post_kernels(remaining),
            emit_rule_meta=rule.emit_rule_meta,
        )

    def _compile_passthrough(
        self,
        rule: RelationshipRule,
        *,
        ctx: ExecutionContext,
        post_kernels: tuple[KernelSpecT, ...],
    ) -> CompiledRule:
        src = rule.inputs[0]
        plan = self.resolver.resolve(src, ctx=ctx)
        plan, remaining = _apply_plan_kernel_specs(plan, post_kernels, ctx=ctx, rule=rule)
        return CompiledRule(
            rule=rule,
            plan=plan,
            execute_fn=None,
            post_kernels=_compile_post_kernels(remaining),
            emit_rule_meta=rule.emit_rule_meta,
        )

    def _compile_union_all(
        self,
        rule: RelationshipRule,
        *,
        ctx: ExecutionContext,
        post_kernels: tuple[KernelSpecT, ...],
    ) -> CompiledRule:
        plans = [self.resolver.resolve(inp, ctx=ctx) for inp in rule.inputs]
        if all(plan.decl is not None for plan in plans):
            aligned = _union_all_plans_aligned(plans, ctx=ctx)
            return CompiledRule(
                rule=rule,
                plan=aligned,
                execute_fn=None,
                post_kernels=_compile_post_kernels(post_kernels),
                emit_rule_meta=rule.emit_rule_meta,
            )

        def _exec(ctx2: ExecutionContext, resolver: PlanResolver) -> TableLike:
            parts: list[TableLike] = []
            for inp in rule.inputs:
                plan = resolver.resolve(inp, ctx=ctx2)
                parts.append(plan.to_table(ctx=ctx2))
            dataset_spec = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(rule.output_dataset)
            if dataset_spec is not None:
                return dataset_spec.unify_tables(parts, ctx=ctx2)
            return SchemaEvolutionSpec().unify_and_cast(
                parts,
                safe_cast=ctx2.safe_cast,
                on_error="unsafe" if ctx2.safe_cast else "raise",
                keep_extra_columns=ctx2.provenance,
            )

        return CompiledRule(
            rule=rule,
            plan=None,
            execute_fn=_exec,
            post_kernels=_compile_post_kernels(post_kernels),
            emit_rule_meta=rule.emit_rule_meta,
        )

    @staticmethod
    def _compile_interval_align(
        rule: RelationshipRule,
        *,
        ctx: ExecutionContext,
        post_kernels: tuple[KernelSpecT, ...],
    ) -> CompiledRule:
        cfg = rule.interval_align
        if cfg is None:
            msg = "INTERVAL_ALIGN rules require interval_align config."
            raise ValueError(msg)
        interval_cfg = IntervalAlignOptions(
            mode=cfg.mode,
            how=cfg.how,
            left_path_col=cfg.left_path_col,
            left_start_col=cfg.left_start_col,
            left_end_col=cfg.left_end_col,
            right_path_col=cfg.right_path_col,
            right_start_col=cfg.right_start_col,
            right_end_col=cfg.right_end_col,
            select_left=cfg.select_left,
            select_right=cfg.select_right,
            tie_breakers=cfg.tie_breakers,
            emit_match_meta=cfg.emit_match_meta,
            match_kind_col=cfg.match_kind_col,
            match_score_col=cfg.match_score_col,
        )

        def _exec(ctx2: ExecutionContext, resolver: PlanResolver) -> TableLike:
            left_ref, right_ref = rule.inputs
            lt = resolver.resolve(left_ref, ctx=ctx2).to_table(ctx=ctx2)
            rt = resolver.resolve(right_ref, ctx=ctx2).to_table(ctx=ctx2)
            return interval_align_table(lt, rt, cfg=interval_cfg)

        _ = ctx
        return CompiledRule(
            rule=rule,
            plan=None,
            execute_fn=_exec,
            post_kernels=_compile_post_kernels(post_kernels),
            emit_rule_meta=rule.emit_rule_meta,
        )

    @staticmethod
    def _compile_winner_select(
        rule: RelationshipRule,
        *,
        ctx: ExecutionContext,
        post_kernels: tuple[KernelSpecT, ...],
    ) -> CompiledRule:
        cfg = rule.winner_select
        if cfg is None:
            msg = "WINNER_SELECT rules require winner_select config."
            raise ValueError(msg)
        winner_cfg = cfg

        def _exec(ctx2: ExecutionContext, resolver: PlanResolver) -> TableLike:
            src = rule.inputs[0]
            table = resolver.resolve(src, ctx=ctx2).to_table(ctx=ctx2)
            spec = DedupeSpec(
                keys=winner_cfg.keys,
                strategy="KEEP_BEST_BY_SCORE",
                tie_breakers=(
                    SortKey(winner_cfg.score_col, winner_cfg.score_order),
                    *winner_cfg.tie_breakers,
                ),
            )
            return apply_dedupe(table, spec=spec, _ctx=ctx2)

        _ = ctx
        return CompiledRule(
            rule=rule,
            plan=None,
            execute_fn=_exec,
            post_kernels=_compile_post_kernels(post_kernels),
            emit_rule_meta=rule.emit_rule_meta,
        )

    def compile_rule(self, rule: RelationshipRule, *, ctx: ExecutionContext) -> CompiledRule:
        """Compile a single relationship rule into an executable unit.

        Parameters
        ----------
        rule:
            Rule to compile.
        ctx:
            Execution context.

        Returns
        -------
        CompiledRule
            Compiled representation of the rule.

        Raises
        ------
        ValueError
            Raised when the rule kind is unknown.
        """
        handlers: dict[RuleKind, Callable[..., CompiledRule]] = {
            RuleKind.FILTER_PROJECT: self._compile_filter_project,
            RuleKind.HASH_JOIN: self._compile_hash_join,
            RuleKind.EXPLODE_LIST: self._compile_passthrough,
            RuleKind.WINNER_SELECT: self._compile_winner_select,
            RuleKind.UNION_ALL: self._compile_union_all,
            RuleKind.INTERVAL_ALIGN: self._compile_interval_align,
        }
        handler = handlers.get(rule.kind)
        if handler is None:
            msg = f"Unknown rule kind: {rule.kind}."
            raise ValueError(msg)
        return handler(rule, ctx=ctx, post_kernels=rule.post_kernels)

    def compile_registry(
        self,
        registry_rules: Sequence[RelationshipRule],
        *,
        ctx: ExecutionContext,
        contract_catalog: ContractCatalog | None = None,
        edge_validation: EdgeContractValidationConfig | None = None,
    ) -> dict[str, CompiledOutput]:
        """Compile relationship rules into executable outputs.

        Parameters
        ----------
        registry_rules:
            Rules to compile.
        ctx:
            Execution context.
        contract_catalog:
            Optional contract catalog for validation and finalization.
        edge_validation:
            Optional edge contract validation config.

        Returns
        -------
        dict[str, CompiledOutput]
            Compiled outputs keyed by output dataset name.

        Raises
        ------
        ValueError
            Raised when output rules disagree on contract_name.
        """
        if contract_catalog is not None:
            validate_relationship_output_contracts_for_edge_kinds(
                rules=registry_rules,
                contract_catalog=contract_catalog,
                config=edge_validation or EdgeContractValidationConfig(),
            )

        by_out: dict[str, list[RelationshipRule]] = {}
        for rule in registry_rules:
            by_out.setdefault(rule.output_dataset, []).append(rule)

        compiled: dict[str, CompiledOutput] = {}
        for out_name, rules in by_out.items():
            rules_sorted = sorted(rules, key=lambda rr: (rr.priority, rr.name))
            contract_names = {rr.contract_name for rr in rules_sorted}
            if len(contract_names) > 1:
                msg = (
                    f"Output {out_name!r} has inconsistent contract_name across rules: "
                    f"{contract_names}."
                )
                raise ValueError(msg)

            contributors = tuple(self.compile_rule(rule, ctx=ctx) for rule in rules_sorted)
            compiled[out_name] = CompiledOutput(
                output_dataset=out_name,
                contract_name=rules_sorted[0].contract_name,
                contributors=contributors,
            )
        return compiled
