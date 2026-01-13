"""Compile relationship rules into executable plans and kernels."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Protocol, cast

import pyarrow as pa

from arrowdsl.compute.filters import FilterSpec
from arrowdsl.compute.kernels import apply_dedupe, explode_list_column, interval_align_table
from arrowdsl.core.context import DeterminismTier, ExecutionContext, Ordering
from arrowdsl.core.interop import ComputeExpression, SchemaLike, TableLike
from arrowdsl.finalize.finalize import Contract, FinalizeResult
from arrowdsl.plan.ops import DedupeSpec, IntervalAlignOptions, SortKey
from arrowdsl.plan.plan import Plan, hash_join, union_all_plans
from arrowdsl.plan.query import open_dataset
from arrowdsl.plan.runner import run_plan
from arrowdsl.schema.build import ConstExpr, FieldExpr, const_array, set_or_append_column
from arrowdsl.schema.ops import align_plan
from arrowdsl.schema.schema import SchemaEvolutionSpec, SchemaMetadataSpec
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
    FilterKernelSpec,
    KernelSpecT,
    ProjectConfig,
    RelationshipRule,
    RenameColumnsSpec,
    RuleKind,
)
from relspec.policies import (
    ambiguity_policy_from_schema,
    confidence_policy_from_schema,
    default_tie_breakers,
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


def _rule_metadata_spec(rule: RelationshipRule) -> SchemaMetadataSpec:
    return SchemaMetadataSpec(
        schema_metadata={
            b"rule_name": rule.name.encode("utf-8"),
            b"rule_priority": str(rule.priority).encode("utf-8"),
        }
    )


def _apply_rule_metadata(
    table: TableLike,
    *,
    rule: RelationshipRule,
) -> TableLike:
    schema = _rule_metadata_spec(rule).apply(table.schema)
    return table.cast(schema)


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


def _build_filter_kernel(spec: FilterKernelSpec) -> KernelFn:
    def _fn(table: TableLike, _ctx: ExecutionContext) -> TableLike:
        predicate = FilterSpec(spec.predicate.to_expr_spec()).mask(table)
        return table.filter(predicate)

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
        resolved = _dedupe_spec_with_defaults(spec.spec, schema=table.schema)
        return apply_dedupe(table, spec=resolved, _ctx=ctx)

    return _fn


def _dedupe_spec_with_defaults(spec: DedupeSpec, *, schema: SchemaLike) -> DedupeSpec:
    if spec.strategy != "KEEP_BEST_BY_SCORE":
        return spec
    defaults = default_tie_breakers(schema)
    filtered_defaults = tuple(sk for sk in defaults if sk.column != "score")
    if spec.tie_breakers:
        if not filtered_defaults:
            return spec
        existing = {sk.column for sk in spec.tie_breakers}
        extra = tuple(sk for sk in filtered_defaults if sk.column not in existing)
        if not extra:
            return spec
        return DedupeSpec(
            keys=spec.keys,
            strategy=spec.strategy,
            tie_breakers=tuple(spec.tie_breakers) + extra,
        )
    if "score" not in schema.names:
        return spec
    return DedupeSpec(
        keys=spec.keys,
        strategy=spec.strategy,
        tie_breakers=(SortKey("score", "descending"), *filtered_defaults),
    )


def _kernel_from_spec(spec: object) -> KernelFn:
    if isinstance(spec, AddLiteralSpec):
        return _build_add_literal_kernel(spec)
    if isinstance(spec, DropColumnsSpec):
        return _build_drop_columns_kernel(spec)
    if isinstance(spec, FilterKernelSpec):
        return _build_filter_kernel(spec)
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


def _apply_filter_to_plan(
    plan: Plan,
    spec: FilterKernelSpec,
    *,
    ctx: ExecutionContext,
    rule: RelationshipRule,
) -> Plan:
    predicate = FilterSpec(spec.predicate.to_expr_spec()).to_expression()
    return plan.filter(predicate, label=plan.label or rule.name, ctx=ctx)


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
    if strategy not in {"KEEP_ARBITRARY", "COLLAPSE_LIST", "KEEP_BEST_BY_SCORE"}:
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
    if strategy == "KEEP_BEST_BY_SCORE":
        resolved = _dedupe_spec_with_defaults(spec.spec, schema=schema)
        if not resolved.tie_breakers:
            return plan, False
        plan = plan.winner_select(
            resolved, columns=non_keys, ctx=ctx, label=plan.label or rule.name
        )
    else:
        agg_fn = "first" if strategy == "KEEP_ARBITRARY" else "list"
        plan = plan.aggregate(
            group_keys=list(spec.spec.keys), aggs=[(col, agg_fn) for col in non_keys]
        )
        agg_names = [f"{col}_{agg_fn}" for col in non_keys]
        exprs = [FieldExpr(name=name).to_expression() for name in spec.spec.keys]
        exprs.extend(FieldExpr(name=name).to_expression() for name in agg_names)
        out_names = list(spec.spec.keys) + non_keys
        plan = plan.project(exprs, out_names, label=plan.label or rule.name)
    return plan, True


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
        if isinstance(spec, FilterKernelSpec):
            plan = _apply_filter_to_plan(plan, spec, ctx=ctx, rule=rule)
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
    return align_plan(plan, schema=schema, ctx=ctx)


def _union_all_plans_aligned(plans: Sequence[Plan], *, ctx: ExecutionContext) -> Plan:
    schema = SchemaEvolutionSpec().unify_schema_from_schemas(
        [plan.schema(ctx=ctx) for plan in plans]
    )
    aligned = [_align_plan(plan, schema=schema, ctx=ctx) for plan in plans]
    return union_all_plans(aligned, label="relspec_union")


def _schema_for_contract(contract: Contract) -> SchemaLike:
    dataset_spec = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(contract.name)
    if dataset_spec is not None:
        return dataset_spec.schema()
    return contract.schema


def _schema_for_rule(
    rule: RelationshipRule,
    *,
    contracts: ContractCatalog | None,
) -> SchemaLike | None:
    if rule.contract_name is None:
        return None
    if contracts is not None:
        return _schema_for_contract(contracts.get(rule.contract_name))
    dataset_spec = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(rule.contract_name)
    if dataset_spec is not None:
        return dataset_spec.schema()
    return None


def apply_policy_defaults(rule: RelationshipRule, schema: SchemaLike) -> RelationshipRule:
    """Apply schema-derived default policies to a rule.

    Parameters
    ----------
    rule:
        Rule to update.
    schema:
        Schema providing policy metadata.

    Returns
    -------
    RelationshipRule
        Rule with policies populated when missing.
    """
    confidence = rule.confidence_policy or confidence_policy_from_schema(schema)
    ambiguity = rule.ambiguity_policy or ambiguity_policy_from_schema(schema)
    if confidence is rule.confidence_policy and ambiguity is rule.ambiguity_policy:
        return rule
    return replace(
        rule,
        confidence_policy=confidence,
        ambiguity_policy=ambiguity,
    )


def validate_policy_requirements(rule: RelationshipRule, schema: SchemaLike) -> None:
    """Validate policy-specific schema requirements.

    Parameters
    ----------
    rule:
        Rule to validate.
    schema:
        Schema providing available columns.

    Raises
    ------
    ValueError
        Raised when required columns are missing.
    """
    names = set(schema.names)
    if rule.confidence_policy is not None:
        required = {"confidence", "score"}
        missing = required - names
        if missing:
            msg = f"Rule {rule.name!r} missing confidence columns: {sorted(missing)}."
            raise ValueError(msg)
    if rule.ambiguity_policy is not None and "ambiguity_group_id" not in names:
        msg = f"Rule {rule.name!r} requires ambiguity_group_id column."
        raise ValueError(msg)


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
            return _apply_rule_metadata(table, rule=self.rule)
        if self.plan is None:
            msg = "CompiledRule has neither plan nor execute_fn."
            raise RuntimeError(msg)
        result = run_plan(
            self.plan,
            ctx=ctx,
            prefer_reader=False,
            attach_ordering_metadata=True,
        )
        table = cast("TableLike", result.value)
        if self.emit_rule_meta:
            table = _build_rule_meta_kernel(self.rule)(table, ctx)
        for fn in self.post_kernels:
            table = fn(table, ctx)
        return _apply_rule_metadata(table, rule=self.rule)


def _should_materialize_rule(compiled: CompiledRule) -> bool:
    if compiled.rule.execution_mode == "table":
        return True
    if compiled.execute_fn is not None:
        return True
    if compiled.plan is None or compiled.plan.decl is None:
        return True
    return bool(compiled.post_kernels)


def _collect_compiled_parts(
    contributors: Sequence[CompiledRule],
    *,
    ctx: ExecutionContext,
    resolver: PlanResolver,
) -> tuple[list[Plan], list[TableLike]]:
    plan_parts: list[Plan] = []
    table_parts: list[TableLike] = []
    for compiled in contributors:
        if _should_materialize_rule(compiled):
            table_parts.append(compiled.execute(ctx=ctx, resolver=resolver))
            continue
        plan = compiled.plan
        if plan is None:
            msg = f"CompiledRule {compiled.rule.name!r} missing plan for plan execution."
            raise RuntimeError(msg)
        plan_parts.append(_apply_rule_meta_to_plan(plan, rule=compiled.rule, ctx=ctx))
    return plan_parts, table_parts


def _finalize_output_tables(
    *,
    output_dataset: str,
    contract_name: str | None,
    table_parts: Sequence[TableLike],
    ctx: ExecutionContext,
    contracts: ContractCatalog,
) -> FinalizeResult:
    if contract_name is None:
        unioned = SchemaEvolutionSpec().unify_and_cast(
            table_parts,
            safe_cast=ctx.safe_cast,
            on_error="unsafe" if ctx.safe_cast else "raise",
            keep_extra_columns=ctx.provenance,
        )
        dummy = Contract(name=f"{output_dataset}_NO_CONTRACT", schema=unioned.schema)
        finalize_ctx = dataset_spec_from_contract(dummy).finalize_context(ctx)
        return finalize_ctx.run(unioned, ctx=ctx)

    contract = contracts.get(contract_name)
    dataset_spec = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(contract.name)
    if dataset_spec is None:
        dataset_spec = dataset_spec_from_contract(contract)
    unioned = dataset_spec.unify_tables(table_parts, ctx=ctx)
    return dataset_spec.finalize_context(ctx).run(unioned, ctx=ctx)


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

        plan_parts, table_parts = _collect_compiled_parts(
            self.contributors,
            ctx=ctx_exec,
            resolver=resolver,
        )

        if plan_parts:
            union = union_all_plans(plan_parts, label=self.output_dataset)
            result = run_plan(
                union,
                ctx=ctx_exec,
                prefer_reader=False,
                attach_ordering_metadata=True,
            )
            table_parts.append(cast("TableLike", result.value))

        return _finalize_output_tables(
            output_dataset=self.output_dataset,
            contract_name=self.contract_name,
            table_parts=table_parts,
            ctx=ctx_exec,
            contracts=contracts,
        )


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
                result = run_plan(
                    plan,
                    ctx=ctx2,
                    prefer_reader=False,
                    attach_ordering_metadata=True,
                )
                parts.append(cast("TableLike", result.value))
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
            left_plan = resolver.resolve(left_ref, ctx=ctx2)
            right_plan = resolver.resolve(right_ref, ctx=ctx2)
            lt_result = run_plan(
                left_plan,
                ctx=ctx2,
                prefer_reader=False,
                attach_ordering_metadata=True,
            )
            rt_result = run_plan(
                right_plan,
                ctx=ctx2,
                prefer_reader=False,
                attach_ordering_metadata=True,
            )
            lt = cast("TableLike", lt_result.value)
            rt = cast("TableLike", rt_result.value)
            return interval_align_table(lt, rt, cfg=interval_cfg)

        _ = ctx
        return CompiledRule(
            rule=rule,
            plan=None,
            execute_fn=_exec,
            post_kernels=_compile_post_kernels(post_kernels),
            emit_rule_meta=rule.emit_rule_meta,
        )

    def _compile_winner_select(
        self,
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
        src = rule.inputs[0]
        plan = self.resolver.resolve(src, ctx=ctx)
        schema = plan.schema(ctx=ctx)
        spec = DedupeSpec(
            keys=winner_cfg.keys,
            strategy="KEEP_BEST_BY_SCORE",
            tie_breakers=(
                SortKey(winner_cfg.score_col, winner_cfg.score_order),
                *winner_cfg.tie_breakers,
            ),
        )
        resolved = _dedupe_spec_with_defaults(spec, schema=schema)
        if plan.decl is not None and resolved.tie_breakers:
            non_keys = [name for name in schema.names if name not in resolved.keys]
            plan = plan.winner_select(resolved, columns=non_keys, ctx=ctx, label=rule.name)
            plan, remaining = _apply_plan_kernel_specs(plan, post_kernels, ctx=ctx, rule=rule)
            return CompiledRule(
                rule=rule,
                plan=plan,
                execute_fn=None,
                post_kernels=_compile_post_kernels(remaining),
                emit_rule_meta=rule.emit_rule_meta,
            )

        def _exec(ctx2: ExecutionContext, resolver: PlanResolver) -> TableLike:
            src_exec = rule.inputs[0]
            plan_exec = resolver.resolve(src_exec, ctx=ctx2)
            result = run_plan(
                plan_exec,
                ctx=ctx2,
                prefer_reader=False,
                attach_ordering_metadata=True,
            )
            table = cast("TableLike", result.value)
            resolved_spec = _dedupe_spec_with_defaults(spec, schema=table.schema)
            return apply_dedupe(table, spec=resolved_spec, _ctx=ctx2)

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
        compiled = handler(rule, ctx=ctx, post_kernels=rule.post_kernels)
        self._enforce_execution_mode(rule, compiled)
        return compiled

    @staticmethod
    def _enforce_execution_mode(rule: RelationshipRule, compiled: CompiledRule) -> None:
        if rule.execution_mode != "plan":
            return
        if compiled.plan is None or compiled.post_kernels:
            msg = f"Rule {rule.name!r} requires plan execution."
            raise ValueError(msg)

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
        rules = list(registry_rules)
        if contract_catalog is not None:
            validate_relationship_output_contracts_for_edge_kinds(
                rules=rules,
                contract_catalog=contract_catalog,
                config=edge_validation or EdgeContractValidationConfig(),
            )

        resolved_rules: list[RelationshipRule] = []
        for rule in rules:
            schema = _schema_for_rule(rule, contracts=contract_catalog)
            resolved_rule = rule
            if schema is not None:
                resolved_rule = apply_policy_defaults(rule, schema)
                validate_policy_requirements(resolved_rule, schema)
            resolved_rules.append(resolved_rule)

        by_out: dict[str, list[RelationshipRule]] = {}
        for rule in resolved_rules:
            by_out.setdefault(rule.output_dataset, []).append(rule)

        compiled: dict[str, CompiledOutput] = {}
        for out_name in sorted(by_out):
            rules = by_out[out_name]
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
