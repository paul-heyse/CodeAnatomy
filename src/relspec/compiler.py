"""Compile relationship rules into executable plans and kernels."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import cmp_to_key
from typing import Literal, Protocol

import arrowdsl.pyarrow_core as pa
from arrowdsl.contracts import Contract, SortKey
from arrowdsl.dataset_io import compile_to_acero_scan, open_dataset
from arrowdsl.expr import E
from arrowdsl.finalize import FinalizeResult, finalize
from arrowdsl.joins import JoinSpec, hash_join
from arrowdsl.kernels import apply_dedupe, explode_list_column
from arrowdsl.plan import Plan, union_all_plans
from arrowdsl.pyarrow_protocols import ComputeExpression, ScalarLike, TableLike
from arrowdsl.queryspec import ProjectionSpec, QuerySpec
from arrowdsl.runtime import DeterminismTier, ExecutionContext, Ordering
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
    IntervalAlignConfig,
    KernelSpecT,
    ProjectConfig,
    RelationshipRule,
    RenameColumnsSpec,
    RuleKind,
)
from relspec.registry import ContractCatalog, DatasetCatalog

type KernelFn = Callable[[TableLike, ExecutionContext], TableLike]
type RowValue = object | None
type RowData = dict[str, RowValue]
type Candidate = dict[str, RowValue]


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

        if ref.query is None:
            cols = tuple(dataset.schema.names)
            ref_query = QuerySpec(projection=ProjectionSpec(base=cols))
        else:
            ref_query = ref.query

        decl = compile_to_acero_scan(dataset, spec=ref_query, ctx=ctx)
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
            table = table.append_column(
                rule.rule_name_col,
                pa.array([rule.name] * count, type=pa.string()),
            )
        if rule.rule_priority_col not in table.column_names:
            table = table.append_column(
                rule.rule_priority_col,
                pa.array([int(rule.priority)] * count, type=pa.int32()),
            )
        return table

    return _add_rule_meta


def _build_add_literal_kernel(spec: AddLiteralSpec) -> KernelFn:
    def _fn(table: TableLike, _ctx: ExecutionContext) -> TableLike:
        if spec.name in table.column_names:
            return table
        return table.append_column(
            spec.name,
            pa.array([spec.value] * table.num_rows),
        )

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
    exprs = [E.field(name) for name in names]
    exprs.append(E.scalar(spec.value))
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
    exprs = [E.field(name) for name in keep]
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
    exprs = [E.field(name) for name in names]
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
    exprs = [E.field(name) for name in spec.spec.keys]
    exprs.extend(E.field(name) for name in agg_names)
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
        exprs.append(E.field(col))
        names.append(col)

    for name, expr in project.exprs.items():
        exprs.append(expr)
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
    exprs = [E.field(name) for name in names]
    if rule.rule_name_col not in names:
        exprs.append(E.scalar(rule.name))
        names.append(rule.rule_name_col)
    if rule.rule_priority_col not in names:
        exprs.append(E.scalar(int(rule.priority)))
        names.append(rule.rule_priority_col)
    return plan.project(exprs, names, label=plan.label or rule.name)


def _col_pylist(table: TableLike, name: str) -> list[RowValue]:
    if name not in table.column_names:
        return [None] * table.num_rows
    values: list[RowValue] = []
    for value in table[name]:
        if isinstance(value, ScalarLike):
            values.append(value.as_py())
        else:
            values.append(value)
    return values


def _row_value_number(value: RowValue) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _row_value_int(value: RowValue) -> int | None:
    num = _row_value_number(value)
    if num is None:
        return None
    return int(num)


def _cmp_values(a: RowValue, b: RowValue) -> int:
    if a is None and b is None:
        return 0
    if a is None:
        return 1
    if b is None:
        return -1
    result = 0
    a_num = _row_value_number(a)
    b_num = _row_value_number(b)
    if a_num is not None and b_num is not None:
        if a_num < b_num:
            result = -1
        elif a_num > b_num:
            result = 1
        return result
    a_str = str(a)
    b_str = str(b)
    if a_str < b_str:
        result = -1
    elif a_str > b_str:
        result = 1
    return result


def _match_ok(
    mode: Literal["EXACT", "CONTAINED_BEST", "OVERLAP_BEST"],
    *,
    left_start: int,
    left_end: int,
    right_start: int,
    right_end: int,
) -> bool:
    if mode == "EXACT":
        return right_start == left_start and right_end == left_end
    if mode == "CONTAINED_BEST":
        return right_start >= left_start and right_end <= left_end
    return not (right_end <= left_start or right_start >= left_end)


def _pick_best_candidate(
    candidates: list[Candidate],
    *,
    tie_breakers: tuple[SortKey, ...],
) -> Candidate | None:
    if not candidates:
        return None

    def _cmp(x: Candidate, y: Candidate) -> int:
        cmp_val = _cmp_values(x.get("_span_len"), y.get("_span_len"))
        if cmp_val != 0:
            return cmp_val
        for sk in tie_breakers:
            cmp_val = _cmp_values(x.get(sk.column), y.get(sk.column))
            if cmp_val == 0:
                continue
            if sk.order == "descending":
                cmp_val = -cmp_val
            return cmp_val
        return 0

    return sorted(candidates, key=cmp_to_key(_cmp))[0]


def _build_right_index(paths: Sequence[RowValue]) -> dict[str, list[int]]:
    idx: dict[str, list[int]] = {}
    for i, path in enumerate(paths):
        if path is None:
            continue
        idx.setdefault(str(path), []).append(i)
    return idx


def _collect_columns(table: TableLike, select_cols: tuple[str, ...]) -> dict[str, list[RowValue]]:
    columns = select_cols or tuple(table.column_names)
    return {col: _col_pylist(table, col) for col in columns}


@dataclass(frozen=True)
class IntervalAlignContext:
    """Shared context for interval alignment."""

    cfg: IntervalAlignConfig
    left_cols: Mapping[str, list[RowValue]]
    right_cols: Mapping[str, list[RowValue]]
    right_starts: Sequence[RowValue]
    right_ends: Sequence[RowValue]


def _build_left_row(
    ctx: IntervalAlignContext,
    *,
    left_idx: int,
    match_kind: str,
    match_score: float | None,
) -> RowData:
    row: RowData = {}
    for col, values in ctx.left_cols.items():
        row[col] = values[left_idx]
    for col in ctx.right_cols:
        row[col] = None
    if ctx.cfg.emit_match_meta:
        row[ctx.cfg.match_kind_col] = match_kind
        row[ctx.cfg.match_score_col] = match_score
    return row


def _build_match_row(
    ctx: IntervalAlignContext,
    *,
    left_idx: int,
    right_idx: int,
    match_kind: str,
    match_score: float | None,
) -> RowData:
    row: RowData = {}
    for col, values in ctx.left_cols.items():
        row[col] = values[left_idx]
    for col, values in ctx.right_cols.items():
        row[col] = values[right_idx]
    if ctx.cfg.emit_match_meta:
        row[ctx.cfg.match_kind_col] = match_kind
        row[ctx.cfg.match_score_col] = match_score
    return row


def _collect_candidates(
    ctx: IntervalAlignContext,
    *,
    left_start: int,
    left_end: int,
    right_indices: Sequence[int],
) -> list[Candidate]:
    candidates: list[Candidate] = []
    right_starts = ctx.right_starts
    right_ends = ctx.right_ends
    for idx in right_indices:
        start_val = _row_value_int(right_starts[idx])
        end_val = _row_value_int(right_ends[idx])
        if start_val is None or end_val is None:
            continue
        right_start = start_val
        right_end = end_val

        if not _match_ok(
            ctx.cfg.mode,
            left_start=left_start,
            left_end=left_end,
            right_start=right_start,
            right_end=right_end,
        ):
            continue

        candidate: Candidate = {"_j": idx, "_span_len": max(0, right_end - right_start)}
        for sk in ctx.cfg.tie_breakers:
            candidate[sk.column] = ctx.right_cols.get(sk.column, [None] * len(right_starts))[idx]
        candidates.append(candidate)
    return candidates


def _interval_align_row(
    ctx: IntervalAlignContext,
    *,
    left_idx: int,
    left_values: tuple[RowValue, RowValue, RowValue],
    right_by_path: Mapping[str, list[int]],
) -> RowData | None:
    path, start_val, end_val = left_values
    row: RowData | None = None
    match_kind: str | None = None

    if path is None or start_val is None or end_val is None:
        match_kind = "NO_PATH_OR_SPAN"
    else:
        left_start = _row_value_int(start_val)
        left_end = _row_value_int(end_val)
        if left_start is None or left_end is None:
            match_kind = "NO_PATH_OR_SPAN"
        else:
            candidates = _collect_candidates(
                ctx,
                left_start=left_start,
                left_end=left_end,
                right_indices=right_by_path.get(str(path), []),
            )
            best = _pick_best_candidate(candidates, tie_breakers=ctx.cfg.tie_breakers)
            if best is None:
                match_kind = "NO_MATCH"
            else:
                right_idx = _row_value_int(best.get("_j"))
                span_len = _row_value_int(best.get("_span_len"))
                if right_idx is None or span_len is None:
                    match_kind = "NO_MATCH"
                else:
                    score = float(-span_len)
                    row = _build_match_row(
                        ctx,
                        left_idx=left_idx,
                        right_idx=right_idx,
                        match_kind=ctx.cfg.mode,
                        match_score=score,
                    )

    if row is None and match_kind is not None and ctx.cfg.how == "left":
        row = _build_left_row(
            ctx,
            left_idx=left_idx,
            match_kind=match_kind,
            match_score=None,
        )
    return row


def _interval_align(left: TableLike, right: TableLike, cfg: IntervalAlignConfig) -> TableLike:
    """Perform a kernel-lane span join.

    Parameters
    ----------
    left:
        Left-side table.
    right:
        Right-side table.
    cfg:
        Interval alignment configuration.

    Returns
    -------
    pyarrow.Table
        Interval-aligned table.
    """
    left_paths = _col_pylist(left, cfg.left_path_col)
    left_starts = _col_pylist(left, cfg.left_start_col)
    left_ends = _col_pylist(left, cfg.left_end_col)

    right_paths = _col_pylist(right, cfg.right_path_col)
    right_starts = _col_pylist(right, cfg.right_start_col)
    right_ends = _col_pylist(right, cfg.right_end_col)

    ctx = IntervalAlignContext(
        cfg=cfg,
        left_cols=_collect_columns(left, cfg.select_left),
        right_cols=_collect_columns(right, cfg.select_right),
        right_starts=right_starts,
        right_ends=right_ends,
    )
    right_by_path = _build_right_index(right_paths)
    out_rows: list[RowData] = []

    for left_idx in range(left.num_rows):
        row = _interval_align_row(
            ctx,
            left_idx=left_idx,
            left_values=(left_paths[left_idx], left_starts[left_idx], left_ends[left_idx]),
            right_by_path=right_by_path,
        )
        if row is not None:
            out_rows.append(row)

    return pa.Table.from_pylist(out_rows)


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

        unioned = pa.concat_tables(table_parts, promote=True)

        if self.contract_name is None:
            dummy = Contract(name=f"{self.output_dataset}_NO_CONTRACT", schema=unioned.schema)
            return finalize(unioned, contract=dummy, ctx=ctx_exec)

        contract = contracts.get(self.contract_name)
        return finalize(unioned, contract=contract, ctx=ctx_exec)


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

        jc = rule.hash_join
        join_plan = hash_join(
            left=left_plan,
            right=right_plan,
            spec=JoinSpec(
                join_type=jc.join_type,
                left_keys=jc.left_keys,
                right_keys=jc.right_keys,
                left_output=jc.left_output,
                right_output=jc.right_output,
                output_suffix_for_left=jc.output_suffix_for_left,
                output_suffix_for_right=jc.output_suffix_for_right,
            ),
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

    @staticmethod
    def _compile_union_all(
        rule: RelationshipRule,
        *,
        ctx: ExecutionContext,
        post_kernels: tuple[KernelSpecT, ...],
    ) -> CompiledRule:
        def _exec(ctx2: ExecutionContext, resolver: PlanResolver) -> TableLike:
            parts: list[TableLike] = []
            for inp in rule.inputs:
                plan = resolver.resolve(inp, ctx=ctx2)
                parts.append(plan.to_table(ctx=ctx2))
            return pa.concat_tables(parts, promote=True)

        _ = ctx
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
        interval_cfg = cfg

        def _exec(ctx2: ExecutionContext, resolver: PlanResolver) -> TableLike:
            left_ref, right_ref = rule.inputs
            lt = resolver.resolve(left_ref, ctx=ctx2).to_table(ctx=ctx2)
            rt = resolver.resolve(right_ref, ctx=ctx2).to_table(ctx=ctx2)
            return _interval_align(lt, rt, interval_cfg)

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
            RuleKind.WINNER_SELECT: self._compile_passthrough,
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
                msg = f"Output {out_name!r} has inconsistent contract_name across rules: {contract_names}."
                raise ValueError(msg)

            contributors = tuple(self.compile_rule(rule, ctx=ctx) for rule in rules_sorted)
            compiled[out_name] = CompiledOutput(
                output_dataset=out_name,
                contract_name=rules_sorted[0].contract_name,
                contributors=contributors,
            )
        return compiled
