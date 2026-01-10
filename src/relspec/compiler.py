from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import pyarrow as pa

from ..arrowdsl.contracts import Contract, SortKey
from ..arrowdsl.dataset_io import compile_to_acero_scan, open_dataset
from ..arrowdsl.expr import E
from ..arrowdsl.finalize import FinalizeResult, finalize
from ..arrowdsl.joins import JoinSpec, hash_join
from ..arrowdsl.kernels import (
    apply_dedupe,
    canonical_sort,
    explode_list_column,
)
from ..arrowdsl.plan import Plan
from ..arrowdsl.queryspec import ProjectionSpec, QuerySpec
from ..arrowdsl.runtime import ExecutionContext
from .edge_contract_validation import (
    EdgeContractValidationConfig,
    validate_relationship_output_contracts_for_edge_kinds,
)
from .model import (
    AddLiteralSpec,
    CanonicalSortKernelSpec,
    DatasetRef,
    DedupeKernelSpec,
    DropColumnsSpec,
    ExplodeListSpec,
    IntervalAlignConfig,
    ProjectConfig,
    RelationshipRule,
    RenameColumnsSpec,
    RuleKind,
)
from .registry import ContractCatalog, DatasetCatalog

KernelFn = Callable[[pa.Table, ExecutionContext], pa.Table]


class PlanResolver(Protocol):
    """
    Resolves a DatasetRef to an Acero-backed Plan.
    """

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> Plan: ...


def _scan_ordering_for_ctx(ctx: ExecutionContext):
    # If scan is configured to produce implicit ordering, mark as implicit; else unordered.
    from ..arrowdsl.runtime import Ordering

    if getattr(ctx.runtime.scan, "implicit_ordering", False) or getattr(
        ctx.runtime.scan, "require_sequenced_output", False
    ):
        return Ordering.implicit()
    return Ordering.unordered()


class FilesystemPlanResolver:
    """
    Default resolver: dataset names map to filesystem-backed Arrow datasets.

    This uses:
      - arrowdsl.dataset_io.open_dataset(...)
      - arrowdsl.dataset_io.compile_to_acero_scan(...)
    """

    def __init__(self, catalog: DatasetCatalog) -> None:
        self.catalog = catalog

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> Plan:
        loc = self.catalog.get(ref.name)
        ds = open_dataset(
            loc.path,
            format=loc.format,
            filesystem=loc.filesystem,
            partitioning=loc.partitioning,
        )

        if ref.query is None:
            # default: scan all columns
            cols = tuple(ds.schema.names)
            ref_query = QuerySpec(projection=ProjectionSpec(base=cols))
        else:
            ref_query = ref.query

        decl = compile_to_acero_scan(ds, spec=ref_query, ctx=ctx)
        label = ref.label or ref.name
        return Plan(decl=decl, label=label, ordering=_scan_ordering_for_ctx(ctx))


class InMemoryPlanResolver:
    """
    Test/dev resolver: dataset names map to in-memory Tables or Plans.
    """

    def __init__(self, mapping: Mapping[str, pa.Table | Plan]) -> None:
        self.mapping = dict(mapping)

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> Plan:
        obj = self.mapping.get(ref.name)
        if obj is None:
            raise KeyError(f"InMemoryPlanResolver: unknown dataset {ref.name!r}")
        if isinstance(obj, Plan):
            return obj
        return Plan.table_source(obj, label=ref.label or ref.name)


# -------------------------
# Kernel compilation helpers
# -------------------------


def _compile_post_kernels(rule: RelationshipRule) -> list[KernelFn]:
    """
    Turn KernelSpecT into concrete post-processing functions.

    These run in the kernel lane (on pa.Table).
    """
    fns: list[KernelFn] = []

    # Optional rule meta injection (useful for winner selection and observability)
    if rule.emit_rule_meta:

        def _add_rule_meta(t: pa.Table, ctx: ExecutionContext) -> pa.Table:
            n = t.num_rows
            if rule.rule_name_col not in t.column_names:
                t = t.append_column(rule.rule_name_col, pa.array([rule.name] * n, type=pa.string()))
            if rule.rule_priority_col not in t.column_names:
                t = t.append_column(
                    rule.rule_priority_col, pa.array([int(rule.priority)] * n, type=pa.int32())
                )
            return t

        fns.append(_add_rule_meta)

    for ks in rule.post_kernels:
        if isinstance(ks, AddLiteralSpec):

            def _mk_add_literal(spec: AddLiteralSpec) -> KernelFn:
                def _fn(t: pa.Table, ctx: ExecutionContext) -> pa.Table:
                    if spec.name in t.column_names:
                        return t
                    return t.append_column(spec.name, pa.array([spec.value] * t.num_rows))

                return _fn

            fns.append(_mk_add_literal(ks))

        elif isinstance(ks, DropColumnsSpec):

            def _mk_drop(spec: DropColumnsSpec) -> KernelFn:
                def _fn(t: pa.Table, ctx: ExecutionContext) -> pa.Table:
                    cols = [c for c in spec.columns if c in t.column_names]
                    return t.drop(cols) if cols else t

                return _fn

            fns.append(_mk_drop(ks))

        elif isinstance(ks, RenameColumnsSpec):

            def _mk_rename(spec: RenameColumnsSpec) -> KernelFn:
                def _fn(t: pa.Table, ctx: ExecutionContext) -> pa.Table:
                    if not spec.mapping:
                        return t
                    names = list(t.column_names)
                    new_names = [spec.mapping.get(n, n) for n in names]
                    return t.rename_columns(new_names)

                return _fn

            fns.append(_mk_rename(ks))

        elif isinstance(ks, ExplodeListSpec):

            def _mk_explode(spec: ExplodeListSpec) -> KernelFn:
                def _fn(t: pa.Table, ctx: ExecutionContext) -> pa.Table:
                    return explode_list_column(
                        t,
                        parent_id_col=spec.parent_id_col,
                        list_col=spec.list_col,
                        out_parent_col=spec.out_parent_col,
                        out_value_col=spec.out_value_col,
                    )

                return _fn

            fns.append(_mk_explode(ks))

        elif isinstance(ks, DedupeKernelSpec):

            def _mk_dedupe(spec: DedupeKernelSpec) -> KernelFn:
                def _fn(t: pa.Table, ctx: ExecutionContext) -> pa.Table:
                    return apply_dedupe(t, spec=spec.spec, ctx=ctx)

                return _fn

            fns.append(_mk_dedupe(ks))

        elif isinstance(ks, CanonicalSortKernelSpec):

            def _mk_sort(spec: CanonicalSortKernelSpec) -> KernelFn:
                def _fn(t: pa.Table, ctx: ExecutionContext) -> pa.Table:
                    return canonical_sort(t, sort_keys=spec.sort_keys)

                return _fn

            fns.append(_mk_sort(ks))

        else:
            raise ValueError(f"Unknown KernelSpec type: {type(ks).__name__}")

    return fns


def _apply_project_to_plan(
    plan: Plan, project: ProjectConfig | None, *, rule: RelationshipRule
) -> Plan:
    """
    Apply a projection in plan lane when possible.

    If project is None, we still may inject rule meta in kernel lane (post kernels).
    """
    if project is None:
        return plan

    # ProjectConfig.select empty => don't project (keep all columns).
    # But Acero ProjectNodeOptions requires explicit columns.
    # So we only do plan-lane projection if select/exprs are explicitly provided.
    if not project.select and not project.exprs:
        return plan

    import pyarrow.compute as pc

    exprs: list[pc.Expression] = []
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


# -------------------------
# Interval alignment (kernel lane)
# -------------------------


def _col_pylist(t: pa.Table, name: str) -> list[Any]:
    if name not in t.column_names:
        return [None] * t.num_rows
    return t[name].to_pylist()


def _cmp_values(a: Any, b: Any) -> int:
    # None sorts last
    if a is None and b is None:
        return 0
    if a is None:
        return 1
    if b is None:
        return -1
    if a < b:
        return -1
    if a > b:
        return 1
    return 0


def _pick_best_candidate(
    candidates: list[dict],
    *,
    tie_breakers: tuple[SortKey, ...],
) -> dict | None:
    """
    Candidates are dicts with at least:
      - "_span_len" (int)
      - plus any tie_breaker columns
    Lower _span_len is better (more specific).
    """
    if not candidates:
        return None

    def cmp(x: dict, y: dict) -> int:
        # 1) smallest span length wins
        c = _cmp_values(x.get("_span_len"), y.get("_span_len"))
        if c != 0:
            return c

        # 2) tie breakers
        for sk in tie_breakers:
            xv = x.get(sk.column)
            yv = y.get(sk.column)
            c2 = _cmp_values(xv, yv)
            if c2 == 0:
                continue
            if sk.order == "descending":
                c2 = -c2
            return c2
        return 0

    from functools import cmp_to_key

    return sorted(candidates, key=cmp_to_key(cmp))[0]


def _interval_align(left: pa.Table, right: pa.Table, cfg: IntervalAlignConfig) -> pa.Table:
    """
    Kernel-lane span join.

    Output columns are:
      - cfg.select_left (from left)
      - cfg.select_right (from right)
      - optional match meta columns
    """
    lp = _col_pylist(left, cfg.left_path_col)
    ls = _col_pylist(left, cfg.left_start_col)
    le = _col_pylist(left, cfg.left_end_col)

    rp = _col_pylist(right, cfg.right_path_col)
    rs = _col_pylist(right, cfg.right_start_col)
    re = _col_pylist(right, cfg.right_end_col)

    # Index right rows by path
    right_by_path: dict[str, list[int]] = {}
    for i, p in enumerate(rp):
        if p is None:
            continue
        right_by_path.setdefault(str(p), []).append(i)

    left_cols = (
        {c: _col_pylist(left, c) for c in cfg.select_left}
        if cfg.select_left
        else {c: _col_pylist(left, c) for c in left.column_names}
    )
    right_cols = (
        {c: _col_pylist(right, c) for c in cfg.select_right}
        if cfg.select_right
        else {c: _col_pylist(right, c) for c in right.column_names}
    )

    out_rows: list[dict] = []

    for i in range(left.num_rows):
        p = lp[i]
        if p is None or ls[i] is None or le[i] is None:
            if cfg.how == "left":
                row = {}
                for c, arr in left_cols.items():
                    row[c] = arr[i]
                for c in right_cols:
                    row[c] = None
                if cfg.emit_match_meta:
                    row[cfg.match_kind_col] = "NO_PATH_OR_SPAN"
                    row[cfg.match_score_col] = None
                out_rows.append(row)
            continue

        p = str(p)
        lstart = int(ls[i])
        lend = int(le[i])

        cand_idxs = right_by_path.get(p, [])
        candidates: list[dict] = []

        for j in cand_idxs:
            if rs[j] is None or re[j] is None:
                continue
            rstart = int(rs[j])
            rend = int(re[j])

            if cfg.mode == "EXACT":
                ok = rstart == lstart and rend == lend
            elif cfg.mode == "CONTAINED_BEST":
                ok = rstart >= lstart and rend <= lend
            else:  # OVERLAP_BEST
                ok = not (rend <= lstart or rstart >= lend)

            if not ok:
                continue

            d = {"_j": j, "_span_len": max(0, rend - rstart)}
            # include tie breaker columns
            for sk in cfg.tie_breakers:
                if sk.column in right_cols:
                    d[sk.column] = right_cols[sk.column][j]
                else:
                    d[sk.column] = None
            candidates.append(d)

        best = _pick_best_candidate(candidates, tie_breakers=cfg.tie_breakers)

        if best is None:
            if cfg.how == "left":
                row = {}
                for c, arr in left_cols.items():
                    row[c] = arr[i]
                for c in right_cols:
                    row[c] = None
                if cfg.emit_match_meta:
                    row[cfg.match_kind_col] = "NO_MATCH"
                    row[cfg.match_score_col] = None
                out_rows.append(row)
            continue

        j = int(best["_j"])
        row = {}
        for c, arr in left_cols.items():
            row[c] = arr[i]
        for c, arr in right_cols.items():
            row[c] = arr[j]

        if cfg.emit_match_meta:
            row[cfg.match_kind_col] = cfg.mode
            # higher score is better; we invert span length so "smaller span" => larger score
            row[cfg.match_score_col] = float(-int(best["_span_len"]))

        out_rows.append(row)

    # Let Arrow infer schema; downstream finalize aligns to Contract anyway.
    return pa.Table.from_pylist(out_rows)


# -------------------------
# Compilation outputs
# -------------------------


@dataclass(frozen=True)
class CompiledRule:
    """
    Executable compiled form of a rule.

    - If plan is set: execute = plan.to_table(ctx) + post_kernels
    - If execute_fn is set: execute_fn(ctx, resolver) returns a pa.Table (composite/kernel-lane)
    """

    rule: RelationshipRule
    plan: Plan | None
    execute_fn: Callable[[ExecutionContext, PlanResolver], pa.Table] | None
    post_kernels: tuple[KernelFn, ...] = ()

    def execute(self, *, ctx: ExecutionContext, resolver: PlanResolver) -> pa.Table:
        if self.execute_fn is not None:
            t = self.execute_fn(ctx, resolver)
            for fn in self.post_kernels:
                t = fn(t, ctx)
            return t
        if self.plan is None:
            raise RuntimeError("CompiledRule has neither plan nor execute_fn")
        t = self.plan.to_table(ctx=ctx)
        for fn in self.post_kernels:
            t = fn(t, ctx)
        return t


@dataclass(frozen=True)
class CompiledOutput:
    """
    One output dataset may be produced by one or multiple rules.

    Execution strategy:
      - execute each contributor to a Table
      - concat_tables(promote=True)
      - finalize against the output Contract
    """

    output_dataset: str
    contract_name: str | None
    contributors: tuple[CompiledRule, ...] = ()

    def execute(
        self, *, ctx: ExecutionContext, resolver: PlanResolver, contracts: ContractCatalog
    ) -> FinalizeResult:
        if not self.contributors:
            raise ValueError(f"CompiledOutput {self.output_dataset!r} has no contributors")

        tables = [cr.execute(ctx=ctx, resolver=resolver) for cr in self.contributors]
        unioned = pa.concat_tables(tables, promote=True)

        if self.contract_name is None:
            # No contract: return a "Finalize-like" object with best-effort.
            # For production you should always specify a contract.
            dummy_schema = unioned.schema
            dummy = Contract(name=f"{self.output_dataset}_NO_CONTRACT", schema=dummy_schema)
            return finalize(unioned, contract=dummy, ctx=ctx)

        contract = contracts.get(self.contract_name)
        return finalize(unioned, contract=contract, ctx=ctx)


# -------------------------
# Compiler
# -------------------------


class RelationshipRuleCompiler:
    """
    Compiles RelationshipRules into executable units (Plans + kernel lane transforms).

    Philosophy:
      - express as much as possible in plan lane (Acero)
      - do non-relational joins (interval spans) explicitly in kernel lane
      - ensure rule meta exists to support deterministic winner selection
    """

    def __init__(self, *, resolver: PlanResolver) -> None:
        self.resolver = resolver

    def compile_rule(self, rule: RelationshipRule, *, ctx: ExecutionContext) -> CompiledRule:
        rule.validate()
        post = tuple(_compile_post_kernels(rule))

        if rule.kind == RuleKind.FILTER_PROJECT:
            src = rule.inputs[0]
            plan = self.resolver.resolve(src, ctx=ctx)
            plan = _apply_project_to_plan(plan, rule.project, rule=rule)
            return CompiledRule(rule=rule, plan=plan, execute_fn=None, post_kernels=post)

        if rule.kind == RuleKind.HASH_JOIN:
            assert rule.hash_join is not None
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
            return CompiledRule(rule=rule, plan=join_plan, execute_fn=None, post_kernels=post)

        if rule.kind == RuleKind.EXPLODE_LIST:
            src = rule.inputs[0]
            plan = self.resolver.resolve(src, ctx=ctx)
            return CompiledRule(rule=rule, plan=plan, execute_fn=None, post_kernels=post)

        if rule.kind == RuleKind.WINNER_SELECT:
            src = rule.inputs[0]
            plan = self.resolver.resolve(src, ctx=ctx)
            return CompiledRule(rule=rule, plan=plan, execute_fn=None, post_kernels=post)

        if rule.kind == RuleKind.UNION_ALL:

            def _exec(ctx2: ExecutionContext, resolver: PlanResolver) -> pa.Table:
                parts: list[pa.Table] = []
                for inp in rule.inputs:
                    p = resolver.resolve(inp, ctx=ctx2)
                    parts.append(p.to_table(ctx=ctx2))
                return pa.concat_tables(parts, promote=True)

            return CompiledRule(rule=rule, plan=None, execute_fn=_exec, post_kernels=post)

        if rule.kind == RuleKind.INTERVAL_ALIGN:
            assert rule.interval_align is not None

            def _exec(ctx2: ExecutionContext, resolver: PlanResolver) -> pa.Table:
                left_ref, right_ref = rule.inputs
                lt = resolver.resolve(left_ref, ctx=ctx2).to_table(ctx=ctx2)
                rt = resolver.resolve(right_ref, ctx=ctx2).to_table(ctx=ctx2)
                out = _interval_align(lt, rt, rule.interval_align)  # kernel-lane join
                return out

            return CompiledRule(rule=rule, plan=None, execute_fn=_exec, post_kernels=post)

        raise ValueError(f"Unknown rule kind: {rule.kind}")

    def compile_registry(
        self,
        registry_rules: Sequence[RelationshipRule],
        *,
        ctx: ExecutionContext,
        contracts: Any = None,
        edge_validation: EdgeContractValidationConfig | None = None,
    ) -> dict[str, CompiledOutput]:
        """
        Compile a set of relationship rules into executable outputs.

        New: if `contracts` is provided, validates that relationship output contracts
        satisfy required edge props for the edge kinds those datasets feed.
        """
        # --- NEW VALIDATION HOOK ---
        if contracts is not None:
            validate_relationship_output_contracts_for_edge_kinds(
                rules=rules,
                contract_catalog=contracts,
                config=edge_validation or EdgeContractValidationConfig(),
            )

        by_out: dict[str, list[RelationshipRule]] = {}
        for r in registry_rules:
            by_out.setdefault(r.output_dataset, []).append(r)

        compiled: dict[str, CompiledOutput] = {}
        for out_name, rules in by_out.items():
            rules_sorted = sorted(rules, key=lambda rr: (rr.priority, rr.name))
            # enforce contract consistency
            contracts = {rr.contract_name for rr in rules_sorted}
            if len(contracts) > 1:
                raise ValueError(
                    f"Output {out_name!r} has inconsistent contract_name across rules: {contracts}"
                )

            contrib = tuple(self.compile_rule(r, ctx=ctx) for r in rules_sorted)
            compiled[out_name] = CompiledOutput(
                output_dataset=out_name,
                contract_name=rules_sorted[0].contract_name,
                contributors=contrib,
            )
        return compiled
