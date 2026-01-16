"""Plan compiler for ArrowDSL fallback Acero execution."""

from __future__ import annotations

import functools
from collections.abc import Callable, Sequence
from typing import cast

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import (
    ComputeExpression,
    DeclarationLike,
    RecordBatchReaderLike,
    TableLike,
    acero,
    pc,
)
from arrowdsl.ir.plan import OpNode, PlanIR
from arrowdsl.ops.catalog import OpDef
from arrowdsl.plan.ops import DedupeSpec, JoinSpec

type CompileFn = Callable[[PlanIR, ExecutionContext], DeclarationLike]
type Handler = Callable[[OpNode, DeclarationLike | None], DeclarationLike]


def _scan_declaration(
    *,
    dataset: object,
    columns: Sequence[str] | dict[str, ComputeExpression],
    predicate: ComputeExpression | None,
    ctx: ExecutionContext,
) -> DeclarationLike:
    scan_kwargs = ctx.runtime.scan.scanner_kwargs()
    scan_kwargs.update(ctx.runtime.scan.scan_node_kwargs())
    opts = acero.ScanNodeOptions(
        dataset,
        columns=columns,
        filter=predicate,
        **scan_kwargs,
    )
    return acero.Declaration("scan", opts)


def _table_source_declaration(*, table: TableLike | RecordBatchReaderLike) -> DeclarationLike:
    return acero.Declaration("table_source", acero.TableSourceNodeOptions(table))


def _filter_declaration(
    *,
    predicate: ComputeExpression,
    input_decl: DeclarationLike,
) -> DeclarationLike:
    return acero.Declaration("filter", acero.FilterNodeOptions(predicate), inputs=[input_decl])


def _project_declaration(
    *,
    expressions: Sequence[ComputeExpression],
    names: Sequence[str],
    input_decl: DeclarationLike,
) -> DeclarationLike:
    return acero.Declaration(
        "project",
        acero.ProjectNodeOptions(list(expressions), list(names)),
        inputs=[input_decl],
    )


def _order_by_declaration(
    *,
    sort_keys: Sequence[tuple[str, str]],
    input_decl: DeclarationLike,
) -> DeclarationLike:
    return acero.Declaration(
        "order_by",
        acero.OrderByNodeOptions(sort_keys=list(sort_keys)),
        inputs=[input_decl],
    )


def _aggregate_declaration(
    *,
    group_keys: Sequence[str],
    aggs: Sequence[tuple[str, str]],
    input_decl: DeclarationLike,
) -> DeclarationLike:
    use_hash = bool(group_keys)
    agg_specs: list[tuple[str, str, object | None, str]] = []
    for col, fn in aggs:
        agg_fn = fn
        if use_hash and not fn.startswith("hash_"):
            agg_fn = f"hash_{fn}"
        agg_specs.append((col, agg_fn, None, f"{col}_{fn}"))
    keys = list(group_keys) if group_keys else None
    return acero.Declaration(
        "aggregate",
        acero.AggregateNodeOptions(agg_specs, keys=keys),
        inputs=[input_decl],
    )


def _hash_join_declaration(
    *,
    left_decl: DeclarationLike,
    right_decl: DeclarationLike,
    spec: JoinSpec,
) -> DeclarationLike:
    return acero.Declaration(
        "hashjoin",
        acero.HashJoinNodeOptions(
            join_type=spec.join_type,
            left_keys=list(spec.left_keys),
            right_keys=list(spec.right_keys),
            left_output=list(spec.left_output),
            right_output=list(spec.right_output),
            output_suffix_for_left=spec.output_suffix_for_left,
            output_suffix_for_right=spec.output_suffix_for_right,
        ),
        inputs=[left_decl, right_decl],
    )


def _winner_select_declaration(
    *,
    input_decl: DeclarationLike,
    spec: DedupeSpec,
    columns: Sequence[str],
) -> DeclarationLike:
    if not spec.keys:
        msg = "winner_select requires non-empty keys."
        raise ValueError(msg)
    if not spec.tie_breakers:
        msg = "winner_select requires at least one tie breaker."
        raise ValueError(msg)
    sort_keys = [(key.column, key.order) for key in spec.tie_breakers]
    order_decl = acero.Declaration(
        "order_by",
        acero.OrderByNodeOptions(sort_keys=list(sort_keys)),
        inputs=[input_decl],
    )
    agg_specs = [(col, "first", None, f"{col}_first") for col in columns]
    agg_decl = acero.Declaration(
        "aggregate",
        acero.AggregateNodeOptions(agg_specs, keys=list(spec.keys)),
        inputs=[order_decl],
    )
    proj_exprs = [pc.field(name) for name in spec.keys]
    proj_names = list(spec.keys)
    for col in columns:
        proj_exprs.append(pc.field(f"{col}_first"))
        proj_names.append(col)
    return acero.Declaration(
        "project",
        acero.ProjectNodeOptions(proj_exprs, proj_names),
        inputs=[agg_decl],
    )


def _scan_decl(node: OpNode, *, ctx: ExecutionContext) -> DeclarationLike:
    dataset = node.args["dataset"]
    columns = cast("Sequence[str] | dict[str, ComputeExpression]", node.args["columns"])
    predicate = cast("ComputeExpression | None", node.args.get("predicate"))
    return _scan_declaration(
        dataset=dataset,
        columns=columns,
        predicate=predicate,
        ctx=ctx,
    )


def _table_source_decl(node: OpNode) -> DeclarationLike:
    table = cast("TableLike | RecordBatchReaderLike", node.args["table"])
    return _table_source_declaration(table=table)


def _union_all_decl(
    node: OpNode, *, ctx: ExecutionContext, compile_ir: CompileFn
) -> DeclarationLike:
    if not node.inputs:
        msg = "union_all requires at least one input plan."
        raise ValueError(msg)
    inputs = [compile_ir(plan, ctx) for plan in node.inputs]
    return acero.Declaration("union", None, inputs=inputs)


def _filter_decl(node: OpNode, *, input_decl: DeclarationLike) -> DeclarationLike:
    return _filter_declaration(
        predicate=cast("ComputeExpression", node.args["predicate"]),
        input_decl=input_decl,
    )


def _project_decl(node: OpNode, *, input_decl: DeclarationLike) -> DeclarationLike:
    return _project_declaration(
        expressions=cast("Sequence[ComputeExpression]", node.args["expressions"]),
        names=cast("Sequence[str]", node.args["names"]),
        input_decl=input_decl,
    )


def _order_by_decl(node: OpNode, *, input_decl: DeclarationLike) -> DeclarationLike:
    return _order_by_declaration(
        sort_keys=cast("Sequence[tuple[str, str]]", node.args["sort_keys"]),
        input_decl=input_decl,
    )


def _aggregate_decl(node: OpNode, *, input_decl: DeclarationLike) -> DeclarationLike:
    return _aggregate_declaration(
        group_keys=cast("Sequence[str]", node.args.get("group_keys", ())),
        aggs=cast("Sequence[tuple[str, str]]", node.args["aggs"]),
        input_decl=input_decl,
    )


def _hash_join_decl(
    node: OpNode,
    *,
    input_decl: DeclarationLike,
    ctx: ExecutionContext,
    compile_ir: CompileFn,
) -> DeclarationLike:
    if not node.inputs:
        msg = "hash_join requires a right-hand input plan."
        raise ValueError(msg)
    right_decl = compile_ir(node.inputs[0], ctx)
    spec = cast("JoinSpec", node.args["spec"])
    return _hash_join_declaration(
        left_decl=input_decl,
        right_decl=right_decl,
        spec=spec,
    )


def _winner_select_decl(node: OpNode, *, input_decl: DeclarationLike) -> DeclarationLike:
    spec = cast("DedupeSpec", node.args["spec"])
    columns = cast("Sequence[str]", node.args["columns"])
    return _winner_select_declaration(
        input_decl=input_decl,
        spec=spec,
        columns=columns,
    )


def _require_input(input_decl: DeclarationLike | None, *, name: str) -> DeclarationLike:
    if input_decl is None:
        msg = f"Op {name!r} requires input declaration."
        raise ValueError(msg)
    return input_decl


def _acero_handler_scan(
    node: OpNode,
    _: DeclarationLike | None,
    *,
    ctx: ExecutionContext,
) -> DeclarationLike:
    return _scan_decl(node, ctx=ctx)


def _acero_handler_table_source(
    node: OpNode,
    _: DeclarationLike | None,
) -> DeclarationLike:
    return _table_source_decl(node)


def _acero_handler_union_all(
    node: OpNode,
    input_decl: DeclarationLike | None,
    *,
    ctx: ExecutionContext,
    compile_ir: CompileFn,
) -> DeclarationLike:
    if input_decl is not None:
        msg = "union_all must be the first operation in a plan."
        raise ValueError(msg)
    return _union_all_decl(node, ctx=ctx, compile_ir=compile_ir)


def _acero_handler_filter(
    node: OpNode,
    input_decl: DeclarationLike | None,
) -> DeclarationLike:
    return _filter_decl(node, input_decl=_require_input(input_decl, name="filter"))


def _acero_handler_project(
    node: OpNode,
    input_decl: DeclarationLike | None,
) -> DeclarationLike:
    return _project_decl(node, input_decl=_require_input(input_decl, name="project"))


def _acero_handler_order_by(
    node: OpNode,
    input_decl: DeclarationLike | None,
) -> DeclarationLike:
    return _order_by_decl(node, input_decl=_require_input(input_decl, name="order_by"))


def _acero_handler_aggregate(
    node: OpNode,
    input_decl: DeclarationLike | None,
) -> DeclarationLike:
    return _aggregate_decl(node, input_decl=_require_input(input_decl, name="aggregate"))


def _acero_handler_hash_join(
    node: OpNode,
    input_decl: DeclarationLike | None,
    *,
    ctx: ExecutionContext,
    compile_ir: CompileFn,
) -> DeclarationLike:
    return _hash_join_decl(
        node,
        input_decl=_require_input(input_decl, name="hash_join"),
        ctx=ctx,
        compile_ir=compile_ir,
    )


def _acero_handler_winner_select(
    node: OpNode,
    input_decl: DeclarationLike | None,
) -> DeclarationLike:
    return _winner_select_decl(
        node,
        input_decl=_require_input(input_decl, name="winner_select"),
    )


def _acero_handlers(ctx: ExecutionContext, compile_ir: CompileFn) -> dict[str, Handler]:
    return {
        "scan": functools.partial(_acero_handler_scan, ctx=ctx),
        "table_source": _acero_handler_table_source,
        "union_all": functools.partial(_acero_handler_union_all, ctx=ctx, compile_ir=compile_ir),
        "filter": _acero_handler_filter,
        "project": _acero_handler_project,
        "order_by": _acero_handler_order_by,
        "aggregate": _acero_handler_aggregate,
        "hash_join": functools.partial(_acero_handler_hash_join, ctx=ctx, compile_ir=compile_ir),
        "winner_select": _acero_handler_winner_select,
    }


def _build_acero_decl(
    node: OpNode,
    op_def: OpDef,
    *,
    input_decl: DeclarationLike | None,
    handlers: dict[str, Handler],
) -> DeclarationLike:
    handler = handlers.get(op_def.name)
    if handler is None:
        msg = f"Op {op_def.name!r} has no Acero compilation handler."
        raise ValueError(msg)
    return handler(node, input_decl)


class PlanCompiler:
    """Compile PlanIR into Acero declarations for fallback execution."""

    def __init__(self, *, catalog: dict[str, OpDef]) -> None:
        self._catalog = catalog

    def _compile_ir(self, plan: PlanIR, ctx: ExecutionContext) -> DeclarationLike:
        plan.validate(catalog=self._catalog)
        decl: DeclarationLike | None = None
        handlers = _acero_handlers(ctx, self._compile_ir)
        for node in plan.nodes:
            op_def = self._catalog[node.name]
            if op_def.acero_node is None and node.name not in {
                "scan",
                "table_source",
                "hash_join",
                "union_all",
                "winner_select",
            }:
                msg = f"Op {node.name!r} has no Acero node."
                raise ValueError(msg)
            decl = _build_acero_decl(
                node,
                op_def,
                input_decl=decl,
                handlers=handlers,
            )
        if decl is None:
            msg = "PlanIR has no operations."
            raise ValueError(msg)
        return decl

    def to_acero(self, plan: PlanIR, *, ctx: ExecutionContext) -> DeclarationLike:
        """Compile PlanIR into an Acero declaration.

        Returns
        -------
        DeclarationLike
            Acero declaration for fallback execution.

        """
        return self._compile_ir(plan, ctx)


__all__ = ["PlanCompiler"]
