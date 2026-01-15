"""Schema helpers for plan-like objects."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import cast

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike
from arrowdsl.ir.plan import OpNode, PlanIR
from arrowdsl.plan.ops import DedupeSpec, JoinSpec
from arrowdsl.plan.plan import Plan
from ibis_engine.plan import IbisPlan


def plan_schema(plan: Plan | IbisPlan, *, ctx: ExecutionContext) -> SchemaLike:
    """Return the schema for a Plan or IbisPlan without materialization.

    Returns
    -------
    SchemaLike
        Arrow schema representing the plan output.
    """
    if isinstance(plan, IbisPlan):
        return plan.expr.schema().to_pyarrow()
    return plan.schema(ctx=ctx)


def plan_output_columns(plan: Plan) -> Sequence[str] | None:
    """Return output column names inferred from Plan IR when possible.

    Returns
    -------
    Sequence[str] | None
        Inferred output column names or None when unavailable.
    """
    return _columns_from_ir(plan.ir)


def _columns_from_ir(ir: PlanIR) -> list[str] | None:
    columns: list[str] | None = None
    for node in ir.nodes:
        columns = _columns_after_node(node, columns)
    return columns


def _columns_after_node(node: OpNode, columns: list[str] | None) -> list[str] | None:
    handler = _NODE_COLUMN_HANDLERS.get(node.name)
    if handler is None:
        return columns
    return handler(node, columns)


def _columns_for_scan(node: OpNode, _columns: list[str] | None) -> list[str] | None:
    raw = cast("Sequence[str] | Mapping[str, object]", node.args.get("columns", ()))
    if isinstance(raw, Mapping):
        return list(raw.keys())
    return list(raw)


def _columns_for_table_source(node: OpNode, _columns: list[str] | None) -> list[str] | None:
    return _columns_from_table(node.args.get("table"))


def _columns_for_project(node: OpNode, _columns: list[str] | None) -> list[str] | None:
    names = cast("Sequence[str]", node.args.get("names", ()))
    return list(names)


def _columns_for_aggregate(node: OpNode, _columns: list[str] | None) -> list[str] | None:
    group_keys = cast("Sequence[str]", node.args.get("group_keys", ()))
    aggs = cast("Sequence[tuple[str, str]]", node.args.get("aggs", ()))
    agg_names = [f"{col}_{fn}" for col, fn in aggs]
    return list(group_keys) + agg_names


def _columns_for_hash_join(node: OpNode, columns: list[str] | None) -> list[str] | None:
    spec = node.args.get("spec")
    if isinstance(spec, JoinSpec):
        return list(spec.left_output) + list(spec.right_output)
    return columns


def _columns_for_winner_select(node: OpNode, columns: list[str] | None) -> list[str] | None:
    spec = node.args.get("spec")
    cols = cast("Sequence[str]", node.args.get("columns", ()))
    if isinstance(spec, DedupeSpec):
        return list(spec.keys) + list(cols)
    return columns


def _columns_for_explode_list(node: OpNode, _columns: list[str] | None) -> list[str] | None:
    out_parent = node.args.get("out_parent_col")
    out_value = node.args.get("out_value_col")
    if isinstance(out_parent, str) and isinstance(out_value, str):
        return [out_parent, out_value]
    return None


def _columns_for_union_all(node: OpNode, _columns: list[str] | None) -> list[str] | None:
    if not node.inputs:
        return None
    return _columns_from_ir(node.inputs[0])


_NODE_COLUMN_HANDLERS: dict[str, Callable[[OpNode, list[str] | None], list[str] | None]] = {
    "scan": _columns_for_scan,
    "table_source": _columns_for_table_source,
    "project": _columns_for_project,
    "aggregate": _columns_for_aggregate,
    "hash_join": _columns_for_hash_join,
    "winner_select": _columns_for_winner_select,
    "explode_list": _columns_for_explode_list,
    "union_all": _columns_for_union_all,
}


def _columns_from_table(table: object) -> list[str] | None:
    if table is None:
        return None
    column_names = getattr(table, "column_names", None)
    if column_names:
        return list(column_names)
    schema = getattr(table, "schema", None)
    if schema is None:
        return None
    names = getattr(schema, "names", None)
    if names:
        return list(names)
    return None


__all__ = ["plan_output_columns", "plan_schema"]
