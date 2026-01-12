"""Plan-lane node emission helpers."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import ensure_expression, pc
from arrowdsl.plan.plan import Plan
from cpg.plan_exprs import coalesce_expr
from cpg.specs import NodeEmitSpec


def emit_node_plan(
    plan: Plan,
    *,
    spec: NodeEmitSpec,
    ctx: ExecutionContext,
) -> Plan:
    """Project a node emission plan from a source plan.

    Returns
    -------
    Plan
        Plan emitting node columns.
    """
    available = set(plan.schema(ctx=ctx).names)
    node_id = coalesce_expr(spec.id_cols, available=available, dtype=pa.string())
    path = coalesce_expr(spec.path_cols, available=available, dtype=pa.string())
    bstart = coalesce_expr(spec.bstart_cols, available=available, dtype=pa.int64())
    bend = coalesce_expr(spec.bend_cols, available=available, dtype=pa.int64())
    file_id = coalesce_expr(spec.file_id_cols, available=available, dtype=pa.string())

    node_kind = ensure_expression(pc.cast(pc.scalar(spec.node_kind.value), pa.string(), safe=False))
    exprs = [
        node_id,
        node_kind,
        path,
        bstart,
        bend,
        file_id,
    ]
    names = ["node_id", "node_kind", "path", "bstart", "bend", "file_id"]
    return plan.project(exprs, names, ctx=ctx)


__all__ = ["emit_node_plan"]
