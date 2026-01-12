"""Plan- and kernel-lane join helpers for normalize pipelines."""

from __future__ import annotations

from arrowdsl.compute.kernels import apply_join
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.plan.ops import JoinSpec
from arrowdsl.plan.plan import Plan

JoinInput = TableLike | Plan
JoinOutput = TableLike | Plan


def join_plan(
    left: JoinInput,
    right: JoinInput,
    *,
    spec: JoinSpec,
    use_threads: bool = True,
    ctx: ExecutionContext | None = None,
) -> JoinOutput:
    """Join tables or plans using a shared JoinSpec.

    Returns
    -------
    TableLike | Plan
        Joined output, plan-backed when inputs are plan-backed.
    """
    if isinstance(left, Plan) or isinstance(right, Plan):
        left_plan = left if isinstance(left, Plan) else Plan.table_source(left)
        right_plan = right if isinstance(right, Plan) else Plan.table_source(right)
        return left_plan.join(right_plan, spec=spec, ctx=ctx)
    return apply_join(left, right, spec=spec, use_threads=use_threads)


__all__ = ["JoinInput", "JoinOutput", "join_plan"]
