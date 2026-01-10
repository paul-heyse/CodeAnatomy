from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from .contracts import Contract
from .finalize import FinalizeResult, finalize
from .plan import Plan
from .runtime import ExecutionContext

if TYPE_CHECKING:  # pragma: no cover
    import pyarrow as pa


KernelFn = Callable[["pa.Table", ExecutionContext], "pa.Table"]


def run_pipeline(
    *,
    plan: Plan,
    contract: Contract,
    ctx: ExecutionContext,
    mode: str | None = None,
    post: Sequence[KernelFn] = (),
) -> FinalizeResult:
    """Execute a Plan, optionally apply kernel-lane post transforms, then finalize."""
    if mode is not None:
        ctx = ctx.with_mode(mode)

    tbl = plan.to_table(ctx=ctx)
    for fn in post:
        tbl = fn(tbl, ctx)

    return finalize(tbl, contract=contract, ctx=ctx)
