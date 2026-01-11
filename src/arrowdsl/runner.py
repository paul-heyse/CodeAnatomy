"""Pipeline runner for Plan execution and finalization."""

from __future__ import annotations

from collections.abc import Callable, Sequence

import pyarrow as pa

from arrowdsl.contracts import Contract
from arrowdsl.finalize import FinalizeResult, finalize
from arrowdsl.plan import Plan
from arrowdsl.runtime import ExecutionContext
from core_types import StrictnessMode

KernelFn = Callable[[pa.Table, ExecutionContext], pa.Table]


def run_pipeline(
    *,
    plan: Plan,
    contract: Contract,
    ctx: ExecutionContext,
    mode: StrictnessMode | None = None,
    post: Sequence[KernelFn] = (),
) -> FinalizeResult:
    """Execute a plan, apply kernels, and finalize results.

    Parameters
    ----------
    plan:
        Plan to execute.
    contract:
        Output contract.
    ctx:
        Execution context.
    mode:
        Optional finalize mode override.
    post:
        Kernel-lane post-processing functions.

    Returns
    -------
    FinalizeResult
        Finalized output bundle.
    """
    if mode is not None:
        ctx = ctx.with_mode(mode)

    table = plan.to_table(ctx=ctx)
    for fn in post:
        table = fn(table, ctx)

    return finalize(table, contract=contract, ctx=ctx)
