"""Pipeline runner for Plan execution and finalization."""

from __future__ import annotations

from collections.abc import Callable, Sequence

from arrowdsl.contracts import Contract
from arrowdsl.finalize import FinalizeResult
from arrowdsl.finalize_context import FinalizeContext
from arrowdsl.ops import KernelOp
from arrowdsl.plan import Plan
from arrowdsl.pyarrow_protocols import TableLike
from arrowdsl.runtime import ExecutionContext
from core_types import StrictnessMode

KernelFn = Callable[[TableLike, ExecutionContext], TableLike]
KernelStep = KernelOp | KernelFn


def run_pipeline(
    *,
    plan: Plan,
    contract: Contract,
    ctx: ExecutionContext,
    mode: StrictnessMode | None = None,
    post: Sequence[KernelStep] = (),
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
    for step in post:
        table = step.apply(table, ctx) if isinstance(step, KernelOp) else step(table, ctx)

    transform = None
    if contract.schema_spec is not None:
        transform = contract.schema_spec.to_transform(
            safe_cast=ctx.safe_cast,
            on_error="unsafe" if ctx.safe_cast else "raise",
        )
    return FinalizeContext(contract=contract, transform=transform).run(table, ctx=ctx)
