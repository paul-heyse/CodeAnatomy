"""Run command for multi-step CQ execution."""

from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.options import RunOptions, options_from_params
from tools.cq.cli_app.params import RunParams


def run(
    *,
    opts: Annotated[RunParams, Parameter(name="*")] | None = None,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Execute a multi-step CQ run plan.

    Returns
    -------
    CliResult
        CLI result wrapper with the run result.

    Raises
    ------
    RuntimeError
        Raised when context is missing.
    """
    from tools.cq.core.run_context import RunContext
    from tools.cq.core.schema import mk_result, ms
    from tools.cq.run.loader import RunPlanError, load_run_plan
    from tools.cq.run.runner import execute_run_plan

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    if opts is None:
        opts = RunParams()
    options = options_from_params(opts, type_=RunOptions)
    try:
        plan = load_run_plan(options)
    except RunPlanError as exc:
        run_ctx = RunContext.from_parts(
            root=ctx.root,
            argv=ctx.argv,
            tc=ctx.toolchain,
            started_ms=ms(),
        )
        result = mk_result(run_ctx.to_runmeta("run"))
        result.summary["error"] = str(exc)
        return CliResult(result=result, context=ctx, filters=options)

    result = execute_run_plan(plan, ctx, stop_on_error=options.stop_on_error)
    return CliResult(result=result, context=ctx, filters=options)
