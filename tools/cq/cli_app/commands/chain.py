"""Command chaining frontend for CQ run."""

from __future__ import annotations

import itertools
from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.decorators import require_context, require_ctx
from tools.cq.run.chain import compile_chain_segments
from tools.cq.run.runner import execute_run_plan


@require_ctx
def chain(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    delimiter: Annotated[str, Parameter(name="--delimiter")] = "AND",
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Execute chained CQ commands via the shared run engine.

    Args:
        *tokens: Command tokens and delimiters from the CLI.
        delimiter: Token used to split command segments.
        ctx: Injected CLI context.

    Returns:
        CliResult: Renderable command result payload.

    Raises:
        RuntimeError: If no chain segments are provided.
    """
    ctx = require_context(ctx)
    groups = [
        list(group)
        for is_delim, group in itertools.groupby(tokens, lambda t: t == delimiter)
        if not is_delim
    ]
    if not groups:
        msg = "No chain segments provided"
        raise RuntimeError(msg)

    plan = compile_chain_segments(groups)
    result = execute_run_plan(plan, ctx)
    return CliResult(result=result, context=ctx)
