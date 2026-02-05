"""Command chaining frontend for CQ run."""

from __future__ import annotations

import itertools
from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.run.chain import compile_chain_segments
from tools.cq.run.runner import execute_run_plan


def chain(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    delimiter: Annotated[str, Parameter(name="--delimiter")] = "AND",
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Execute chained CQ commands via the shared run engine.

    Returns
    -------
    CliResult
        CLI result wrapper with the run result.

    Raises
    ------
    RuntimeError
        Raised when context is missing or no segments are provided.
    """
    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

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
