"""Expand command for details-kind payload handles."""

from __future__ import annotations

import json
from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.infrastructure import require_context
from tools.cq.macros.expand import cmd_expand


def expand(
    kind: Annotated[str, Parameter(help="Details kind to expand")],
    *,
    handle: Annotated[
        str,
        Parameter(
            name="--handle",
            help="JSON object handle payload produced by enrichment details preview",
        ),
    ] = "{}",
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Expand details-kind handles to full payloads.

    Returns:
        CliResult: CLI result wrapping expanded details payloads.
    """
    ctx = require_context(ctx)
    try:
        parsed = json.loads(handle)
    except json.JSONDecodeError as exc:
        from tools.cq.core.result_factory import build_error_result
        from tools.cq.core.schema import ms

        result = build_error_result(
            macro="expand",
            root=ctx.root,
            argv=ctx.argv,
            tc=ctx.toolchain,
            started_ms=ms(),
            error=f"Invalid --handle JSON: {exc}",
        )
        return CliResult(result=result, context=ctx, filters=None)
    if not isinstance(parsed, dict):
        from tools.cq.core.result_factory import build_error_result
        from tools.cq.core.schema import ms

        result = build_error_result(
            macro="expand",
            root=ctx.root,
            argv=ctx.argv,
            tc=ctx.toolchain,
            started_ms=ms(),
            error="--handle must decode to a JSON object",
        )
        return CliResult(result=result, context=ctx, filters=None)

    result = cmd_expand(
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.toolchain,
        kind=kind,
        handle=parsed,
    )
    return CliResult(result=result, context=ctx, filters=None)


__all__ = ["expand"]
