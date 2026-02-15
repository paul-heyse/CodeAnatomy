"""Administrative CQ commands."""

from __future__ import annotations

import json
from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext, CliResult, CliTextResult
from tools.cq.cli_app.decorators import require_context, require_ctx
from tools.cq.cli_app.types import OutputFormat, SchemaKind
from tools.cq.core.schema_export import cq_result_schema, cq_schema_components, query_schema


def _emit_payload(
    ctx: CliContext,
    payload: object,
    *,
    text_fallback: str | None = None,
) -> CliResult:
    if ctx.output_format == OutputFormat.json:
        return CliResult(
            result=CliTextResult(text=json.dumps(payload, indent=2), media_type="application/json"),
            context=ctx,
            filters=None,
        )

    if text_fallback is None and isinstance(payload, dict):
        message = payload.get("message")
        if isinstance(message, str) and message:
            text_fallback = message
    if text_fallback is None:
        text_fallback = json.dumps(payload, indent=2)

    return CliResult(
        result=CliTextResult(text=text_fallback, media_type="text/plain"),
        context=ctx,
        filters=None,
    )


@require_ctx
def index(
    *,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Show deprecation notice for removed index management command.

    Returns:
        CliResult: Normalized CLI text result.
    """
    ctx = require_context(ctx)
    return _emit_payload(
        ctx,
        {
            "deprecated": True,
            "message": "Index management has been removed. Caching is no longer used.",
        },
        text_fallback="Index management has been removed. Caching is no longer used.",
    )


@require_ctx
def cache(
    *,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Show deprecation notice for removed cache management command.

    Returns:
        CliResult: Normalized CLI text result.
    """
    ctx = require_context(ctx)
    return _emit_payload(
        ctx,
        {
            "deprecated": True,
            "message": "Cache management has been removed. Caching is no longer used.",
        },
        text_fallback="Cache management has been removed. Caching is no longer used.",
    )


@require_ctx
def schema(
    *,
    kind: Annotated[SchemaKind, Parameter(help="Schema export kind")] = SchemaKind.result,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Export JSON schema payloads for CQ contracts.

    Returns:
        CliResult: Normalized CLI text result.
    """
    ctx = require_context(ctx)
    if kind == SchemaKind.result:
        payload: object = cq_result_schema()
    elif kind == SchemaKind.query:
        payload = query_schema()
    else:
        schema_rows, components = cq_schema_components()
        payload = {"schema": schema_rows, "components": components}
    return _emit_payload(ctx, payload, text_fallback=json.dumps(payload, indent=2))


__all__ = ["cache", "index", "schema"]
