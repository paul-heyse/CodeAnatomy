"""Administrative CQ commands."""

from __future__ import annotations

import json
from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.infrastructure import require_context, require_ctx
from tools.cq.cli_app.protocol_output import json_result, text_result, wants_json
from tools.cq.cli_app.types import SchemaKind
from tools.cq.core.schema_export import cq_result_schema, cq_schema_components, query_schema


def _emit_payload(
    ctx: CliContext,
    payload: object,
    *,
    text_fallback: str | None = None,
) -> CliResult:
    """Emit payload as JSON or text based on output format.

    Args:
        ctx: CLI context with output format preference.
        payload: Object to serialize.
        text_fallback: Optional text fallback for non-JSON output.

    Returns:
        CliResult: Wrapped CLI result.
    """
    if wants_json(ctx):
        return json_result(ctx, payload)

    if text_fallback is None and isinstance(payload, dict):
        message = payload.get("message")
        if isinstance(message, str) and message:
            text_fallback = message
    if text_fallback is None:
        text_fallback = json.dumps(payload, indent=2)

    return text_result(ctx, text_fallback)


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
