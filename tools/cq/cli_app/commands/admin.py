"""Administrative CQ commands."""

from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.infrastructure import require_context
from tools.cq.cli_app.protocol_output import emit_payload
from tools.cq.cli_app.types import SchemaKind
from tools.cq.core.schema_export import cq_result_schema, cq_schema_components, query_schema


def index(
    *,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Show deprecation notice for removed index management command.

    Returns:
        CliResult: Normalized CLI text result.
    """
    ctx = require_context(ctx)
    return emit_payload(
        ctx,
        {
            "deprecated": True,
            "message": "Index management has been removed. Caching is no longer used.",
        },
        text_fallback="Index management has been removed. Caching is no longer used.",
    )


def cache(
    *,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Show deprecation notice for removed cache management command.

    Returns:
        CliResult: Normalized CLI text result.
    """
    ctx = require_context(ctx)
    return emit_payload(
        ctx,
        {
            "deprecated": True,
            "message": "Cache management has been removed. Caching is no longer used.",
        },
        text_fallback="Cache management has been removed. Caching is no longer used.",
    )


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
    return emit_payload(ctx, payload)


__all__ = ["cache", "index", "schema"]
