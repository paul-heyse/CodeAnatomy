"""Administrative CQ commands."""

from __future__ import annotations

import json
import sys
from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext
from tools.cq.cli_app.decorators import require_context, require_ctx
from tools.cq.cli_app.types import OutputFormat, SchemaKind
from tools.cq.core.schema_export import cq_result_schema, cq_schema_components, query_schema


def _emit_payload(ctx: CliContext, payload: object) -> int:
    if ctx.output_format == OutputFormat.json:
        sys.stdout.write(f"{json.dumps(payload, indent=2)}\n")
        return 0
    if isinstance(payload, dict):
        message = payload.get("message")
        if isinstance(message, str) and message:
            sys.stdout.write(f"{message}\n")
            return 0
    sys.stdout.write(f"{json.dumps(payload, indent=2)}\n")
    return 0


@require_ctx
def index(
    *,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> int:
    """Show deprecation notice for removed index management command.

    Returns:
        int: Process exit code.
    """
    ctx = require_context(ctx)
    return _emit_payload(
        ctx,
        {
            "deprecated": True,
            "message": "Index management has been removed. Caching is no longer used.",
        },
    )


@require_ctx
def cache(
    *,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> int:
    """Show deprecation notice for removed cache management command.

    Returns:
        int: Process exit code.
    """
    ctx = require_context(ctx)
    return _emit_payload(
        ctx,
        {
            "deprecated": True,
            "message": "Cache management has been removed. Caching is no longer used.",
        },
    )


@require_ctx
def schema(
    *,
    kind: Annotated[SchemaKind, Parameter(help="Schema export kind")] = SchemaKind.result,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> int:
    """Export JSON schema payloads for CQ contracts.

    Returns:
        int: Process exit code.
    """
    ctx = require_context(ctx)
    if kind == SchemaKind.result:
        payload: object = cq_result_schema()
    elif kind == SchemaKind.query:
        payload = query_schema()
    else:
        schema_rows, components = cq_schema_components()
        payload = {"schema": schema_rows, "components": components}
    return _emit_payload(ctx, payload)


__all__ = ["cache", "index", "schema"]
