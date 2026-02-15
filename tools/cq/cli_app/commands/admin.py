"""Administrative commands for cq CLI.

This module contains the index and cache management commands.
"""

from __future__ import annotations

import sys
from typing import Annotated

from cyclopts import Parameter

# Import CliContext at runtime for cyclopts type hint resolution
from tools.cq.cli_app.context import CliContext
from tools.cq.cli_app.decorators import require_context, require_ctx
from tools.cq.cli_app.types import OutputFormat, SchemaKind


def _emit_deprecated_message(ctx: CliContext, message: str) -> None:
    from tools.cq.core.codec import dumps_json_value

    if ctx.output_format == OutputFormat.json:
        sys.stdout.write(
            dumps_json_value(
                {
                    "deprecated": True,
                    "message": message,
                },
                indent=2,
            )
        )
        sys.stdout.write("\n")
        return
    sys.stdout.write(f"{message}\n")


@require_ctx
def index(
    *,
    rebuild: Annotated[
        bool,
        Parameter(name="--rebuild", help="(Deprecated) Rebuild index"),
    ] = False,
    status: Annotated[
        bool,
        Parameter(name="--status", help="(Deprecated) Show index status"),
    ] = False,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> int:
    """Handle deprecated index management flags.

    Args:
        rebuild: Deprecated rebuild flag.
        status: Deprecated status flag.
        ctx: Injected CLI context.

    Returns:
        int: Process status code.
    """
    ctx = require_context(ctx)

    # Parameters rebuild and status are deprecated placeholders - intentionally unused
    del rebuild, status

    _emit_deprecated_message(ctx, "Index management has been removed. Caching is no longer used.")
    return 0


@require_ctx
def cache(
    *,
    stats: Annotated[
        bool,
        Parameter(name="--stats", help="(Deprecated) Show cache statistics"),
    ] = False,
    clear: Annotated[
        bool,
        Parameter(name="--clear", help="(Deprecated) Clear cache"),
    ] = False,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> int:
    """Handle deprecated cache management flags.

    Args:
        stats: Deprecated stats flag.
        clear: Deprecated clear flag.
        ctx: Injected CLI context.

    Returns:
        int: Process status code.
    """
    ctx = require_context(ctx)

    # Parameters stats and clear are deprecated placeholders - intentionally unused
    del stats, clear

    _emit_deprecated_message(ctx, "Cache management has been removed. Caching is no longer used.")
    return 0


@require_ctx
def schema(
    *,
    kind: Annotated[
        SchemaKind,
        Parameter(
            name="--kind",
            help="Schema kind: result, query, or components",
        ),
    ] = SchemaKind.result,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> int:
    """Emit msgspec JSON Schema for CQ types.

    Args:
        kind: Schema kind to emit (`result`, `query`, or `components`).
        ctx: Injected CLI context.

    Returns:
        int: Process status code.
    """
    ctx = require_context(ctx)

    from tools.cq.core.codec import dumps_json_value
    from tools.cq.core.schema_export import cq_result_schema, cq_schema_components, query_schema

    if kind is SchemaKind.result:
        payload = cq_result_schema()
    elif kind is SchemaKind.query:
        payload = query_schema()
    else:
        schema_doc, components = cq_schema_components()
        payload = {"schema": schema_doc, "components": components}

    sys.stdout.write(dumps_json_value(payload, indent=2))
    sys.stdout.write("\n")
    return 0
