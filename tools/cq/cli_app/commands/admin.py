"""Administrative commands for cq CLI.

This module contains the index and cache management commands.
"""

from __future__ import annotations

import json
import sys
from typing import Annotated

from cyclopts import Parameter

# Import CliContext at runtime for cyclopts type hint resolution
from tools.cq.cli_app.context import CliContext


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
    """Index management (deprecated - caching has been removed).

    The caching infrastructure has been removed from cq.
    This command is a no-op placeholder.
    """
    # Parameters rebuild and status are deprecated placeholders - intentionally unused
    del rebuild, status

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    sys.stdout.write("Index management has been removed. Caching is no longer used.\n")
    return 0


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
    """Cache management (deprecated - caching has been removed).

    The caching infrastructure has been removed from cq.
    This command is a no-op placeholder.
    """
    # Parameters stats and clear are deprecated placeholders - intentionally unused
    del stats, clear

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    sys.stdout.write("Cache management has been removed. Caching is no longer used.\n")
    return 0


def schema(
    *,
    kind: Annotated[
        str,
        Parameter(
            name="--kind",
            help="Schema kind: result, query, or components",
        ),
    ] = "result",
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> int:
    """Emit msgspec JSON Schema for CQ types."""
    from tools.cq.core.schema_export import cq_result_schema, cq_schema_components, query_schema

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    kind_value = kind.strip().lower()
    if kind_value == "result":
        payload = cq_result_schema()
    elif kind_value == "query":
        payload = query_schema()
    elif kind_value == "components":
        schema_doc, components = cq_schema_components()
        payload = {"schema": schema_doc, "components": components}
    else:
        msg = f"Unknown schema kind: {kind}"
        raise ValueError(msg)

    sys.stdout.write(json.dumps(payload, indent=2))
    sys.stdout.write("\n")
    return 0
