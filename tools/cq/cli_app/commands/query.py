"""Query command for cq CLI.

This module contains the 'q' query command.
"""

from __future__ import annotations

from typing import Annotated

import msgspec
from cyclopts import Parameter

# Import CliContext at runtime for cyclopts type hint resolution
from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.infrastructure import require_context
from tools.cq.cli_app.params import QueryParams
from tools.cq.cli_app.schema_projection import query_options_from_projected_params
from tools.cq.query.router import route_query_or_search


def q(
    query_string: Annotated[str, Parameter(help='Query string (e.g., "entity=function name=foo")')],
    *,
    opts: Annotated[QueryParams, Parameter(name="*")] | None = None,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Run a declarative code query using ast-grep.

    Args:
        query_string: Query expression provided by the user.
        opts: Optional query command options.
        ctx: Injected CLI context.

    Returns:
        CliResult: Renderable command result payload.

    Raises:
        RuntimeError: If routing returns neither a parsed query nor fallback result.
    """
    from tools.cq.query.enrichment import SymtableEnricher
    from tools.cq.query.executor_plan_dispatch import ExecutePlanRequestV1, execute_plan
    from tools.cq.query.planner import compile_query

    ctx = require_context(ctx)
    if opts is None:
        opts = QueryParams()
    options = query_options_from_projected_params(opts)
    routing = route_query_or_search(
        query_string,
        root=ctx.root,
        argv=ctx.argv,
        toolchain=ctx.toolchain,
        services=ctx.services,
        include=options.include if options.include else None,
        exclude=options.exclude if options.exclude else None,
    )
    if routing.result is not None:
        return CliResult(result=routing.result, context=ctx, filters=options)
    parsed_query = routing.parsed_query
    if parsed_query is None:
        msg = "Query routing returned neither parsed query nor fallback result."
        raise RuntimeError(msg)

    if options.explain_files and not parsed_query.explain:
        parsed_query = msgspec.structs.replace(parsed_query, explain=True)

    # Compile and execute
    plan = compile_query(parsed_query)
    result = execute_plan(
        ExecutePlanRequestV1(
            plan=plan,
            query=parsed_query,
            root=str(ctx.root),
            services=ctx.services,
            symtable_enricher=SymtableEnricher(ctx.root),
            argv=tuple(ctx.argv),
            query_text=query_string,
        ),
        tc=ctx.toolchain,
    )

    return CliResult(result=result, context=ctx, filters=options)
