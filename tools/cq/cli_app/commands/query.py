"""Query command for cq CLI.

This module contains the 'q' query command.
"""

from __future__ import annotations

import re
from typing import Annotated

import msgspec
from cyclopts import Parameter

# Import CliContext at runtime for cyclopts type hint resolution
from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.decorators import require_context, require_ctx
from tools.cq.cli_app.options import QueryOptions, options_from_params
from tools.cq.cli_app.params import QueryParams


def _has_query_tokens(query_string: str) -> bool:
    """Check whether a query string contains any key=value tokens.

    Returns:
        bool: True if any tokenized query parts are present.
    """
    token_pattern = r"([\w.]+|\$+\w+)=(?:'([^']+)'|\"([^\"]+)\"|([^\s]+))"
    return bool(list(re.finditer(token_pattern, query_string)))


@require_ctx
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
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.core.bootstrap import resolve_runtime_services
    from tools.cq.core.run_context import RunContext
    from tools.cq.core.schema import mk_result, ms
    from tools.cq.core.services import SearchServiceRequest
    from tools.cq.query.executor import ExecutePlanRequestV1, execute_plan
    from tools.cq.query.parser import QueryParseError, parse_query
    from tools.cq.query.planner import compile_query

    ctx = require_context(ctx)
    if opts is None:
        opts = QueryParams()
    options = options_from_params(opts, type_=QueryOptions)
    has_tokens = _has_query_tokens(query_string)

    # Parse the query string first; fallback only for plain searches.
    try:
        parsed_query = parse_query(query_string)
    except QueryParseError as e:
        if not has_tokens:
            from tools.cq.query.language import DEFAULT_QUERY_LANGUAGE_SCOPE
            from tools.cq.search.smart_search import SMART_SEARCH_LIMITS

            # Build include globs from include patterns
            include_globs = options.include if options.include else None

            services = resolve_runtime_services(ctx.root)
            result = services.search.execute(
                SearchServiceRequest(
                    root=ctx.root,
                    query=query_string,
                    mode=None,  # Auto-detect
                    lang_scope=DEFAULT_QUERY_LANGUAGE_SCOPE,
                    include_globs=include_globs,
                    exclude_globs=options.exclude if options.exclude else None,
                    include_strings=False,
                    limits=SMART_SEARCH_LIMITS,
                    tc=ctx.toolchain,
                    argv=ctx.argv,
                )
            )
            return CliResult(result=result, context=ctx, filters=options)
        started_ms = ms()
        run_ctx = RunContext.from_parts(
            root=ctx.root,
            argv=ctx.argv,
            tc=ctx.toolchain,
            started_ms=started_ms,
        )
        run = run_ctx.to_runmeta("q")
        result = mk_result(run)
        result.summary["error"] = str(e)
        return CliResult(result=result, context=ctx, filters=options)

    if options.explain_files and not parsed_query.explain:
        parsed_query = msgspec.structs.replace(parsed_query, explain=True)

    # Compile and execute
    plan = compile_query(parsed_query)
    result = execute_plan(
        ExecutePlanRequestV1(
            plan=plan,
            query=parsed_query,
            root=str(ctx.root),
            argv=tuple(ctx.argv),
            query_text=query_string,
        ),
        tc=ctx.toolchain,
    )

    return CliResult(result=result, context=ctx, filters=options)
