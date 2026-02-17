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
from tools.cq.core.result_factory import build_error_result
from tools.cq.orchestration.request_factory import (
    RequestContextV1,
    RequestFactory,
    SearchRequestOptionsV1,
)


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
    from tools.cq.core.schema import ms
    from tools.cq.query.executor_runtime import ExecutePlanRequestV1, execute_plan
    from tools.cq.query.parser import QueryParseError, has_query_tokens, parse_query
    from tools.cq.query.planner import compile_query

    ctx = require_context(ctx)
    if opts is None:
        opts = QueryParams()
    options = query_options_from_projected_params(opts)
    has_tokens = has_query_tokens(query_string)

    # Parse the query string first; fallback only for plain searches.
    try:
        parsed_query = parse_query(query_string)
    except QueryParseError as e:
        if not has_tokens:
            from tools.cq.core.types import DEFAULT_QUERY_LANGUAGE_SCOPE
            from tools.cq.search.pipeline.smart_search import SMART_SEARCH_LIMITS

            # Build include globs from include patterns
            include_globs = options.include if options.include else None

            request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
            request = RequestFactory.search(
                request_ctx,
                query=query_string,
                options=SearchRequestOptionsV1(
                    mode=None,
                    lang_scope=DEFAULT_QUERY_LANGUAGE_SCOPE,
                    include_globs=include_globs,
                    exclude_globs=options.exclude if options.exclude else None,
                    include_strings=False,
                    limits=SMART_SEARCH_LIMITS,
                ),
            )

            result = ctx.services.search.execute(request)
            return CliResult(result=result, context=ctx, filters=options)
        result = build_error_result(
            macro="q",
            root=ctx.root,
            argv=ctx.argv,
            tc=ctx.toolchain,
            started_ms=ms(),
            error=e,
        )
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
            services=ctx.services,
            argv=tuple(ctx.argv),
            query_text=query_string,
        ),
        tc=ctx.toolchain,
    )

    return CliResult(result=result, context=ctx, filters=options)
