"""Search command for cq CLI.

This module contains the 'search' command for smart code search.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.infrastructure import require_context
from tools.cq.cli_app.options import SearchOptions, options_from_params
from tools.cq.cli_app.params import SearchParams
from tools.cq.orchestration.request_factory import (
    RequestContextV1,
    RequestFactory,
    SearchRequestOptionsV1,
)


def search(
    query: Annotated[str, Parameter(help="Search query")],
    *,
    opts: Annotated[SearchParams, Parameter(name="*")] | None = None,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Search for code patterns with semantic enrichment.

    Args:
        query: Search string provided by the user.
        opts: Optional search command options.
        ctx: Injected CLI context.

    Returns:
        CliResult: Renderable command result payload.
    """
    from tools.cq.query.language import parse_query_language_scope
    from tools.cq.search._shared.types import QueryMode
    from tools.cq.search.pipeline.enrichment_contracts import parse_incremental_enrichment_mode
    from tools.cq.search.pipeline.smart_search import SMART_SEARCH_LIMITS

    ctx = require_context(ctx)
    # Determine mode
    if opts is None:
        opts = SearchParams()
    options = options_from_params(opts, type_=SearchOptions)

    mode: QueryMode | None = None
    if options.regex:
        mode = QueryMode.REGEX
    elif options.literal:
        mode = QueryMode.LITERAL

    # Treat --in as scan scope, not a root override
    include_globs: list[str] = list(options.include) if options.include else []
    if options.in_dir:
        in_value = options.in_dir.rstrip("/")
        requested = Path(in_value)
        candidate = requested if requested.is_absolute() else (ctx.root / requested)
        looks_like_file = candidate.is_file() or (requested.suffix and not in_value.endswith("/"))
        include_globs.append(in_value if looks_like_file else f"{in_value}/**")

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.search(
        request_ctx,
        query=query,
        options=SearchRequestOptionsV1(
            mode=mode,
            lang_scope=parse_query_language_scope(str(options.lang)),
            include_globs=include_globs if include_globs else None,
            exclude_globs=list(options.exclude) if options.exclude else None,
            include_strings=options.include_strings,
            with_neighborhood=options.with_neighborhood,
            limits=SMART_SEARCH_LIMITS,
            incremental_enrichment_enabled=options.enrich,
            incremental_enrichment_mode=parse_incremental_enrichment_mode(options.enrich_mode),
        ),
    )

    result = ctx.services.search.execute(request)

    return CliResult(result=result, context=ctx, filters=options)
