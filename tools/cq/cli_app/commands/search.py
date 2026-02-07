"""Search command for cq CLI.

This module contains the 'search' command for smart code search.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.options import SearchOptions, options_from_params
from tools.cq.cli_app.params import SearchParams


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

    Raises:
        RuntimeError: If command context is not injected.
    """
    from tools.cq.query.language import parse_query_language_scope
    from tools.cq.search.classifier import QueryMode
    from tools.cq.search.smart_search import SMART_SEARCH_LIMITS, smart_search

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

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

    result = smart_search(
        ctx.root,
        query,
        mode=mode,
        lang_scope=parse_query_language_scope(options.lang),
        include_globs=include_globs if include_globs else None,
        exclude_globs=list(options.exclude) if options.exclude else None,
        include_strings=options.include_strings,
        limits=SMART_SEARCH_LIMITS,
        tc=ctx.toolchain,
        argv=ctx.argv,
    )

    return CliResult(result=result, context=ctx, filters=options)
