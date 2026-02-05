"""Search command for cq CLI.

This module contains the 'search' command for smart code search.
"""

from __future__ import annotations

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

    Smart Search provides semantically-enriched, grouped results using:
    - rpygrep for high-performance candidate generation
    - ast-grep for AST-level classification
    - symtable for scope analysis

    Examples
    --------
        cq search build_graph
        cq search "config.*path" --regex
        cq search "hello world" --literal
        cq search CqResult --in tools/cq/core/
        cq search foo --include-strings

    Returns
    -------
    CliResult
        Structured command result.

    Raises
    ------
    RuntimeError
        Raised when CLI context is unavailable.
    """
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
        include_globs.append(f"{options.in_dir}/**")

    result = smart_search(
        ctx.root,
        query,
        mode=mode,
        include_globs=include_globs if include_globs else None,
        exclude_globs=list(options.exclude) if options.exclude else None,
        include_strings=options.include_strings,
        limits=SMART_SEARCH_LIMITS,
        tc=ctx.toolchain,
        argv=ctx.argv,
    )

    return CliResult(result=result, context=ctx, filters=options)
