"""Query command for cq CLI.

This module contains the 'q' query command.
"""

from __future__ import annotations

import re
from typing import Annotated

import msgspec
from cyclopts import Parameter

# Import CliContext at runtime for cyclopts type hint resolution
from tools.cq.cli_app.context import CliContext, CliResult, FilterConfig


def _has_query_tokens(query_string: str) -> bool:
    """Check whether a query string contains any key=value tokens.

    Returns
    -------
    bool
        True if any tokenized query parts are present.
    """
    token_pattern = r"([\w.]+|\$+\w+)=(?:'([^']+)'|\"([^\"]+)\"|([^\s]+))"
    return bool(list(re.finditer(token_pattern, query_string)))


def q(
    query_string: Annotated[str, Parameter(help='Query string (e.g., "entity=function name=foo")')],
    *,
    explain_files: Annotated[
        bool, Parameter(name="--explain-files", help="Include file filtering diagnostics")
    ] = False,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
    include: Annotated[list[str] | None, Parameter(help="Include patterns")] = None,
    exclude: Annotated[list[str] | None, Parameter(help="Exclude patterns")] = None,
    impact_filter: Annotated[str | None, Parameter(name="--impact", help="Impact filter")] = None,
    confidence: Annotated[str | None, Parameter(help="Confidence filter")] = None,
    severity: Annotated[str | None, Parameter(help="Severity filter")] = None,
    limit: Annotated[int | None, Parameter(help="Max findings")] = None,
) -> CliResult:
    """Run a declarative code query using ast-grep.

    Query syntax: key=value pairs separated by spaces.

    Required:
        entity=TYPE    Entity type (function, class, method, module, callsite, import)

    Optional:
        name=PATTERN   Name to match (exact or ~regex)
        expand=KIND    Graph expansion (callers, callees, imports, raises, scope)
                       Use KIND(depth=N) to set depth (default: 1)
        in=DIR         Search only in directory
        exclude=DIRS   Exclude directories (comma-separated)
        fields=FIELDS  Output fields (def,loc,callers,callees,evidence,imports)
        limit=N        Maximum results
        explain=true   Include query plan explanation

    For plain queries without key=value pairs (e.g., "build_graph"),
    this command falls back to Smart Search.

    Examples
    --------
        cq q "entity=function name=build_graph_product"
        cq q "entity=function name=detect expand=callers(depth=2) in=tools/cq/"
        cq q "entity=class in=src/relspec/ fields=def,imports"
        cq q build_graph  # falls back to smart search

    Returns
    -------
    CliResult
        Structured command result.

    Raises
    ------
    RuntimeError
        Raised when CLI context is unavailable.
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.core.schema import mk_result, mk_runmeta, ms
    from tools.cq.query.executor import execute_plan
    from tools.cq.query.parser import QueryParseError, parse_query
    from tools.cq.query.planner import compile_query

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    has_tokens = _has_query_tokens(query_string)

    # Parse the query string first; fallback only for plain searches.
    try:
        parsed_query = parse_query(query_string)
    except QueryParseError as e:
        if not has_tokens:
            from tools.cq.search.smart_search import SMART_SEARCH_LIMITS, smart_search

            # Build include globs from include patterns
            include_globs = list(include) if include else None

            result = smart_search(
                ctx.root,
                query_string,
                mode=None,  # Auto-detect
                include_globs=include_globs,
                exclude_globs=list(exclude) if exclude else None,
                include_strings=False,
                limits=SMART_SEARCH_LIMITS,
                tc=ctx.toolchain,
                argv=ctx.argv,
            )
            filters = _build_filters(include, exclude, impact_filter, confidence, severity, limit)
            return CliResult(result=result, context=ctx, filters=filters)
        started_ms = ms()
        run = mk_runmeta(
            macro="q",
            argv=ctx.argv,
            root=str(ctx.root),
            started_ms=started_ms,
            toolchain=ctx.toolchain.to_dict(),
        )
        result = mk_result(run)
        result.summary["error"] = str(e)
        filters = _build_filters(include, exclude, impact_filter, confidence, severity, limit)
        return CliResult(result=result, context=ctx, filters=filters)

    if explain_files and not parsed_query.explain:
        parsed_query = msgspec.structs.replace(parsed_query, explain=True)

    # Compile and execute
    plan = compile_query(parsed_query)
    result = execute_plan(
        plan=plan,
        query=parsed_query,
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
    )

    filters = _build_filters(include, exclude, impact_filter, confidence, severity, limit)
    return CliResult(result=result, context=ctx, filters=filters)


def _build_filters(
    include: list[str] | None,
    exclude: list[str] | None,
    impact: str | None,
    confidence: str | None,
    severity: str | None,
    limit: int | None,
) -> FilterConfig:
    """Build a FilterConfig from CLI arguments.

    Parameters
    ----------
    include
        Include patterns.
    exclude
        Exclude patterns.
    impact
        Comma-separated impact buckets.
    confidence
        Comma-separated confidence buckets.
    severity
        Comma-separated severity levels.
    limit
        Maximum findings.

    Returns
    -------
    FilterConfig
        Filter configuration.
    """
    from tools.cq.cli_app.context import FilterConfig

    impact_list: list[str] = []
    if impact:
        for part in impact.split(","):
            segment = part.strip()
            if segment:
                impact_list.append(segment)

    confidence_list: list[str] = []
    if confidence:
        for part in confidence.split(","):
            segment = part.strip()
            if segment:
                confidence_list.append(segment)

    severity_list: list[str] = []
    if severity:
        for part in severity.split(","):
            segment = part.strip()
            if segment:
                severity_list.append(segment)

    return FilterConfig(
        include=list(include) if include else [],
        exclude=list(exclude) if exclude else [],
        impact=impact_list,
        confidence=confidence_list,
        severity=severity_list,
        limit=limit,
    )
