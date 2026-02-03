"""Query command for cq CLI.

This module contains the 'q' query command.
"""

from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter
import msgspec

# Import CliContext at runtime for cyclopts type hint resolution
from tools.cq.cli_app.context import CliContext, CliResult, FilterConfig


def q(
    query_string: Annotated[str, Parameter(help='Query string (e.g., "entity=function name=foo")')],
    *,
    explain_files: Annotated[bool, Parameter(name="--explain-files", help="Include file filtering diagnostics")] = False,
    no_cache: Annotated[bool, Parameter(name="--no-cache", help="Disable query result caching")] = False,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
    include: Annotated[list[str] | None, Parameter(help="Include patterns")] = None,
    exclude: Annotated[list[str] | None, Parameter(help="Exclude patterns")] = None,
    impact_filter: Annotated[str | None, Parameter(name="--impact", help="Impact filter")] = None,
    confidence: Annotated[str | None, Parameter(help="Confidence filter")] = None,
    severity: Annotated[str | None, Parameter(help="Severity filter")] = None,
    limit: Annotated[int | None, Parameter(help="Max findings")] = None,
) -> CliResult:
    """Declarative code query using ast-grep.

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

    Examples:
        cq q "entity=function name=build_graph_product"
        cq q "entity=function name=detect expand=callers(depth=2) in=tools/cq/"
        cq q "entity=class in=src/relspec/ fields=def,imports"
    """
    from tools.cq.cli_app.context import CliResult, FilterConfig
    from tools.cq.cache.diskcache_profile import default_cq_diskcache_profile
    from tools.cq.core.schema import mk_result, mk_runmeta, ms
    from tools.cq.index.diskcache_query_cache import QueryCache
    from tools.cq.index.diskcache_index_cache import IndexCache
    from tools.cq.query.executor import execute_plan
    from tools.cq.query.parser import QueryParseError, parse_query
    from tools.cq.query.planner import compile_query

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    # Parse the query string
    try:
        parsed_query = parse_query(query_string)
    except QueryParseError as e:
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
    use_cache = not no_cache
    index_cache: IndexCache | None = None
    query_cache: QueryCache | None = None

    if use_cache:
        rule_version = ctx.toolchain.sgpy_version or "unknown"
        profile = default_cq_diskcache_profile()
        index_cache = IndexCache(ctx.root, rule_version, profile=profile)
        index_cache.initialize()
        query_cache = QueryCache(ctx.root, profile=profile)

    if index_cache is None or query_cache is None:
        result = execute_plan(
            plan=plan,
            query=parsed_query,
            tc=ctx.toolchain,
            root=ctx.root,
            argv=ctx.argv,
            use_cache=False,
        )
    else:
        with index_cache, query_cache:
            result = execute_plan(
                plan=plan,
                query=parsed_query,
                tc=ctx.toolchain,
                root=ctx.root,
                argv=ctx.argv,
                index_cache=index_cache,
                query_cache=query_cache,
                use_cache=use_cache,
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
            part = part.strip()
            if part:
                impact_list.append(part)

    confidence_list: list[str] = []
    if confidence:
        for part in confidence.split(","):
            part = part.strip()
            if part:
                confidence_list.append(part)

    severity_list: list[str] = []
    if severity:
        for part in severity.split(","):
            part = part.strip()
            if part:
                severity_list.append(part)

    return FilterConfig(
        include=list(include) if include else [],
        exclude=list(exclude) if exclude else [],
        impact=impact_list,
        confidence=confidence_list,
        severity=severity_list,
        limit=limit,
    )
