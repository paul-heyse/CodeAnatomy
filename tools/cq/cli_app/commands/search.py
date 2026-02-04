"""Search command for cq CLI.

This module contains the 'search' command for smart code search.
"""

from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext, CliResult, FilterConfig


def search(
    query: Annotated[str, Parameter(help="Search query")],
    *,
    regex: Annotated[bool, Parameter(name="--regex", help="Treat query as regex")] = False,
    literal: Annotated[bool, Parameter(name="--literal", help="Treat query as literal")] = False,
    include_strings: Annotated[
        bool,
        Parameter(name="--include-strings", help="Include matches in strings/comments/docstrings"),
    ] = False,
    in_dir: Annotated[str | None, Parameter(name="--in", help="Restrict to directory")] = None,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
    include: Annotated[list[str] | None, Parameter(help="Include patterns")] = None,
    exclude: Annotated[list[str] | None, Parameter(help="Exclude patterns")] = None,
    impact_filter: Annotated[str | None, Parameter(name="--impact", help="Impact filter")] = None,
    confidence: Annotated[str | None, Parameter(help="Confidence filter")] = None,
    severity: Annotated[str | None, Parameter(help="Severity filter")] = None,
    limit: Annotated[int | None, Parameter(help="Max findings")] = None,
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
    mode: QueryMode | None = None
    if regex:
        mode = QueryMode.REGEX
    elif literal:
        mode = QueryMode.LITERAL

    # Treat --in as scan scope, not a root override
    include_globs: list[str] = list(include) if include else []
    if in_dir:
        include_globs.append(f"{in_dir}/**")

    result = smart_search(
        ctx.root,
        query,
        mode=mode,
        include_globs=include_globs if include_globs else None,
        exclude_globs=list(exclude) if exclude else None,
        include_strings=include_strings,
        limits=SMART_SEARCH_LIMITS,
        tc=ctx.toolchain,
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
