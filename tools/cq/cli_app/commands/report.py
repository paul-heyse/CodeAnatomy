"""Report command for cq CLI.

This module contains the report command for running bundled analyses.
"""

from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

# Import CliContext at runtime for cyclopts type hint resolution
from tools.cq.cli_app.context import CliContext, CliResult, FilterConfig


def report(
    preset: Annotated[str, Parameter(help="Report preset (refactor-impact, safety-reliability, change-propagation, dependency-health)")],
    *,
    target: Annotated[str, Parameter(help="Target spec (function:foo, class:Bar, module:pkg.mod, path:src/...)")],
    in_dir: Annotated[str | None, Parameter(name="--in", help="Restrict analysis to a directory")] = None,
    param: Annotated[str | None, Parameter(help="Parameter name for impact analysis")] = None,
    signature: Annotated[str | None, Parameter(name="--to", help="Proposed signature for sig-impact analysis")] = None,
    bytecode_show: Annotated[str | None, Parameter(name="--bytecode-show", help="Bytecode surface fields")] = None,
    no_cache: Annotated[bool, Parameter(name="--no-cache", help="Disable caching")] = False,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
    include: Annotated[list[str] | None, Parameter(help="Include patterns")] = None,
    exclude: Annotated[list[str] | None, Parameter(help="Exclude patterns")] = None,
    impact_filter: Annotated[str | None, Parameter(name="--impact", help="Impact filter")] = None,
    confidence: Annotated[str | None, Parameter(help="Confidence filter")] = None,
    severity: Annotated[str | None, Parameter(help="Severity filter")] = None,
    limit: Annotated[int | None, Parameter(help="Max findings")] = None,
) -> CliResult:
    """Run target-scoped report bundles."""
    from tools.cq.cli_app.context import CliResult, FilterConfig
    from tools.cq.core.bundles import BundleContext, parse_target_spec, run_bundle
    from tools.cq.core.schema import mk_result, mk_runmeta, ms
    from tools.cq.index.query_cache import QueryCache
    from tools.cq.index.sqlite_cache import IndexCache

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    # Validate preset
    valid_presets = {"refactor-impact", "safety-reliability", "change-propagation", "dependency-health"}
    if preset not in valid_presets:
        started_ms = ms()
        run = mk_runmeta(
            macro="report",
            argv=ctx.argv,
            root=str(ctx.root),
            started_ms=started_ms,
            toolchain=ctx.toolchain.to_dict(),
        )
        result = mk_result(run)
        result.summary["error"] = f"Invalid preset: {preset}. Must be one of: {', '.join(sorted(valid_presets))}"
        filters = _build_filters(include, exclude, impact_filter, confidence, severity, limit)
        return CliResult(result=result, context=ctx, filters=filters)

    # Parse target spec
    try:
        target_spec = parse_target_spec(target)
    except ValueError as exc:
        started_ms = ms()
        run = mk_runmeta(
            macro="report",
            argv=ctx.argv,
            root=str(ctx.root),
            started_ms=started_ms,
            toolchain=ctx.toolchain.to_dict(),
        )
        result = mk_result(run)
        result.summary["error"] = str(exc)
        filters = _build_filters(include, exclude, impact_filter, confidence, severity, limit)
        return CliResult(result=result, context=ctx, filters=filters)

    use_cache = not no_cache
    index_cache: IndexCache | None = None
    query_cache: QueryCache | None = None

    if use_cache:
        rule_version = ctx.toolchain.sgpy_version or "unknown"
        index_cache = IndexCache(ctx.root, rule_version)
        index_cache.initialize()
        query_cache = QueryCache(ctx.root / ".cq" / "cache")

    bundle_ctx = BundleContext(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        target=target_spec,
        in_dir=in_dir,
        param=param,
        signature=signature,
        bytecode_show=bytecode_show,
        use_cache=use_cache,
        index_cache=index_cache,
        query_cache=query_cache,
    )

    if index_cache is None or query_cache is None:
        result = run_bundle(preset, bundle_ctx)
    else:
        with index_cache, query_cache:
            result = run_bundle(preset, bundle_ctx)

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
