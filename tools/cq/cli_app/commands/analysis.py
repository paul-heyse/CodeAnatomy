"""Analysis commands for cq CLI.

This module contains analysis commands: impact, calls, imports, exceptions,
sig_impact, side_effects, scopes, bytecode_surface.
"""

from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

# Import CliContext at runtime for cyclopts type hint resolution
from tools.cq.cli_app.context import CliContext, CliResult, FilterConfig


def impact(
    function: Annotated[str, Parameter(help="Function name to analyze")],
    *,
    param: Annotated[str, Parameter(help="Parameter name to trace")],
    depth: Annotated[int, Parameter(help="Maximum call depth")] = 5,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
    include: Annotated[list[str] | None, Parameter(help="Include patterns")] = None,
    exclude: Annotated[list[str] | None, Parameter(help="Exclude patterns")] = None,
    impact_filter: Annotated[str | None, Parameter(name="--impact", help="Impact filter")] = None,
    confidence: Annotated[str | None, Parameter(help="Confidence filter")] = None,
    severity: Annotated[str | None, Parameter(help="Severity filter")] = None,
    limit: Annotated[int | None, Parameter(help="Max findings")] = None,
) -> CliResult:
    """Trace data flow from a function parameter.

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
    from tools.cq.macros.impact import ImpactRequest, cmd_impact

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    request = ImpactRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        function_name=function,
        param_name=param,
        max_depth=depth,
    )
    result = cmd_impact(request)

    filters = _build_filters(include, exclude, impact_filter, confidence, severity, limit)
    return CliResult(result=result, context=ctx, filters=filters)


def calls(
    function: Annotated[str, Parameter(help="Function name to find calls for")],
    *,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
    include: Annotated[list[str] | None, Parameter(help="Include patterns")] = None,
    exclude: Annotated[list[str] | None, Parameter(help="Exclude patterns")] = None,
    impact_filter: Annotated[str | None, Parameter(name="--impact", help="Impact filter")] = None,
    confidence: Annotated[str | None, Parameter(help="Confidence filter")] = None,
    severity: Annotated[str | None, Parameter(help="Severity filter")] = None,
    limit: Annotated[int | None, Parameter(help="Max findings")] = None,
) -> CliResult:
    """Census all call sites for a function.

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
    from tools.cq.macros.calls import cmd_calls

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    result = cmd_calls(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        function_name=function,
    )

    filters = _build_filters(include, exclude, impact_filter, confidence, severity, limit)
    return CliResult(result=result, context=ctx, filters=filters)


def imports(
    *,
    cycles: Annotated[bool, Parameter(help="Run cycle detection")] = False,
    module: Annotated[str | None, Parameter(help="Focus on specific module")] = None,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
    include: Annotated[list[str] | None, Parameter(help="Include patterns")] = None,
    exclude: Annotated[list[str] | None, Parameter(help="Exclude patterns")] = None,
    impact_filter: Annotated[str | None, Parameter(name="--impact", help="Impact filter")] = None,
    confidence: Annotated[str | None, Parameter(help="Confidence filter")] = None,
    severity: Annotated[str | None, Parameter(help="Severity filter")] = None,
    limit: Annotated[int | None, Parameter(help="Max findings")] = None,
) -> CliResult:
    """Analyze import structure and cycles.

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
    from tools.cq.macros.imports import ImportRequest, cmd_imports

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    request = ImportRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        cycles=cycles,
        module=module,
    )
    result = cmd_imports(request)

    filters = _build_filters(include, exclude, impact_filter, confidence, severity, limit)
    return CliResult(result=result, context=ctx, filters=filters)


def exceptions(
    *,
    function: Annotated[str | None, Parameter(help="Focus on specific function")] = None,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
    include: Annotated[list[str] | None, Parameter(help="Include patterns")] = None,
    exclude: Annotated[list[str] | None, Parameter(help="Exclude patterns")] = None,
    impact_filter: Annotated[str | None, Parameter(name="--impact", help="Impact filter")] = None,
    confidence: Annotated[str | None, Parameter(help="Confidence filter")] = None,
    severity: Annotated[str | None, Parameter(help="Severity filter")] = None,
    limit: Annotated[int | None, Parameter(help="Max findings")] = None,
) -> CliResult:
    """Analyze exception handling patterns.

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
    from tools.cq.macros.exceptions import cmd_exceptions

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    result = cmd_exceptions(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        function=function,
    )

    filters = _build_filters(include, exclude, impact_filter, confidence, severity, limit)
    return CliResult(result=result, context=ctx, filters=filters)


def sig_impact(
    symbol: Annotated[str, Parameter(help="Function name to analyze")],
    *,
    to: Annotated[str, Parameter(help='New signature (e.g., "foo(a, b, *, c=None)")')],
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
    include: Annotated[list[str] | None, Parameter(help="Include patterns")] = None,
    exclude: Annotated[list[str] | None, Parameter(help="Exclude patterns")] = None,
    impact_filter: Annotated[str | None, Parameter(name="--impact", help="Impact filter")] = None,
    confidence: Annotated[str | None, Parameter(help="Confidence filter")] = None,
    severity: Annotated[str | None, Parameter(help="Severity filter")] = None,
    limit: Annotated[int | None, Parameter(help="Max findings")] = None,
) -> CliResult:
    """Analyze impact of a signature change.

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
    from tools.cq.macros.sig_impact import SigImpactRequest, cmd_sig_impact

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    request = SigImpactRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        symbol=symbol,
        to=to,
    )
    result = cmd_sig_impact(request)

    filters = _build_filters(include, exclude, impact_filter, confidence, severity, limit)
    return CliResult(result=result, context=ctx, filters=filters)


def side_effects(
    *,
    max_files: Annotated[int, Parameter(help="Maximum files to scan")] = 2000,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
    include: Annotated[list[str] | None, Parameter(help="Include patterns")] = None,
    exclude: Annotated[list[str] | None, Parameter(help="Exclude patterns")] = None,
    impact_filter: Annotated[str | None, Parameter(name="--impact", help="Impact filter")] = None,
    confidence: Annotated[str | None, Parameter(help="Confidence filter")] = None,
    severity: Annotated[str | None, Parameter(help="Severity filter")] = None,
    limit: Annotated[int | None, Parameter(help="Max findings")] = None,
) -> CliResult:
    """Detect import-time side effects.

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
    from tools.cq.macros.side_effects import SideEffectsRequest, cmd_side_effects

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    request = SideEffectsRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        max_files=max_files,
    )
    result = cmd_side_effects(request)

    filters = _build_filters(include, exclude, impact_filter, confidence, severity, limit)
    return CliResult(result=result, context=ctx, filters=filters)


def scopes(
    target: Annotated[str, Parameter(help="File path or symbol name to analyze")],
    *,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
    include: Annotated[list[str] | None, Parameter(help="Include patterns")] = None,
    exclude: Annotated[list[str] | None, Parameter(help="Exclude patterns")] = None,
    impact_filter: Annotated[str | None, Parameter(name="--impact", help="Impact filter")] = None,
    confidence: Annotated[str | None, Parameter(help="Confidence filter")] = None,
    severity: Annotated[str | None, Parameter(help="Severity filter")] = None,
    limit: Annotated[int | None, Parameter(help="Max findings")] = None,
) -> CliResult:
    """Analyze scope capture (closures).

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
    from tools.cq.macros.scopes import ScopeRequest, cmd_scopes

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    request = ScopeRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        target=target,
    )
    result = cmd_scopes(request)

    filters = _build_filters(include, exclude, impact_filter, confidence, severity, limit)
    return CliResult(result=result, context=ctx, filters=filters)


def bytecode_surface(
    target: Annotated[str, Parameter(help="File path or symbol name to analyze")],
    *,
    show: Annotated[
        str, Parameter(help="What to show: globals,attrs,constants,opcodes")
    ] = "globals,attrs,constants",
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
    include: Annotated[list[str] | None, Parameter(help="Include patterns")] = None,
    exclude: Annotated[list[str] | None, Parameter(help="Exclude patterns")] = None,
    impact_filter: Annotated[str | None, Parameter(name="--impact", help="Impact filter")] = None,
    confidence: Annotated[str | None, Parameter(help="Confidence filter")] = None,
    severity: Annotated[str | None, Parameter(help="Severity filter")] = None,
    limit: Annotated[int | None, Parameter(help="Max findings")] = None,
) -> CliResult:
    """Analyze bytecode for hidden dependencies.

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
    from tools.cq.macros.bytecode import BytecodeSurfaceRequest, cmd_bytecode_surface

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    request = BytecodeSurfaceRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        target=target,
        show=show,
    )
    result = cmd_bytecode_surface(request)

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
