"""Analysis commands for cq CLI.

This module contains analysis commands: impact, calls, imports, exceptions,
sig_impact, side_effects, scopes, bytecode_surface.
"""

from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

# Import CliContext at runtime for cyclopts type hint resolution
from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.options import (
    BytecodeSurfaceOptions,
    CommonFilters,
    ExceptionsOptions,
    ImpactOptions,
    ImportsOptions,
    SideEffectsOptions,
    SigImpactOptions,
    options_from_params,
)
from tools.cq.cli_app.params import (
    BytecodeSurfaceParams,
    ExceptionsParams,
    FilterParams,
    ImpactParams,
    ImportsParams,
    SideEffectsParams,
    SigImpactParams,
)


def impact(
    function: Annotated[str, Parameter(help="Function name to analyze")],
    *,
    opts: Annotated[ImpactParams, Parameter(name="*")],
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Trace data flow from a function parameter.

    Args:
        function: Function name to analyze.
        opts: Parsed CLI options for impact analysis.
        ctx: Injected CLI context.

    Returns:
        CliResult: Renderable command result payload.

    Raises:
        RuntimeError: If command context is not injected or required options are missing.
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.macros.impact import ImpactRequest, cmd_impact

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    options = options_from_params(opts, type_=ImpactOptions)
    if options.param is None:
        msg = "Parameter name is required"
        raise RuntimeError(msg)

    request = ImpactRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        function_name=function,
        param_name=options.param,
        max_depth=options.depth,
    )
    result = cmd_impact(request)

    return CliResult(result=result, context=ctx, filters=options)


def calls(
    function: Annotated[str, Parameter(help="Function name to find calls for")],
    *,
    opts: Annotated[FilterParams, Parameter(name="*")] | None = None,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Census all call sites for a function.

    Args:
        function: Function name to analyze.
        opts: Optional shared filter options.
        ctx: Injected CLI context.

    Returns:
        CliResult: Renderable command result payload.

    Raises:
        RuntimeError: If command context is not injected.
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.core.bootstrap import resolve_runtime_services
    from tools.cq.core.services import CallsServiceRequest

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    if opts is None:
        opts = FilterParams()
    options = options_from_params(opts, type_=CommonFilters)

    services = resolve_runtime_services(ctx.root)
    result = services.calls.execute(
        CallsServiceRequest(
            root=ctx.root,
            function_name=function,
            tc=ctx.toolchain,
            argv=ctx.argv,
        )
    )

    return CliResult(result=result, context=ctx, filters=options)


def imports(
    *,
    opts: Annotated[ImportsParams, Parameter(name="*")] | None = None,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Analyze import structure and cycles.

    Args:
        opts: Optional imports-specific options.
        ctx: Injected CLI context.

    Returns:
        CliResult: Renderable command result payload.

    Raises:
        RuntimeError: If command context is not injected.
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.macros.imports import ImportRequest, cmd_imports

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    if opts is None:
        opts = ImportsParams()
    options = options_from_params(opts, type_=ImportsOptions)

    request = ImportRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        cycles=options.cycles,
        module=options.module,
        include=options.include,
        exclude=options.exclude,
    )
    result = cmd_imports(request)

    return CliResult(result=result, context=ctx, filters=options)


def exceptions(
    *,
    opts: Annotated[ExceptionsParams, Parameter(name="*")] | None = None,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Analyze exception handling patterns.

    Args:
        opts: Optional exception-analysis options.
        ctx: Injected CLI context.

    Returns:
        CliResult: Renderable command result payload.

    Raises:
        RuntimeError: If command context is not injected.
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.macros.exceptions import cmd_exceptions

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    if opts is None:
        opts = ExceptionsParams()
    options = options_from_params(opts, type_=ExceptionsOptions)

    result = cmd_exceptions(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        function=options.function,
        include=options.include,
        exclude=options.exclude,
    )

    return CliResult(result=result, context=ctx, filters=options)


def sig_impact(
    symbol: Annotated[str, Parameter(help="Function name to analyze")],
    *,
    opts: Annotated[SigImpactParams, Parameter(name="*")],
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Analyze impact of a signature change.

    Args:
        symbol: Function symbol to analyze.
        opts: Parsed signature-impact options.
        ctx: Injected CLI context.

    Returns:
        CliResult: Renderable command result payload.

    Raises:
        RuntimeError: If command context is not injected or `--to` is missing.
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.macros.sig_impact import SigImpactRequest, cmd_sig_impact

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    options = options_from_params(opts, type_=SigImpactOptions)
    if options.to is None:
        msg = "Signature value is required"
        raise RuntimeError(msg)

    request = SigImpactRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        symbol=symbol,
        to=options.to,
    )
    result = cmd_sig_impact(request)

    return CliResult(result=result, context=ctx, filters=options)


def side_effects(
    *,
    opts: Annotated[SideEffectsParams, Parameter(name="*")] | None = None,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Detect import-time side effects.

    Args:
        opts: Optional side-effects options.
        ctx: Injected CLI context.

    Returns:
        CliResult: Renderable command result payload.

    Raises:
        RuntimeError: If command context is not injected.
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.macros.side_effects import SideEffectsRequest, cmd_side_effects

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    if opts is None:
        opts = SideEffectsParams()
    options = options_from_params(opts, type_=SideEffectsOptions)

    request = SideEffectsRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        max_files=options.max_files,
        include=options.include,
        exclude=options.exclude,
    )
    result = cmd_side_effects(request)

    return CliResult(result=result, context=ctx, filters=options)


def scopes(
    target: Annotated[str, Parameter(help="File path or symbol name to analyze")],
    *,
    opts: Annotated[FilterParams, Parameter(name="*")] | None = None,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Analyze scope capture (closures).

    Args:
        target: File path or symbol to inspect for closure capture.
        opts: Optional shared filter options.
        ctx: Injected CLI context.

    Returns:
        CliResult: Renderable command result payload.

    Raises:
        RuntimeError: If command context is not injected.
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.macros.scopes import ScopeRequest, cmd_scopes

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    if opts is None:
        opts = FilterParams()
    options = options_from_params(opts, type_=CommonFilters)

    request = ScopeRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        target=target,
    )
    result = cmd_scopes(request)

    return CliResult(result=result, context=ctx, filters=options)


def bytecode_surface(
    target: Annotated[str, Parameter(help="File path or symbol name to analyze")],
    *,
    opts: Annotated[BytecodeSurfaceParams, Parameter(name="*")] | None = None,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Analyze bytecode for hidden dependencies.

    Args:
        target: File path or symbol to inspect.
        opts: Optional bytecode-surface options.
        ctx: Injected CLI context.

    Returns:
        CliResult: Renderable command result payload.

    Raises:
        RuntimeError: If command context is not injected.
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.macros.bytecode import BytecodeSurfaceRequest, cmd_bytecode_surface

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    if opts is None:
        opts = BytecodeSurfaceParams()
    options = options_from_params(opts, type_=BytecodeSurfaceOptions)

    request = BytecodeSurfaceRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        target=target,
        show=options.show,
    )
    result = cmd_bytecode_surface(request)

    return CliResult(result=result, context=ctx, filters=options)
