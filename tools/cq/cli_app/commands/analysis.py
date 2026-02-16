"""Analysis commands for cq CLI.

This module contains analysis commands: impact, calls, imports, exceptions,
sig_impact, side_effects, scopes, bytecode_surface.
"""

from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

# Import CliContext at runtime for cyclopts type hint resolution
from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.infrastructure import require_context
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
from tools.cq.core.request_factory import RequestContextV1, RequestFactory


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
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.macros.impact import cmd_impact

    ctx = require_context(ctx)
    options = options_from_params(opts, type_=ImpactOptions)

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.impact(
        request_ctx,
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
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.core.bootstrap import resolve_runtime_services

    ctx = require_context(ctx)
    if opts is None:
        opts = FilterParams()
    options = options_from_params(opts, type_=CommonFilters)

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.calls(request_ctx, function_name=function)

    services = resolve_runtime_services(ctx.root)
    result = services.calls.execute(request)

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
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.macros.imports import cmd_imports

    ctx = require_context(ctx)
    if opts is None:
        opts = ImportsParams()
    options = options_from_params(opts, type_=ImportsOptions)

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.imports_cmd(
        request_ctx,
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
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.macros.exceptions import cmd_exceptions

    ctx = require_context(ctx)
    if opts is None:
        opts = ExceptionsParams()
    options = options_from_params(opts, type_=ExceptionsOptions)

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.exceptions(
        request_ctx,
        function=options.function,
        include=options.include,
        exclude=options.exclude,
    )
    result = cmd_exceptions(request)

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
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.macros.sig_impact import cmd_sig_impact

    ctx = require_context(ctx)
    options = options_from_params(opts, type_=SigImpactOptions)

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.sig_impact(request_ctx, symbol=symbol, to=options.to)
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
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.macros.side_effects import cmd_side_effects

    ctx = require_context(ctx)
    if opts is None:
        opts = SideEffectsParams()
    options = options_from_params(opts, type_=SideEffectsOptions)

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.side_effects(
        request_ctx,
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
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.macros.scopes import cmd_scopes

    ctx = require_context(ctx)
    if opts is None:
        opts = FilterParams()
    options = options_from_params(opts, type_=CommonFilters)

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.scopes(request_ctx, target=target)
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
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.macros.bytecode import cmd_bytecode_surface

    ctx = require_context(ctx)
    if opts is None:
        opts = BytecodeSurfaceParams()
    options = options_from_params(opts, type_=BytecodeSurfaceOptions)

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.bytecode_surface(request_ctx, target=target, show=options.show)
    result = cmd_bytecode_surface(request)

    return CliResult(result=result, context=ctx, filters=options)
