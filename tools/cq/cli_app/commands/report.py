"""Report command for cq CLI.

This module contains the report command for running bundled analyses.
"""

from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

# Import CliContext at runtime for cyclopts type hint resolution
from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.options import ReportOptions, options_from_params
from tools.cq.cli_app.params import ReportParams


def report(
    preset: Annotated[
        str,
        Parameter(
            help="Report preset (refactor-impact, safety-reliability, change-propagation, dependency-health)"
        ),
    ],
    *,
    opts: Annotated[ReportParams, Parameter(name="*")],
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Run target-scoped report bundles.

    Returns
    -------
    CliResult
        Report results with context and filters.

    Raises
    ------
    RuntimeError
        If the CLI context was not injected.
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.core.bundles import BundleContext, parse_target_spec, run_bundle
    from tools.cq.core.run_context import RunContext
    from tools.cq.core.schema import mk_result, ms

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    options = options_from_params(opts, type_=ReportOptions)

    if options.target is None:
        started_ms = ms()
        run_ctx = RunContext.from_parts(
            root=ctx.root,
            argv=ctx.argv,
            tc=ctx.toolchain,
            started_ms=started_ms,
        )
        run = run_ctx.to_runmeta("report")
        result = mk_result(run)
        result.summary["error"] = "Target spec is required"
        return CliResult(result=result, context=ctx, filters=options)

    # Validate preset
    valid_presets = {
        "refactor-impact",
        "safety-reliability",
        "change-propagation",
        "dependency-health",
    }
    if preset not in valid_presets:
        started_ms = ms()
        run_ctx = RunContext.from_parts(
            root=ctx.root,
            argv=ctx.argv,
            tc=ctx.toolchain,
            started_ms=started_ms,
        )
        run = run_ctx.to_runmeta("report")
        result = mk_result(run)
        result.summary["error"] = (
            f"Invalid preset: {preset}. Must be one of: {', '.join(sorted(valid_presets))}"
        )
        return CliResult(result=result, context=ctx, filters=options)

    # Parse target spec
    try:
        target_spec = parse_target_spec(options.target)
    except ValueError as exc:
        started_ms = ms()
        run_ctx = RunContext.from_parts(
            root=ctx.root,
            argv=ctx.argv,
            tc=ctx.toolchain,
            started_ms=started_ms,
        )
        run = run_ctx.to_runmeta("report")
        result = mk_result(run)
        result.summary["error"] = str(exc)
        return CliResult(result=result, context=ctx, filters=options)

    bundle_ctx = BundleContext(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        target=target_spec,
        in_dir=options.in_dir,
        param=options.param,
        signature=options.signature,
        bytecode_show=options.bytecode_show,
    )

    result = run_bundle(preset, bundle_ctx)

    return CliResult(result=result, context=ctx, filters=options)
