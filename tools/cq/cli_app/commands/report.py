"""Report command for cq CLI.

This module contains the report command for running bundled analyses.
"""

from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

# Import CliContext at runtime for cyclopts type hint resolution
from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.decorators import require_context, require_ctx
from tools.cq.cli_app.options import ReportOptions, options_from_params
from tools.cq.cli_app.params import ReportParams
from tools.cq.cli_app.types import ReportPreset


@require_ctx
def report(
    preset: Annotated[
        ReportPreset,
        Parameter(
            help="Report preset (refactor-impact, safety-reliability, change-propagation, dependency-health)"
        ),
    ],
    *,
    opts: Annotated[ReportParams, Parameter(name="*")],
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Run target-scoped report bundles.

    Args:
        preset: Bundle preset to run.
        opts: Parsed report options.
        ctx: Injected CLI context.

    Returns:
        CliResult: Renderable command result payload.
    """
    from tools.cq.cli_app.context import CliResult
    from tools.cq.core.bundles import BundleContext, parse_target_spec, run_bundle
    from tools.cq.core.run_context import RunContext
    from tools.cq.core.schema import mk_result, ms

    ctx = require_context(ctx)

    options = options_from_params(opts, type_=ReportOptions)

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

    result = run_bundle(str(preset), bundle_ctx)

    return CliResult(result=result, context=ctx, filters=options)
