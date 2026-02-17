"""Report command for cq CLI.

This module contains the report command for running bundled analyses.
"""

from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

# Import CliContext at runtime for cyclopts type hint resolution
from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.infrastructure import require_context
from tools.cq.cli_app.params import ReportParams
from tools.cq.cli_app.schema_projection import report_options_from_projected_params
from tools.cq.cli_app.types import ReportPreset
from tools.cq.core.result_factory import build_error_result


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
    from tools.cq.core.schema import ms
    from tools.cq.core.target_specs import parse_bundle_target_spec
    from tools.cq.orchestration.bundles import BundleContext, run_bundle

    ctx = require_context(ctx)
    options = report_options_from_projected_params(opts)

    # Parse target spec
    try:
        target_spec = parse_bundle_target_spec(options.target)
    except ValueError as exc:
        result = build_error_result(
            macro="report",
            root=ctx.root,
            argv=ctx.argv,
            tc=ctx.toolchain,
            started_ms=ms(),
            error=exc,
        )
        return CliResult(result=result, context=ctx, filters=options)

    bundle_ctx = BundleContext(
        tc=ctx.toolchain,
        services=ctx.services,
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
