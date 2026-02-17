"""Neighborhood command for cq CLI."""

from __future__ import annotations

from typing import Annotated

from cyclopts import App, Parameter

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.infrastructure import analysis_group, require_context
from tools.cq.cli_app.params import NeighborhoodParams
from tools.cq.cli_app.schema_projection import neighborhood_options_from_projected_params
from tools.cq.neighborhood.executor import NeighborhoodExecutionRequestV1, execute_neighborhood
from tools.cq.search._shared.enrichment_contracts import parse_incremental_enrichment_mode

neighborhood_app = App(
    name="neighborhood",
    help="Analyze semantic neighborhood of a target",
    group=analysis_group,
)


@neighborhood_app.default
def neighborhood(
    target: Annotated[str, Parameter(help="Target location (file:line[:col] or symbol)")],
    *,
    opts: Annotated[NeighborhoodParams, Parameter(name="*")] | None = None,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Analyze semantic neighborhood of a target symbol or location.

    Returns:
        CLI result containing rendered neighborhood findings.
    """
    ctx = require_context(ctx)
    params = neighborhood_options_from_projected_params(opts)
    result = execute_neighborhood(
        NeighborhoodExecutionRequestV1(
            target=target,
            root=ctx.root,
            argv=ctx.argv,
            toolchain=ctx.toolchain,
            lang=str(params.lang),
            top_k=params.top_k,
            semantic_enrichment=params.semantic_enrichment,
            incremental_enrichment_enabled=params.enrich,
            incremental_enrichment_mode=parse_incremental_enrichment_mode(params.enrich_mode),
            artifact_dir=ctx.artifact_dir,
            services=ctx.services,
        )
    )

    return CliResult(result=result, context=ctx, filters=None)


def get_app() -> App:
    """Return neighborhood sub-app for lazy registration."""
    return neighborhood_app


__all__ = ["get_app", "neighborhood_app"]
