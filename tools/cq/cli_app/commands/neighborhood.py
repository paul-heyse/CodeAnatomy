"""Neighborhood command for cq CLI."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from cyclopts import App, Parameter, validators

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.infrastructure import analysis_group, require_context
from tools.cq.cli_app.types import NeighborhoodLanguageToken
from tools.cq.neighborhood.executor import NeighborhoodExecutionRequest, execute_neighborhood
from tools.cq.search.pipeline.enrichment_contracts import parse_incremental_enrichment_mode

neighborhood_app = App(
    name="neighborhood",
    help="Analyze semantic neighborhood of a target",
    group=analysis_group,
)


@dataclass(kw_only=True)
class NeighborhoodParams:
    """Options for the neighborhood command."""

    lang: Annotated[
        NeighborhoodLanguageToken,
        Parameter(name="--lang", help="Query language (python, rust)"),
    ] = NeighborhoodLanguageToken.python
    top_k: Annotated[
        int,
        Parameter(
            name="--top-k",
            help="Max items per slice",
            validator=validators.Number(gte=1, lte=10_000),
        ),
    ] = 10
    semantic_enrichment: Annotated[
        bool,
        Parameter(
            name="--semantic-enrichment",
            negative="--no-semantic-enrichment",
            negative_bool=(),
            help="Enable semantic enrichment",
        ),
    ] = True
    enrich: Annotated[
        bool,
        Parameter(
            name="--enrich",
            negative="--no-enrich",
            negative_bool=(),
            help="Enable incremental enrichment for neighborhood target analysis",
        ),
    ] = True
    enrich_mode: Annotated[
        str,
        Parameter(
            name="--enrich-mode",
            help="Incremental enrichment mode (ts_only, ts_sym, ts_sym_dis, full)",
        ),
    ] = "ts_sym"


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
    params = opts if opts is not None else NeighborhoodParams()
    result = execute_neighborhood(
        NeighborhoodExecutionRequest(
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
