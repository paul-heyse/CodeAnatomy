"""Neighborhood command for cq CLI."""

from __future__ import annotations

from typing import Annotated

from cyclopts import App, Parameter, validators

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.infrastructure import analysis_group, require_context
from tools.cq.cli_app.types import NeighborhoodLanguageToken
from tools.cq.neighborhood.executor import NeighborhoodExecutionRequest, execute_neighborhood

neighborhood_app = App(
    name="neighborhood",
    help="Analyze semantic neighborhood of a target",
    group=analysis_group,
)


@neighborhood_app.default
def neighborhood(
    target: Annotated[str, Parameter(help="Target location (file:line[:col] or symbol)")],
    *,
    lang: Annotated[
        NeighborhoodLanguageToken,
        Parameter(name="--lang", help="Query language (python, rust)"),
    ] = NeighborhoodLanguageToken.python,
    top_k: Annotated[
        int,
        Parameter(
            name="--top-k",
            help="Max items per slice",
            validator=validators.Number(gte=1, lte=10_000),
        ),
    ] = 10,
    semantic_enrichment: Annotated[
        bool,
        Parameter(
            name="--semantic-enrichment",
            negative="--no-semantic-enrichment",
            negative_bool=(),
            help="Enable semantic enrichment",
        ),
    ] = True,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Analyze semantic neighborhood of a target symbol or location.

    Returns:
        CLI result containing rendered neighborhood findings.
    """
    ctx = require_context(ctx)
    result = execute_neighborhood(
        NeighborhoodExecutionRequest(
            target=target,
            root=ctx.root,
            argv=ctx.argv,
            toolchain=ctx.toolchain,
            lang=str(lang),
            top_k=top_k,
            semantic_enrichment=semantic_enrichment,
            artifact_dir=ctx.artifact_dir,
            services=ctx.services,
        )
    )

    return CliResult(result=result, context=ctx, filters=None)


def get_app() -> App:
    """Return neighborhood sub-app for lazy registration."""
    return neighborhood_app


__all__ = ["get_app", "neighborhood_app"]
