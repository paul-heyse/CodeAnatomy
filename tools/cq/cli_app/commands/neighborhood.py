"""Neighborhood command for cq CLI."""

from __future__ import annotations

from typing import Annotated

from cyclopts import App, Parameter, validators

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.infrastructure import analysis_group, require_context
from tools.cq.cli_app.types import NeighborhoodLanguageToken
from tools.cq.core.cache.run_lifecycle import maybe_evict_run_cache_tag
from tools.cq.core.schema import assign_result_finding_ids, mk_runmeta, ms
from tools.cq.neighborhood.semantic_env import semantic_env_from_bundle
from tools.cq.utils.uuid_factory import uuid7_str

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
    from tools.cq.core.target_specs import parse_target_spec
    from tools.cq.neighborhood.bundle_builder import BundleBuildRequest, build_neighborhood_bundle
    from tools.cq.neighborhood.snb_renderer import RenderSnbRequest, render_snb_result
    from tools.cq.neighborhood.target_resolution import resolve_target

    ctx = require_context(ctx)
    started = ms()
    run_id = uuid7_str()
    resolved_lang = str(lang)

    spec = parse_target_spec(target)
    resolved = resolve_target(
        spec,
        root=ctx.root,
        language=resolved_lang,
        allow_symbol_fallback=True,
    )

    request = BundleBuildRequest(
        target_name=resolved.target_name,
        target_file=resolved.target_file,
        target_line=resolved.target_line,
        target_col=resolved.target_col,
        target_uri=resolved.target_uri,
        root=ctx.root,
        language=resolved_lang,
        symbol_hint=resolved.symbol_hint,
        top_k=top_k,
        enable_semantic_enrichment=semantic_enrichment,
        artifact_dir=ctx.artifact_dir,
        allow_symbol_fallback=True,
        target_degrade_events=resolved.degrade_events,
    )

    bundle = build_neighborhood_bundle(request)
    run = mk_runmeta(
        macro="neighborhood",
        argv=ctx.argv,
        root=str(ctx.root),
        started_ms=started,
        toolchain=ctx.toolchain.to_dict(),
        run_id=run_id,
    )

    result = render_snb_result(
        RenderSnbRequest(
            run=run,
            bundle=bundle,
            target=target,
            language=resolved_lang,
            top_k=top_k,
            enable_semantic_enrichment=semantic_enrichment,
            semantic_env=semantic_env_from_bundle(bundle),
        )
    )
    result.summary["target_resolution_kind"] = resolved.resolution_kind
    assign_result_finding_ids(result)
    maybe_evict_run_cache_tag(root=ctx.root, language=resolved_lang, run_id=run_id)

    return CliResult(result=result, context=ctx, filters=None)


def get_app() -> App:
    """Return neighborhood sub-app for lazy registration."""
    return neighborhood_app


__all__ = ["get_app", "neighborhood_app"]
