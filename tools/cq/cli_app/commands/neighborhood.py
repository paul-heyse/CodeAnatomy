# ruff: noqa: DOC201,DOC501
"""Neighborhood command for cq CLI."""

from __future__ import annotations

from typing import Annotated

from cyclopts import App, Parameter

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.core.schema import mk_runmeta, ms
from tools.cq.core.snb_schema import SemanticNeighborhoodBundleV1

neighborhood_app = App(name="neighborhood", help="Analyze semantic neighborhood of a target")
nb_app = App(name="nb", help="Analyze semantic neighborhood of a target (alias)")


@neighborhood_app.default
@nb_app.default
def neighborhood(
    target: Annotated[str, Parameter(help="Target location (file:line[:col] or symbol)")],
    *,
    lang: Annotated[str, Parameter(name="--lang", help="Query language")] = "python",
    top_k: Annotated[int, Parameter(name="--top-k", help="Max items per slice")] = 10,
    no_lsp: Annotated[bool, Parameter(name="--no-lsp", help="Disable LSP enrichment")] = False,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Analyze semantic neighborhood of a target symbol or location."""
    from tools.cq.neighborhood.bundle_builder import BundleBuildRequest, build_neighborhood_bundle
    from tools.cq.neighborhood.scan_snapshot import ScanSnapshot
    from tools.cq.neighborhood.snb_renderer import render_snb_result
    from tools.cq.neighborhood.target_resolution import parse_target_spec, resolve_target
    from tools.cq.query.sg_parser import sg_scan

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    started = ms()

    records = sg_scan(
        paths=[ctx.root],
        lang=lang if lang in {"python", "rust"} else "python",  # type: ignore[arg-type]
        root=ctx.root,
    )
    snapshot = ScanSnapshot.from_records(records)

    spec = parse_target_spec(target)
    resolved = resolve_target(
        spec,
        snapshot,
        root=ctx.root,
        allow_symbol_fallback=True,
    )

    request = BundleBuildRequest(
        target_name=resolved.target_name,
        target_file=resolved.target_file,
        target_line=resolved.target_line,
        target_col=resolved.target_col,
        target_uri=resolved.target_uri,
        root=ctx.root,
        snapshot=snapshot,
        language=lang,
        symbol_hint=resolved.symbol_hint,
        top_k=top_k,
        enable_lsp=not no_lsp,
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
    )

    result = render_snb_result(
        run=run,
        bundle=bundle,
        target=target,
        language=lang,
        top_k=top_k,
        enable_lsp=not no_lsp,
        lsp_env=_lsp_env_from_bundle(bundle),
    )
    result.summary["target_resolution_kind"] = resolved.resolution_kind

    return CliResult(result=result, context=ctx, filters=None)


def _lsp_env_from_bundle(bundle: SemanticNeighborhoodBundleV1) -> dict[str, object]:
    if bundle.meta is None or not bundle.meta.lsp_servers:
        return {}
    first = bundle.meta.lsp_servers[0]
    env: dict[str, object] = {}
    for in_key, out_key in (
        ("workspace_health", "lsp_health"),
        ("quiescent", "lsp_quiescent"),
        ("position_encoding", "lsp_position_encoding"),
    ):
        value = first.get(in_key)
        if value is not None:
            env[out_key] = value
    return env
