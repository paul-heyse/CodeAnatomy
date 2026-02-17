"""Shared neighborhood execution orchestration."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

from tools.cq.core.cache.run_lifecycle import maybe_evict_run_cache_tag
from tools.cq.core.schema import CqResult, assign_result_finding_ids, mk_runmeta, ms
from tools.cq.core.structs import CqStruct
from tools.cq.core.summary_contract import NeighborhoodSummaryV1, summary_for_variant
from tools.cq.core.target_specs import parse_target_spec
from tools.cq.core.types import QueryLanguage
from tools.cq.neighborhood.bundle_builder import BundleBuildRequest, build_neighborhood_bundle
from tools.cq.neighborhood.semantic_env import semantic_env_from_bundle
from tools.cq.neighborhood.snb_renderer import RenderSnbRequest, render_snb_result
from tools.cq.neighborhood.target_resolution import resolve_target
from tools.cq.utils.uuid_factory import uuid7_str

if TYPE_CHECKING:
    from tools.cq.core.bootstrap import CqRuntimeServices
    from tools.cq.core.toolchain import Toolchain


class NeighborhoodExecutionRequest(CqStruct, frozen=True):
    """Input contract for shared neighborhood execution."""

    target: str
    root: Path
    argv: list[str]
    toolchain: Toolchain
    lang: str | None = None
    top_k: int = 10
    semantic_enrichment: bool = True
    artifact_dir: Path | None = None
    run_id: str | None = None
    services: CqRuntimeServices | None = None


def _coerce_neighborhood_summary(result: CqResult) -> None:
    summary = result.summary
    if isinstance(summary, NeighborhoodSummaryV1):
        return
    coerced = summary_for_variant("neighborhood")
    for field in coerced.__struct_fields__:
        if field in summary.__struct_fields__:
            setattr(coerced, field, getattr(summary, field))
    result.summary = coerced


def execute_neighborhood(request: NeighborhoodExecutionRequest) -> CqResult:
    """Execute the neighborhood workflow for CLI and run-step callsites.

    Returns:
        CqResult: Rendered neighborhood result.
    """
    started = ms()
    active_run_id = request.run_id or uuid7_str()
    resolved_lang: QueryLanguage = (
        cast("QueryLanguage", request.lang) if request.lang in {"python", "rust"} else "python"
    )

    spec = parse_target_spec(request.target)
    resolved = resolve_target(
        spec,
        root=request.root,
        language=resolved_lang,
        allow_symbol_fallback=True,
    )

    bundle = build_neighborhood_bundle(
        BundleBuildRequest(
            target_name=resolved.target_name,
            target_file=resolved.target_file,
            target_line=resolved.target_line,
            target_col=resolved.target_col,
            target_uri=resolved.target_uri,
            root=request.root,
            language=resolved_lang,
            symbol_hint=resolved.symbol_hint,
            top_k=request.top_k,
            enable_semantic_enrichment=request.semantic_enrichment,
            artifact_dir=request.artifact_dir,
            allow_symbol_fallback=True,
            target_degrade_events=resolved.degrade_events,
        )
    )

    run = mk_runmeta(
        macro="neighborhood",
        argv=request.argv,
        root=str(request.root),
        started_ms=started,
        toolchain=request.toolchain.to_dict(),
        run_id=active_run_id,
    )

    result = render_snb_result(
        RenderSnbRequest(
            run=run,
            bundle=bundle,
            target=request.target,
            language=resolved_lang,
            top_k=request.top_k,
            enable_semantic_enrichment=request.semantic_enrichment,
            semantic_env=semantic_env_from_bundle(bundle),
        )
    )
    _coerce_neighborhood_summary(result)
    neighborhood_summary = cast("NeighborhoodSummaryV1", result.summary)
    neighborhood_summary.target_resolution_kind = resolved.resolution_kind
    result = assign_result_finding_ids(result)
    maybe_evict_run_cache_tag(root=request.root, language=resolved_lang, run_id=active_run_id)
    return result


__all__ = ["NeighborhoodExecutionRequest", "execute_neighborhood"]
