"""Artifact persistence helpers for CQ CLI results."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

import msgspec

from tools.cq.core.artifacts import (
    save_artifact_json,
    save_diagnostics_artifact,
    save_neighborhood_overflow_artifact,
    save_search_artifact_bundle_cache,
)
from tools.cq.core.cache.contracts import SearchArtifactBundleV1

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult
    from tools.cq.core.summary_types import SummaryEnvelopeV1


def _attach_insight_artifact_refs(
    result: CqResult,
    *,
    diagnostics_ref: str | None = None,
    telemetry_ref: str | None = None,
    neighborhood_overflow_ref: str | None = None,
) -> CqResult:
    from tools.cq.core.front_door_dispatch import (
        attach_artifact_refs,
        attach_neighborhood_overflow_ref,
    )
    from tools.cq.core.front_door_render import (
        coerce_front_door_insight,
        to_public_front_door_insight_dict,
    )

    insight = coerce_front_door_insight(result.summary.front_door_insight)
    if insight is None:
        return result
    updated = attach_artifact_refs(
        insight,
        diagnostics=diagnostics_ref,
        telemetry=telemetry_ref,
        neighborhood_overflow=neighborhood_overflow_ref,
    )
    if neighborhood_overflow_ref:
        updated = attach_neighborhood_overflow_ref(updated, overflow_ref=neighborhood_overflow_ref)
    return msgspec.structs.replace(
        result,
        summary=msgspec.structs.replace(
            result.summary,
            front_door_insight=to_public_front_door_insight_dict(updated),
        ),
    )


def persist_result_artifacts(
    *,
    result: CqResult,
    artifact_dir: str | None,
    no_save: bool,
    pop_search_object_view_for_run: Callable[[str], object | None],
) -> CqResult:
    """Persist result artifacts and annotate insight refs.

    Returns:
        CqResult: Result payload with persisted artifact references attached.
    """
    if no_save:
        run_id = result.run.run_id
        if result.run.macro == "search" and run_id is not None:
            _ = pop_search_object_view_for_run(run_id)
        return result
    if result.run.macro == "search":
        return _save_search_artifacts(
            result,
            pop_search_object_view_for_run=pop_search_object_view_for_run,
        )
    return _save_general_artifacts(result, artifact_dir)


def _save_search_artifacts(
    result: CqResult,
    *,
    pop_search_object_view_for_run: Callable[[str], object | None],
) -> CqResult:
    run_id = result.run.run_id
    if run_id is None:
        return result
    object_view = pop_search_object_view_for_run(run_id)
    if object_view is None:
        return result
    search_artifact = save_search_artifact_bundle_cache(
        result,
        _build_search_artifact_bundle(result, object_view),
    )
    if search_artifact is None:
        return result
    result = msgspec.structs.replace(result, artifacts=(*result.artifacts, search_artifact))
    return _attach_insight_artifact_refs(
        result,
        diagnostics_ref=search_artifact.path,
        telemetry_ref=search_artifact.path,
        neighborhood_overflow_ref=None,
    )


def _save_general_artifacts(result: CqResult, artifact_dir: str | None) -> CqResult:
    artifact = save_artifact_json(result, artifact_dir)
    artifacts = [*result.artifacts, artifact]
    diagnostics_artifact = save_diagnostics_artifact(result, artifact_dir)
    if diagnostics_artifact is not None:
        artifacts.append(diagnostics_artifact)
    overflow_artifact = save_neighborhood_overflow_artifact(result, artifact_dir)
    if overflow_artifact is not None:
        artifacts.append(overflow_artifact)
    result = msgspec.structs.replace(result, artifacts=tuple(artifacts))
    return _attach_insight_artifact_refs(
        result,
        diagnostics_ref=diagnostics_artifact.path if diagnostics_artifact is not None else None,
        telemetry_ref=diagnostics_artifact.path if diagnostics_artifact is not None else None,
        neighborhood_overflow_ref=overflow_artifact.path if overflow_artifact is not None else None,
    )


def _build_search_artifact_bundle(
    result: CqResult,
    object_view: object,
) -> SearchArtifactBundleV1:
    import msgspec

    from tools.cq.search.objects.render import SearchObjectResolvedViewV1

    resolved_view = msgspec.convert(object_view, type=SearchObjectResolvedViewV1)
    return SearchArtifactBundleV1(
        run_id=result.run.run_id or "no_run_id",
        query=_search_query(result.summary),
        macro=result.run.macro,
        summary=_search_artifact_summary(result.summary),
        object_summaries=[msgspec.to_builtins(item) for item in resolved_view.summaries],
        occurrences=[msgspec.to_builtins(item) for item in resolved_view.occurrences],
        diagnostics=_search_artifact_diagnostics(result.summary),
        snippets=dict(resolved_view.snippets),
        created_ms=(
            result.run.run_created_ms
            if isinstance(result.run.run_created_ms, int | float)
            else result.run.started_ms
        ),
    )


def _search_query(summary: SummaryEnvelopeV1) -> str:
    raw = summary.query
    if isinstance(raw, str) and raw:
        return raw
    return "<unknown>"


def _search_artifact_summary(summary: SummaryEnvelopeV1) -> dict[str, object]:
    keys = (
        "query",
        "mode",
        "lang_scope",
        "returned_matches",
        "total_matches",
        "matched_files",
        "scanned_files",
        "resolved_objects",
        "resolved_occurrences",
    )
    payload: dict[str, object] = {}
    for key in keys:
        value = getattr(summary, key)
        if value is not None:
            payload[key] = value
    return payload


def _search_artifact_diagnostics(summary: SummaryEnvelopeV1) -> dict[str, object]:
    keys = (
        "enrichment_telemetry",
        "python_semantic_overview",
        "python_semantic_telemetry",
        "python_semantic_diagnostics",
        "rust_semantic_telemetry",
        "cross_language_diagnostics",
        "semantic_planes",
        "language_capabilities",
        "dropped_by_scope",
    )
    payload: dict[str, object] = {}
    for key in keys:
        value = getattr(summary, key)
        if value is not None:
            payload[key] = value
    return payload


__all__ = ["persist_result_artifacts"]
