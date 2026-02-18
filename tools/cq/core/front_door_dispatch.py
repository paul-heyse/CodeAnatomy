"""Canonical front-door dispatch surface.

Owns public dispatch entry points and delegates to assembly internals.
"""

from __future__ import annotations

from collections.abc import Sequence

from tools.cq.core.front_door_assembly import (
    attach_artifact_refs as _attach_artifact_refs,
)
from tools.cq.core.front_door_assembly import (
    attach_neighborhood_overflow_ref as _attach_neighborhood_overflow_ref,
)
from tools.cq.core.front_door_assembly import (
    augment_insight_with_semantic as _augment_insight_with_semantic,
)
from tools.cq.core.front_door_assembly import (
    build_calls_insight as _build_calls_insight,
)
from tools.cq.core.front_door_assembly import (
    build_entity_insight as _build_entity_insight,
)
from tools.cq.core.front_door_assembly import (
    build_neighborhood_from_slices as _build_neighborhood_from_slices,
)
from tools.cq.core.front_door_assembly import (
    build_search_insight as _build_search_insight,
)
from tools.cq.core.front_door_assembly import (
    mark_partial_for_missing_languages as _mark_partial_for_missing_languages,
)
from tools.cq.core.front_door_contracts import DEFAULT_INSIGHT_THRESHOLDS, NeighborhoodSource
from tools.cq.core.front_door_schema import (
    CallsInsightBuildRequestV1,
    EntityInsightBuildRequestV1,
    FrontDoorInsightV1,
    InsightNeighborhoodV1,
    SearchInsightBuildRequestV1,
)
from tools.cq.core.snb_schema import NeighborhoodSliceV1

__all__ = [
    "attach_artifact_refs",
    "attach_neighborhood_overflow_ref",
    "augment_insight_with_semantic",
    "build_calls_insight",
    "build_entity_insight",
    "build_neighborhood_from_slices",
    "build_search_insight",
    "mark_partial_for_missing_languages",
]


def build_neighborhood_from_slices(
    slices: Sequence[NeighborhoodSliceV1],
    *,
    preview_per_slice: int = DEFAULT_INSIGHT_THRESHOLDS.default_preview_per_slice,
    source: NeighborhoodSource = "structural",
    overflow_artifact_ref: str | None = None,
) -> InsightNeighborhoodV1:
    """Delegate neighborhood construction to assembly internals.

    Returns:
        Canonical neighborhood slice aggregate payload.
    """
    return _build_neighborhood_from_slices(
        slices,
        preview_per_slice=preview_per_slice,
        source=source,
        overflow_artifact_ref=overflow_artifact_ref,
    )


def build_search_insight(request: SearchInsightBuildRequestV1) -> FrontDoorInsightV1:
    """Delegate search insight assembly.

    Returns:
        Search front-door insight payload.
    """
    return _build_search_insight(request)


def build_calls_insight(request: CallsInsightBuildRequestV1) -> FrontDoorInsightV1:
    """Delegate calls insight assembly.

    Returns:
        Calls front-door insight payload.
    """
    return _build_calls_insight(request)


def build_entity_insight(request: EntityInsightBuildRequestV1) -> FrontDoorInsightV1:
    """Delegate entity insight assembly.

    Returns:
        Entity front-door insight payload.
    """
    return _build_entity_insight(request)


def augment_insight_with_semantic(
    insight: FrontDoorInsightV1,
    semantic_payload: dict[str, object],
    *,
    preview_per_slice: int | None = None,
) -> FrontDoorInsightV1:
    """Delegate semantic augmentation for front-door insights.

    Returns:
        Insight payload with semantic overlays applied.
    """
    return _augment_insight_with_semantic(
        insight,
        semantic_payload,
        preview_per_slice=preview_per_slice,
    )


def attach_artifact_refs(
    insight: FrontDoorInsightV1,
    *,
    diagnostics: str | None = None,
    telemetry: str | None = None,
    neighborhood_overflow: str | None = None,
) -> FrontDoorInsightV1:
    """Attach artifact refs to insight using canonical assembly behavior.

    Returns:
        Insight payload with updated artifact references.
    """
    return _attach_artifact_refs(
        insight,
        diagnostics=diagnostics,
        telemetry=telemetry,
        neighborhood_overflow=neighborhood_overflow,
    )


def attach_neighborhood_overflow_ref(
    insight: FrontDoorInsightV1,
    *,
    overflow_ref: str,
) -> FrontDoorInsightV1:
    """Attach neighborhood overflow reference when available.

    Returns:
        Insight payload with neighborhood overflow artifact reference attached.
    """
    return _attach_neighborhood_overflow_ref(insight, overflow_ref=overflow_ref)


def mark_partial_for_missing_languages(
    insight: FrontDoorInsightV1,
    *,
    missing_languages: Sequence[str],
) -> FrontDoorInsightV1:
    """Mark front-door insight partial when language coverage is incomplete.

    Returns:
        Insight payload marked partial for missing language coverage.
    """
    return _mark_partial_for_missing_languages(insight, missing_languages=missing_languages)
