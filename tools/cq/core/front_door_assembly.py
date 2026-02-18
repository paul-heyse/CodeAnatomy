"""Front-door orchestration boundary.

This module owns top-level front-door dispatch and artifact attachment policy.
Shared helper logic lives in ``front_door_support.py``.
"""

from __future__ import annotations

from collections.abc import Sequence

import msgspec

from tools.cq.core.front_door_contracts import (
    DEFAULT_INSIGHT_THRESHOLDS,
    Availability,
    CallsInsightBuildRequestV1,
    EntityInsightBuildRequestV1,
    FrontDoorInsightV1,
    InsightArtifactRefsV1,
    InsightBudgetV1,
    InsightConfidenceV1,
    InsightDegradationV1,
    InsightLocationV1,
    InsightNeighborhoodV1,
    InsightRiskCountersV1,
    InsightRiskV1,
    InsightSliceV1,
    InsightSource,
    InsightTargetV1,
    InsightThresholdPolicyV1,
    NeighborhoodSource,
    SearchInsightBuildRequestV1,
    SummaryLike,
)
from tools.cq.core.front_door_risk import risk_from_counters
from tools.cq.core.front_door_support import (
    build_neighborhood_from_slices,
    confidence_from_findings,
    default_calls_budget,
    default_entity_budget,
    default_search_budget,
    degradation_from_summary,
    empty_neighborhood,
    max_bucket,
    merge_slice,
    node_refs_from_semantic_entries,
    read_reference_total,
    read_total,
    string_or_none,
    target_from_finding,
)
from tools.cq.core.semantic_contracts import SemanticStatus
from tools.cq.core.snb_schema import NeighborhoodSliceV1, SemanticNodeRefV1


def augment_insight_with_semantic(
    insight: FrontDoorInsightV1,
    semantic_payload: dict[str, object],
    *,
    preview_per_slice: int | None = None,
) -> FrontDoorInsightV1:
    """Overlay static semantic data via extracted entity front-door module.

    Returns:
        FrontDoorInsightV1: Insight enriched with semantic overlays.
    """
    from tools.cq.core.front_door_entity import augment_insight_with_semantic as impl

    return impl(
        insight,
        semantic_payload,
        preview_per_slice=preview_per_slice,
    )


def build_search_insight(request: SearchInsightBuildRequestV1) -> FrontDoorInsightV1:
    """Build search front-door insight via extracted search module.

    Returns:
        FrontDoorInsightV1: Search insight payload.
    """
    from tools.cq.core.front_door_search import build_search_insight as impl

    return impl(request)


def build_calls_insight(request: CallsInsightBuildRequestV1) -> FrontDoorInsightV1:
    """Build calls front-door insight via extracted calls module.

    Returns:
        FrontDoorInsightV1: Calls insight payload.
    """
    from tools.cq.core.front_door_calls import build_calls_insight as impl

    return impl(request)


def build_entity_insight(request: EntityInsightBuildRequestV1) -> FrontDoorInsightV1:
    """Build entity front-door insight via extracted entity module.

    Returns:
        FrontDoorInsightV1: Entity insight payload.
    """
    from tools.cq.core.front_door_entity import build_entity_insight as impl

    return impl(request)


def attach_artifact_refs(
    insight: FrontDoorInsightV1,
    *,
    diagnostics: str | None = None,
    telemetry: str | None = None,
    neighborhood_overflow: str | None = None,
) -> FrontDoorInsightV1:
    """Attach artifact refs to an existing insight object.

    Returns:
        FrontDoorInsightV1: Insight with updated artifact references.
    """
    refs = msgspec.structs.replace(
        insight.artifact_refs,
        diagnostics=diagnostics or insight.artifact_refs.diagnostics,
        telemetry=telemetry or insight.artifact_refs.telemetry,
        neighborhood_overflow=neighborhood_overflow or insight.artifact_refs.neighborhood_overflow,
    )
    return msgspec.structs.replace(insight, artifact_refs=refs)


def attach_neighborhood_overflow_ref(
    insight: FrontDoorInsightV1,
    *,
    overflow_ref: str,
) -> FrontDoorInsightV1:
    """Attach overflow artifact ref to truncated neighborhood slices.

    Returns:
        FrontDoorInsightV1: Insight with overflow refs attached to truncated slices.
    """

    def _with_ref(slice_payload: InsightSliceV1) -> InsightSliceV1:
        if slice_payload.total <= len(slice_payload.preview):
            return slice_payload
        return msgspec.structs.replace(
            slice_payload,
            overflow_artifact_ref=overflow_ref,
        )

    neighborhood = msgspec.structs.replace(
        insight.neighborhood,
        callers=_with_ref(insight.neighborhood.callers),
        callees=_with_ref(insight.neighborhood.callees),
        references=_with_ref(insight.neighborhood.references),
        hierarchy_or_scope=_with_ref(insight.neighborhood.hierarchy_or_scope),
    )
    refs = msgspec.structs.replace(insight.artifact_refs, neighborhood_overflow=overflow_ref)
    return msgspec.structs.replace(insight, neighborhood=neighborhood, artifact_refs=refs)


def mark_partial_for_missing_languages(
    insight: FrontDoorInsightV1,
    *,
    missing_languages: Sequence[str],
) -> FrontDoorInsightV1:
    """Mark insight slices partial when language partitions are missing.

    Returns:
        FrontDoorInsightV1: Insight with partial scope/degradation annotations.
    """
    missing = tuple(sorted({lang.strip() for lang in missing_languages if lang.strip()}))
    if not missing:
        return insight

    def _downgrade(slice_payload: InsightSliceV1) -> InsightSliceV1:
        availability: Availability
        if slice_payload.availability in {"unavailable", "full"}:
            availability = "partial"
        else:
            availability = slice_payload.availability
        return msgspec.structs.replace(slice_payload, availability=availability)

    neighborhood = msgspec.structs.replace(
        insight.neighborhood,
        callers=_downgrade(insight.neighborhood.callers),
        callees=_downgrade(insight.neighborhood.callees),
        references=_downgrade(insight.neighborhood.references),
        hierarchy_or_scope=_downgrade(insight.neighborhood.hierarchy_or_scope),
    )
    note = f"missing_languages={','.join(missing)}"
    notes = tuple(dict.fromkeys([*insight.degradation.notes, note]))
    degradation = msgspec.structs.replace(
        insight.degradation,
        scope_filter="partial",
        notes=notes,
    )
    return msgspec.structs.replace(insight, neighborhood=neighborhood, degradation=degradation)


__all__ = [
    "DEFAULT_INSIGHT_THRESHOLDS",
    "Availability",
    "CallsInsightBuildRequestV1",
    "EntityInsightBuildRequestV1",
    "FrontDoorInsightV1",
    "InsightArtifactRefsV1",
    "InsightBudgetV1",
    "InsightConfidenceV1",
    "InsightDegradationV1",
    "InsightLocationV1",
    "InsightNeighborhoodV1",
    "InsightRiskCountersV1",
    "InsightRiskV1",
    "InsightSliceV1",
    "InsightSource",
    "InsightTargetV1",
    "InsightThresholdPolicyV1",
    "NeighborhoodSliceV1",
    "NeighborhoodSource",
    "SearchInsightBuildRequestV1",
    "SemanticNodeRefV1",
    "SemanticStatus",
    "SummaryLike",
    "attach_artifact_refs",
    "attach_neighborhood_overflow_ref",
    "augment_insight_with_semantic",
    "build_calls_insight",
    "build_entity_insight",
    "build_neighborhood_from_slices",
    "build_search_insight",
    "confidence_from_findings",
    "default_calls_budget",
    "default_entity_budget",
    "default_search_budget",
    "degradation_from_summary",
    "empty_neighborhood",
    "mark_partial_for_missing_languages",
    "max_bucket",
    "merge_slice",
    "node_refs_from_semantic_entries",
    "read_reference_total",
    "read_total",
    "risk_from_counters",
    "string_or_none",
    "target_from_finding",
]
