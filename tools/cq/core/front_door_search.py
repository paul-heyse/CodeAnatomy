"""Search-focused front-door insight assembly."""

from __future__ import annotations

import msgspec

from tools.cq.core.front_door_contracts import (
    FrontDoorInsightV1,
    InsightNeighborhoodV1,
    InsightRiskCountersV1,
    SearchInsightBuildRequestV1,
)
from tools.cq.core.front_door_risk import risk_from_counters
from tools.cq.core.snb_schema import NeighborhoodSliceV1

__all__ = ["build_neighborhood_from_slices", "build_search_insight"]


def build_neighborhood_from_slices(
    slices: tuple[NeighborhoodSliceV1, ...] | list[NeighborhoodSliceV1],
    *,
    preview_per_slice: int,
    source: str,
    overflow_artifact_ref: str | None = None,
) -> InsightNeighborhoodV1:
    """Delegate neighborhood assembly to the canonical assembly module.

    Returns:
        InsightNeighborhoodV1: Neighborhood slices normalized for front-door rendering.
    """
    from tools.cq.core import front_door_assembly as assembly

    return assembly.build_neighborhood_from_slices(
        slices,
        preview_per_slice=preview_per_slice,
        source=source,  # type: ignore[arg-type]
        overflow_artifact_ref=overflow_artifact_ref,
    )


def build_search_insight(request: SearchInsightBuildRequestV1) -> FrontDoorInsightV1:
    """Build search front-door insight payload.

    Returns:
        FrontDoorInsightV1: Search insight card derived from findings and summary.
    """
    from tools.cq.core import front_door_assembly as assembly

    target = assembly.target_from_finding(
        request.primary_target,
        fallback_symbol=assembly.string_or_none(assembly.summary_value(request.summary, "query"))
        or "search target",
        fallback_kind="query",
        selection_reason=(
            "top_definition" if request.primary_target is not None else "fallback_query"
        ),
    )
    confidence = assembly.confidence_from_findings(request.target_candidates)
    confidence = msgspec.structs.replace(
        confidence,
        evidence_kind=confidence.evidence_kind
        if confidence.evidence_kind != "unknown"
        else assembly.string_or_none(assembly.summary_value(request.summary, "scan_method"))
        or "resolved_ast",
    )
    neighborhood = request.neighborhood or assembly.empty_neighborhood()
    risk = request.risk
    if risk is None:
        risk = risk_from_counters(
            InsightRiskCountersV1(
                callers=neighborhood.callers.total,
                callees=neighborhood.callees.total,
            )
        )
    degradation = request.degradation or assembly.degradation_from_summary(request.summary)
    budget = request.budget or assembly.default_search_budget(
        target_candidate_count=len(request.target_candidates)
    )
    return FrontDoorInsightV1(
        source="search",
        target=target,
        neighborhood=neighborhood,
        risk=risk,
        confidence=confidence,
        degradation=degradation,
        budget=budget,
    )
