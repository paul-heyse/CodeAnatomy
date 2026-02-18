"""Entity-focused front-door insight assembly."""

from __future__ import annotations

import msgspec

from tools.cq.core.front_door_contracts import (
    EntityInsightBuildRequestV1,
    FrontDoorInsightV1,
    InsightConfidenceV1,
    InsightDegradationV1,
    InsightRiskCountersV1,
)
from tools.cq.core.front_door_risk import risk_from_counters
from tools.cq.core.front_door_support import (
    default_entity_budget,
    empty_neighborhood,
    max_bucket,
    merge_slice,
    node_refs_from_semantic_entries,
    read_reference_total,
    read_total,
    string_or_none,
    target_from_finding,
)
from tools.cq.core.render_utils import summary_value

__all__ = ["augment_insight_with_semantic", "build_entity_insight"]


def augment_insight_with_semantic(
    insight: FrontDoorInsightV1,
    semantic_payload: dict[str, object],
    *,
    preview_per_slice: int | None = None,
) -> FrontDoorInsightV1:
    """Overlay static semantic data on top of an existing insight payload.

    Returns:
        FrontDoorInsightV1: Insight enriched with semantic neighborhood/type data.
    """
    limit = preview_per_slice or insight.budget.preview_per_slice
    neighborhood = insight.neighborhood

    call_graph = semantic_payload.get("call_graph")
    if isinstance(call_graph, dict):
        callers_preview = node_refs_from_semantic_entries(
            call_graph.get("incoming_callers"),
            limit,
        )
        callees_preview = node_refs_from_semantic_entries(
            call_graph.get("outgoing_callees"),
            limit,
        )
        callers_total = read_total(
            call_graph.get("incoming_total"),
            fallback=len(callers_preview),
        )
        callees_total = read_total(
            call_graph.get("outgoing_total"),
            fallback=len(callees_preview),
        )
        neighborhood = msgspec.structs.replace(
            neighborhood,
            callers=merge_slice(
                neighborhood.callers,
                total=callers_total,
                preview=callers_preview,
                source="semantic",
            ),
            callees=merge_slice(
                neighborhood.callees,
                total=callees_total,
                preview=callees_preview,
                source="semantic",
            ),
        )

    references_total = read_reference_total(semantic_payload)
    if references_total is not None:
        neighborhood = msgspec.structs.replace(
            neighborhood,
            references=merge_slice(
                neighborhood.references,
                total=references_total,
                preview=neighborhood.references.preview,
                source="semantic",
            ),
        )

    target = insight.target
    type_contract = semantic_payload.get("type_contract")
    if isinstance(type_contract, dict):
        signature = string_or_none(type_contract.get("callable_signature"))
        resolved_type = string_or_none(type_contract.get("resolved_type"))
        target = msgspec.structs.replace(
            target,
            signature=signature or resolved_type or target.signature,
        )

    confidence = msgspec.structs.replace(
        insight.confidence,
        evidence_kind=insight.confidence.evidence_kind
        if insight.confidence.evidence_kind != "unknown"
        else "resolved_static_semantic",
        score=max(insight.confidence.score, 0.8),
        bucket=max_bucket(insight.confidence.bucket, "high"),
    )
    degradation = msgspec.structs.replace(insight.degradation, semantic="ok")
    return msgspec.structs.replace(
        insight,
        target=target,
        neighborhood=neighborhood,
        confidence=confidence,
        degradation=degradation,
    )


def build_entity_insight(request: EntityInsightBuildRequestV1) -> FrontDoorInsightV1:
    """Build entity front-door insight payload.

    Returns:
        FrontDoorInsightV1: Entity insight card based on query results and summary.
    """
    target = target_from_finding(
        request.primary_target,
        fallback_symbol=string_or_none(summary_value(request.summary, "query"))
        or string_or_none(summary_value(request.summary, "entity_kind"))
        or "entity target",
        fallback_kind=string_or_none(summary_value(request.summary, "entity_kind")) or "entity",
        selection_reason=(
            "top_entity_result" if request.primary_target is not None else "fallback_query"
        ),
    )
    neighborhood = request.neighborhood or empty_neighborhood()
    risk = request.risk
    if risk is None:
        risk = risk_from_counters(
            InsightRiskCountersV1(
                callers=neighborhood.callers.total,
                callees=neighborhood.callees.total,
            )
        )
    confidence = request.confidence
    if confidence is None:
        confidence = InsightConfidenceV1(evidence_kind="resolved_ast", score=0.8, bucket="high")
    return FrontDoorInsightV1(
        source="entity",
        target=target,
        neighborhood=neighborhood,
        risk=risk,
        confidence=confidence,
        degradation=request.degradation or InsightDegradationV1(),
        budget=request.budget or default_entity_budget(),
    )
