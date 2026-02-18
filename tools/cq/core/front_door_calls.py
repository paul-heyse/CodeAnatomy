"""Calls-focused front-door insight assembly."""

from __future__ import annotations

import msgspec

from tools.cq.core.front_door_contracts import (
    CallsInsightBuildRequestV1,
    FrontDoorInsightV1,
    InsightDegradationV1,
    InsightRiskCountersV1,
    InsightTargetV1,
)
from tools.cq.core.front_door_risk import risk_from_counters

__all__ = ["build_calls_insight"]


def build_calls_insight(request: CallsInsightBuildRequestV1) -> FrontDoorInsightV1:
    """Build calls front-door insight payload.

    Returns:
        FrontDoorInsightV1: Calls insight card populated from macro summary data.
    """
    from tools.cq.core import front_door_assembly as assembly

    target = InsightTargetV1(
        symbol=request.function_name,
        kind="function",
        location=request.location or assembly.InsightLocationV1(),
        signature=request.signature,
        selection_reason="resolved_calls_target",
    )
    counters = InsightRiskCountersV1(
        callers=request.neighborhood.callers.total,
        callees=request.neighborhood.callees.total,
        files_with_calls=request.files_with_calls,
        arg_shape_count=request.arg_shape_count,
        forwarding_count=request.forwarding_count,
        hazard_count=sum(request.hazard_counts.values()),
    )
    risk = risk_from_counters(counters)
    if request.hazard_counts:
        drivers = tuple(sorted(request.hazard_counts.keys()))
        risk = msgspec.structs.replace(
            risk,
            drivers=tuple(dict.fromkeys([*risk.drivers, *drivers])),
        )
    return FrontDoorInsightV1(
        source="calls",
        target=target,
        neighborhood=request.neighborhood,
        risk=risk,
        confidence=request.confidence,
        degradation=request.degradation or InsightDegradationV1(),
        budget=request.budget or assembly.default_calls_budget(),
    )
