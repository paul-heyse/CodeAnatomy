"""Front-door insight schema contracts."""

from __future__ import annotations

from tools.cq.core.front_door_builders import (
    Availability,
    CallsInsightBuildRequestV1,
    EntityInsightBuildRequestV1,
    InsightSource,
    NeighborhoodSource,
    RiskLevel,
    SearchInsightBuildRequestV1,
)
from tools.cq.core.front_door_contracts import (
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
    InsightTargetV1,
)

__all__ = [
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
    "NeighborhoodSource",
    "RiskLevel",
    "SearchInsightBuildRequestV1",
]
