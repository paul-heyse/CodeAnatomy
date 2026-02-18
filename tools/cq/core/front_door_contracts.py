"""Canonical front-door insight contracts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Literal

from tools.cq.core.semantic_contracts import SemanticStatus
from tools.cq.core.snb_schema import SemanticNodeRefV1
from tools.cq.core.structs import CqStruct
from tools.cq.core.summary_types import SummaryEnvelopeV1

if TYPE_CHECKING:
    from tools.cq.core.schema import Finding

InsightSource = Literal["search", "calls", "entity"]
Availability = Literal["full", "partial", "unavailable"]
NeighborhoodSource = Literal["structural", "semantic", "heuristic", "none"]
RiskLevel = Literal["low", "med", "high"]
type SummaryLike = SummaryEnvelopeV1 | Mapping[str, object]


class InsightThresholdPolicyV1(CqStruct, frozen=True):
    """Front-door threshold policy for risk/budget calculations."""

    default_top_candidates: int = 3
    default_preview_per_slice: int = 5
    high_caller_threshold: int = 10
    high_caller_strict_threshold: int = 10
    medium_caller_threshold: int = 4
    medium_caller_strict_threshold: int = 3
    arg_variance_threshold: int = 3
    files_with_calls_threshold: int = 3
    default_semantic_targets: int = 1


DEFAULT_INSIGHT_THRESHOLDS = InsightThresholdPolicyV1()


class InsightLocationV1(CqStruct, frozen=True):
    """Location payload for a selected target."""

    file: str = ""
    line: int | None = None
    col: int | None = None


class InsightTargetV1(CqStruct, frozen=True):
    """Primary target selected by a front-door command."""

    symbol: str
    kind: str = "unknown"
    location: InsightLocationV1 = InsightLocationV1()
    signature: str | None = None
    qualname: str | None = None
    selection_reason: str = ""


class InsightSliceV1(CqStruct, frozen=True):
    """Preview-able neighborhood slice with provenance and availability."""

    total: int = 0
    preview: tuple[SemanticNodeRefV1, ...] = ()
    availability: Availability = "unavailable"
    source: NeighborhoodSource = "none"
    overflow_artifact_ref: str | None = None


class InsightNeighborhoodV1(CqStruct, frozen=True):
    """Neighborhood envelope used by the front-door card."""

    callers: InsightSliceV1 = InsightSliceV1()
    callees: InsightSliceV1 = InsightSliceV1()
    references: InsightSliceV1 = InsightSliceV1()
    hierarchy_or_scope: InsightSliceV1 = InsightSliceV1()


class InsightRiskCountersV1(CqStruct, frozen=True):
    """Deterministic risk counters for edit-surface evaluation."""

    callers: int = 0
    callees: int = 0
    files_with_calls: int = 0
    arg_shape_count: int = 0
    forwarding_count: int = 0
    hazard_count: int = 0
    closure_capture_count: int = 0


class InsightRiskV1(CqStruct, frozen=True):
    """Risk level + explicit drivers and counters."""

    level: RiskLevel = "low"
    drivers: tuple[str, ...] = ()
    counters: InsightRiskCountersV1 = InsightRiskCountersV1()


class InsightConfidenceV1(CqStruct, frozen=True):
    """Confidence payload used by card headline and machine parsing."""

    evidence_kind: str = "unknown"
    score: float = 0.0
    bucket: str = "low"


class InsightDegradationV1(CqStruct, frozen=True):
    """Compact degradation status for front-door rendering."""

    semantic: SemanticStatus = "unavailable"
    scan: str = "ok"
    scope_filter: str = "none"
    notes: tuple[str, ...] = ()


class InsightBudgetV1(CqStruct, frozen=True):
    """Budget knobs used to keep front-door output bounded."""

    top_candidates: int = DEFAULT_INSIGHT_THRESHOLDS.default_top_candidates
    preview_per_slice: int = DEFAULT_INSIGHT_THRESHOLDS.default_preview_per_slice
    semantic_targets: int = DEFAULT_INSIGHT_THRESHOLDS.default_semantic_targets


class InsightArtifactRefsV1(CqStruct, frozen=True):
    """Artifact references for offloaded diagnostic/detail payloads."""

    diagnostics: str | None = None
    telemetry: str | None = None
    neighborhood_overflow: str | None = None


class FrontDoorInsightV1(CqStruct, frozen=True):
    """Canonical front-door insight schema for search/calls/entity."""

    source: InsightSource
    target: InsightTargetV1
    neighborhood: InsightNeighborhoodV1 = InsightNeighborhoodV1()
    risk: InsightRiskV1 = InsightRiskV1()
    confidence: InsightConfidenceV1 = InsightConfidenceV1()
    degradation: InsightDegradationV1 = InsightDegradationV1()
    budget: InsightBudgetV1 = InsightBudgetV1()
    artifact_refs: InsightArtifactRefsV1 = InsightArtifactRefsV1()
    schema_version: str = "cq.insight.v1"


class SearchInsightBuildRequestV1(CqStruct, frozen=True):
    """Typed request contract for search insight assembly."""

    summary: SummaryLike
    primary_target: Finding | None
    target_candidates: tuple[Finding, ...]
    neighborhood: InsightNeighborhoodV1 | None = None
    risk: InsightRiskV1 | None = None
    degradation: InsightDegradationV1 | None = None
    budget: InsightBudgetV1 | None = None


class CallsInsightBuildRequestV1(CqStruct, frozen=True):
    """Typed request contract for calls insight assembly."""

    function_name: str
    signature: str | None
    location: InsightLocationV1 | None
    neighborhood: InsightNeighborhoodV1
    files_with_calls: int
    arg_shape_count: int
    forwarding_count: int
    hazard_counts: dict[str, int]
    confidence: InsightConfidenceV1
    budget: InsightBudgetV1 | None = None
    degradation: InsightDegradationV1 | None = None


class EntityInsightBuildRequestV1(CqStruct, frozen=True):
    """Typed request contract for entity insight assembly."""

    summary: SummaryLike
    primary_target: Finding | None
    neighborhood: InsightNeighborhoodV1 | None = None
    risk: InsightRiskV1 | None = None
    confidence: InsightConfidenceV1 | None = None
    degradation: InsightDegradationV1 | None = None
    budget: InsightBudgetV1 | None = None


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
    "NeighborhoodSource",
    "RiskLevel",
    "SearchInsightBuildRequestV1",
    "SemanticStatus",
    "SummaryLike",
]
