"""Front-door insight rendering and serialization helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from tools.cq.core.front_door_contracts import (
    FrontDoorInsightV1,
    InsightArtifactRefsV1,
    InsightBudgetV1,
    InsightConfidenceV1,
    InsightDegradationV1,
    InsightLocationV1,
    InsightNeighborhoodV1,
    InsightRiskV1,
    InsightTargetV1,
)
from tools.cq.core.snb_schema import SemanticNodeRefV1
from tools.cq.core.typed_boundary import BoundaryDecodeError, convert_lax


def render_insight_card(insight: FrontDoorInsightV1) -> list[str]:
    """Render a compact markdown card from a front-door insight.

    Returns:
        Markdown lines for a concise insight card.
    """
    lines = ["## Insight Card", _render_target_line(insight.target)]
    lines.extend(_render_neighborhood_lines(insight.neighborhood))
    lines.append(_render_risk_line(insight.risk))
    lines.append("")
    return lines


def _render_target_line(target: InsightTargetV1) -> str:
    location = _format_target_location(target.location)
    target_parts = [f"**{target.symbol}**", f"({target.kind})", location]
    if target.signature:
        target_parts.append(f"`{target.signature}`")
    return f"- Target: {' '.join(part for part in target_parts if part)}"


def _format_target_location(location: InsightLocationV1) -> str:
    if not location.file:
        return ""
    if location.line is not None:
        return f"`{location.file}:{location.line}`"
    return f"`{location.file}`"


def _render_neighborhood_lines(neighborhood: InsightNeighborhoodV1) -> list[str]:
    lines = [
        (
            "- Neighborhood: "
            f"callers={neighborhood.callers.total}, "
            f"callees={neighborhood.callees.total}, "
            f"references={neighborhood.references.total}, "
            f"scope={neighborhood.hierarchy_or_scope.total}"
        )
    ]
    callers_preview = _preview_labels(neighborhood.callers.preview)
    if callers_preview:
        lines.append(f"  - Top callers: {', '.join(callers_preview)}")
    callees_preview = _preview_labels(neighborhood.callees.preview)
    if callees_preview:
        lines.append(f"  - Top callees: {', '.join(callees_preview)}")
    return lines


def _render_risk_line(risk: InsightRiskV1) -> str:
    driver_text = ", ".join(risk.drivers) if risk.drivers else "none"
    counters = risk.counters
    parts = [f"level={risk.level}", f"drivers={driver_text}"]
    if counters.hazard_count:
        parts.append(f"hazards={counters.hazard_count}")
    if counters.forwarding_count:
        parts.append(f"forwarding={counters.forwarding_count}")
    return f"- Risk: {'; '.join(parts)}"


def _render_confidence_line(confidence: InsightConfidenceV1) -> str:
    return (
        "- Confidence: "
        f"evidence={confidence.evidence_kind}, "
        f"score={confidence.score:.2f}, "
        f"bucket={confidence.bucket}"
    )


def _render_degradation_line(degradation: InsightDegradationV1) -> str:
    notes = f" ({'; '.join(degradation.notes)})" if degradation.notes else ""
    return (
        "- Degradation: "
        f"semantic={degradation.semantic}, "
        f"scan={degradation.scan}, "
        f"scope_filter={degradation.scope_filter}{notes}"
    )


def _render_budget_line(budget: InsightBudgetV1) -> str:
    return (
        "- Budget: "
        f"top_candidates={budget.top_candidates}, "
        f"preview_per_slice={budget.preview_per_slice}, "
        f"semantic_targets={budget.semantic_targets}"
    )


def _render_artifact_refs_line(artifact_refs: InsightArtifactRefsV1) -> str | None:
    ref_parts = [
        f"diagnostics={artifact_refs.diagnostics}" if artifact_refs.diagnostics else None,
        f"telemetry={artifact_refs.telemetry}" if artifact_refs.telemetry else None,
        f"neighborhood_overflow={artifact_refs.neighborhood_overflow}"
        if artifact_refs.neighborhood_overflow
        else None,
    ]
    refs = [part for part in ref_parts if part is not None]
    if not refs:
        return None
    return f"- Artifact Refs: {' | '.join(refs)}"


def _preview_labels(nodes: Iterable[SemanticNodeRefV1]) -> list[str]:
    labels: list[str] = [node.display_label or node.name for node in nodes]
    return labels[:3]


def coerce_front_door_insight(payload: object) -> FrontDoorInsightV1 | None:
    """Best-effort conversion from summary payload to insight struct.

    Returns:
        Parsed insight struct when conversion succeeds; otherwise ``None``.
    """
    if isinstance(payload, FrontDoorInsightV1):
        return payload
    if not isinstance(payload, Mapping):
        return None
    raw_payload = dict(payload)
    if not raw_payload:
        return None
    if "source" not in raw_payload and "target" not in raw_payload:
        return None
    try:
        return convert_lax(raw_payload, type_=FrontDoorInsightV1)
    except BoundaryDecodeError:
        return None


def to_public_front_door_insight_dict(insight: FrontDoorInsightV1) -> dict[str, object]:
    """Serialize insight contract to deterministic builtins mapping.

    Returns:
        JSON-safe mapping form of the insight payload.
    """
    from tools.cq.core.front_door_serialization import (
        to_public_front_door_insight_dict as _serialize_front_door,
    )

    return _serialize_front_door(insight)


__all__ = [
    "coerce_front_door_insight",
    "render_insight_card",
    "to_public_front_door_insight_dict",
]
