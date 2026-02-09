"""Fallback quarantine guardrails for decision provenance."""

from __future__ import annotations

from collections.abc import Mapping

import msgspec

from relspec.decision_provenance import DecisionProvenanceGraph
from serde_msgspec import StructBaseStrict


class QuarantineThresholds(StructBaseStrict, frozen=True):
    """Thresholds controlling fallback quarantine decisions."""

    min_confidence: float = 0.35
    max_fallback_ratio: float = 0.30


class FallbackQuarantineDecision(StructBaseStrict, frozen=True):
    """Fallback quarantine decision for one context label."""

    context_label: str
    decision_count: int
    fallback_count: int
    fallback_ratio: float
    mean_confidence: float
    reason: str
    quarantined: bool


class FallbackQuarantineReport(StructBaseStrict, frozen=True):
    """Aggregate fallback quarantine report."""

    run_id: str
    decision_count: int
    fallback_count: int
    quarantined_contexts: tuple[str, ...] = ()
    decisions: tuple[FallbackQuarantineDecision, ...] = ()
    reason_counts: Mapping[str, int] = msgspec.field(default_factory=dict)
    thresholds: QuarantineThresholds = msgspec.field(default_factory=QuarantineThresholds)


def evaluate_fallback_quarantine(
    graph: DecisionProvenanceGraph,
    *,
    thresholds: QuarantineThresholds | None = None,
) -> FallbackQuarantineReport:
    """Evaluate fallback quarantine status from a provenance graph.

    Returns:
    -------
    FallbackQuarantineReport
        Aggregate quarantine evaluation for all decision contexts.
    """
    resolved_thresholds = thresholds or QuarantineThresholds()
    by_context: dict[str, list[tuple[float, str | None]]] = {}
    reason_counts: dict[str, int] = {}

    for decision in graph.decisions:
        context = decision.context_label or "__global__"
        by_context.setdefault(context, []).append(
            (float(decision.confidence_score), decision.fallback_reason)
        )
        if decision.fallback_reason is not None:
            reason_counts[decision.fallback_reason] = (
                reason_counts.get(decision.fallback_reason, 0) + 1
            )

    decisions: list[FallbackQuarantineDecision] = []
    quarantined_contexts: list[str] = []
    fallback_total = 0

    for context in sorted(by_context):
        records = by_context[context]
        decision_count = len(records)
        fallback_count = sum(1 for _score, reason in records if reason is not None)
        fallback_total += fallback_count
        mean_confidence = (
            sum(score for score, _reason in records) / float(decision_count)
            if decision_count > 0
            else 1.0
        )
        fallback_ratio = (
            float(fallback_count) / float(decision_count) if decision_count > 0 else 0.0
        )
        quarantined = (
            fallback_ratio > resolved_thresholds.max_fallback_ratio
            and mean_confidence < resolved_thresholds.min_confidence
        )
        reason = "stable"
        if quarantined:
            reason = "high_fallback_rate_low_confidence"
            quarantined_contexts.append(context)
        elif fallback_count > 0:
            reason = "fallback_observed"
        decisions.append(
            FallbackQuarantineDecision(
                context_label=context,
                decision_count=decision_count,
                fallback_count=fallback_count,
                fallback_ratio=fallback_ratio,
                mean_confidence=mean_confidence,
                reason=reason,
                quarantined=quarantined,
            )
        )

    return FallbackQuarantineReport(
        run_id=graph.run_id,
        decision_count=len(graph.decisions),
        fallback_count=fallback_total,
        quarantined_contexts=tuple(quarantined_contexts),
        decisions=tuple(decisions),
        reason_counts=reason_counts,
        thresholds=resolved_thresholds,
    )


__all__ = [
    "FallbackQuarantineDecision",
    "FallbackQuarantineReport",
    "QuarantineThresholds",
    "evaluate_fallback_quarantine",
]
