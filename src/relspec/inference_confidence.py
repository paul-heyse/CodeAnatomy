"""Confidence metadata for inference-driven decisions.

Attach confidence and rationale to inferred decisions so the system can
fall back conservatively when evidence is insufficient, and governance
dashboards can track policy maturity.
"""

from __future__ import annotations

from serde_msgspec import StructBaseStrict


class InferenceConfidence(StructBaseStrict, frozen=True):
    """Confidence metadata for an inference-driven decision.

    Attributes:
    ----------
    confidence_score
        Numeric confidence in the range [0.0, 1.0].
    evidence_sources
        Sources that contributed to the decision (e.g. ``"lineage"``,
        ``"stats"``, ``"capabilities"``).
    fallback_reason
        Populated when confidence is insufficient and a conservative
        fallback was chosen.
    decision_type
        Category of the decision (e.g. ``"scan_policy"``,
        ``"join_strategy"``).
    decision_value
        The actual decision that was made.
    """

    confidence_score: float
    evidence_sources: tuple[str, ...] = ()
    fallback_reason: str | None = None
    decision_type: str = ""
    decision_value: str = ""


def high_confidence(
    decision_type: str,
    decision_value: str,
    evidence_sources: tuple[str, ...],
    *,
    score: float = 0.9,
) -> InferenceConfidence:
    """Build an ``InferenceConfidence`` for a high-confidence decision.

    Parameters
    ----------
    decision_type
        Category of the decision.
    decision_value
        The actual decision that was made.
    evidence_sources
        Sources backing the decision.
    score
        Confidence score (must be >= 0.8).

    Returns:
    -------
    InferenceConfidence
        Confidence payload with ``confidence_score >= 0.8``.
    """
    clamped = max(0.8, min(score, 1.0))
    return InferenceConfidence(
        confidence_score=clamped,
        evidence_sources=evidence_sources,
        decision_type=decision_type,
        decision_value=decision_value,
    )


def low_confidence(
    decision_type: str,
    decision_value: str,
    fallback_reason: str,
    evidence_sources: tuple[str, ...],
    *,
    score: float = 0.4,
) -> InferenceConfidence:
    """Build an ``InferenceConfidence`` for a low-confidence decision.

    Parameters
    ----------
    decision_type
        Category of the decision.
    decision_value
        The actual decision that was made.
    fallback_reason
        Why the system fell back to a conservative choice.
    evidence_sources
        Sources that were available (possibly incomplete).
    score
        Confidence score (must be < 0.5).

    Returns:
    -------
    InferenceConfidence
        Confidence payload with ``confidence_score < 0.5``.
    """
    clamped = max(0.0, min(score, 0.49))
    return InferenceConfidence(
        confidence_score=clamped,
        evidence_sources=evidence_sources,
        fallback_reason=fallback_reason,
        decision_type=decision_type,
        decision_value=decision_value,
    )


__all__ = [
    "InferenceConfidence",
    "high_confidence",
    "low_confidence",
]
