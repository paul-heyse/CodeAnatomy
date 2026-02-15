"""Shared scoring payload helpers for CQ macros."""

from __future__ import annotations

from typing import cast

from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    bucket,
    confidence_score,
    impact_score,
)
from tools.cq.macros.contracts import MacroScorePayloadV1


def macro_scoring_details(
    *,
    sites: int,
    files: int,
    depth: int = 0,
    breakages: int = 0,
    ambiguities: int = 0,
    evidence_kind: str = "resolved_ast",
) -> dict[str, object]:
    """Build a normalized scoring-details mapping for macro findings."""
    impact = impact_score(
        ImpactSignals(
            sites=sites,
            files=files,
            depth=depth,
            breakages=breakages,
            ambiguities=ambiguities,
        )
    )
    confidence = confidence_score(ConfidenceSignals(evidence_kind=evidence_kind))
    return {
        "impact_score": impact,
        "impact_bucket": bucket(impact),
        "confidence_score": confidence,
        "confidence_bucket": bucket(confidence),
        "evidence_kind": evidence_kind,
    }


def macro_score_payload(*, files: int, findings: int) -> MacroScorePayloadV1:
    """Build a normalized macro scoring payload from simple counters."""
    scoring = macro_scoring_details(
        sites=findings,
        files=files,
        evidence_kind="resolved_ast",
    )
    impact = cast("float", scoring["impact_score"])
    confidence = cast("float", scoring["confidence_score"])
    return MacroScorePayloadV1(
        impact=impact,
        confidence=confidence,
        impact_bucket=str(scoring["impact_bucket"]),
        confidence_bucket=str(scoring["confidence_bucket"]),
        details={
            "files": files,
            "findings": findings,
        },
    )


__all__ = ["macro_score_payload", "macro_scoring_details"]
