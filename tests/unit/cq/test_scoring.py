"""Tests for CQ scoring helpers."""

from __future__ import annotations

from tools.cq.core.schema import ScoreDetails
from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    build_detail_payload,
    build_score_details,
)


def test_build_score_details_with_signals() -> None:
    impact = ImpactSignals(sites=100, files=20, depth=10, breakages=10, ambiguities=10)
    confidence = ConfidenceSignals(evidence_kind="bytecode")

    details = build_score_details(impact=impact, confidence=confidence)

    assert details is not None
    assert details.impact_score == 1.0
    assert details.impact_bucket == "high"
    assert details.confidence_score == 0.90
    assert details.confidence_bucket == "high"
    assert details.evidence_kind == "bytecode"


def test_build_detail_payload_from_score() -> None:
    score = ScoreDetails(impact_score=0.5, impact_bucket="med")

    payload = build_detail_payload(score=score, kind="test", data={"foo": "bar"})

    assert payload.kind == "test"
    assert payload.score == score
    assert payload.data == {"foo": "bar"}


def test_build_detail_payload_from_mapping() -> None:
    scoring = {
        "impact_score": 0.2,
        "impact_bucket": "low",
        "confidence_score": 0.75,
        "confidence_bucket": "high",
        "evidence_kind": "rg_only",
    }

    payload = build_detail_payload(scoring=scoring, kind="legacy", data={"key": 1})

    assert payload.kind == "legacy"
    assert payload.score is not None
    assert payload.score.impact_score == 0.2
    assert payload.score.impact_bucket == "low"
    assert payload.score.confidence_score == 0.75
    assert payload.score.confidence_bucket == "high"
    assert payload.score.evidence_kind == "rg_only"
    assert payload.data == {"key": 1}
