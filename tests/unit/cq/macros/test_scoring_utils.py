"""Tests for shared macro scoring helpers."""

from __future__ import annotations

from tools.cq.macros.scoring_utils import macro_score_payload, macro_scoring_details


def test_macro_score_payload_populates_buckets() -> None:
    payload = macro_score_payload(files=3, findings=5)
    assert payload.impact >= 0.0
    assert payload.confidence >= 0.0
    assert payload.impact_bucket in {"low", "med", "high"}
    assert payload.confidence_bucket in {"low", "med", "high"}


def test_macro_scoring_details_supports_breakage_signals() -> None:
    details = macro_scoring_details(
        sites=10,
        files=4,
        depth=1,
        breakages=2,
        ambiguities=3,
        evidence_kind="resolved_ast",
    )
    assert details["impact_score"] is not None
    assert details["confidence_score"] is not None
    assert details["evidence_kind"] == "resolved_ast"
