"""Tests for fallback quarantine evaluation."""

from __future__ import annotations

from relspec.decision_provenance import DecisionProvenanceGraph, DecisionRecord
from relspec.fallback_quarantine import QuarantineThresholds, evaluate_fallback_quarantine


def test_quarantine_flags_low_confidence_high_fallback_context() -> None:
    graph = DecisionProvenanceGraph(
        run_id="run-1",
        decisions=(
            DecisionRecord(
                decision_id="d1",
                domain="scan_policy",
                decision_type="compiled",
                decision_value="x",
                confidence_score=0.1,
                fallback_reason="no_stats",
                context_label="dataset_a",
            ),
            DecisionRecord(
                decision_id="d2",
                domain="scan_policy",
                decision_type="compiled",
                decision_value="y",
                confidence_score=0.2,
                fallback_reason="no_stats",
                context_label="dataset_a",
            ),
        ),
        root_ids=("d1",),
    )
    report = evaluate_fallback_quarantine(
        graph,
        thresholds=QuarantineThresholds(min_confidence=0.35, max_fallback_ratio=0.30),
    )
    assert report.decision_count == 2
    assert report.fallback_count == 2
    assert report.quarantined_contexts == ("dataset_a",)


def test_quarantine_skips_stable_contexts() -> None:
    graph = DecisionProvenanceGraph(
        run_id="run-1",
        decisions=(
            DecisionRecord(
                decision_id="d1",
                domain="cache_policy",
                decision_type="compiled",
                decision_value="delta_output",
                confidence_score=0.95,
                context_label="view_a",
            ),
        ),
        root_ids=("d1",),
    )
    report = evaluate_fallback_quarantine(graph)
    assert report.fallback_count == 0
    assert report.quarantined_contexts == ()
    assert report.decisions[0].quarantined is False
