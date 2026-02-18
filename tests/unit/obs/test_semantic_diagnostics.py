"""Tests for semantic diagnostics sinks."""

from __future__ import annotations

from obs.diagnostics import (
    DiagnosticsCollector,
    SemanticQualityArtifact,
    record_semantic_quality_artifact,
    record_semantic_quality_events,
)
from obs.quality_metrics import quality_issue_rows

EXPECTED_ROW_COUNT = 10


def test_semantic_quality_artifact_payload() -> None:
    """Semantic quality artifacts should capture payload fields."""
    collector = DiagnosticsCollector()
    artifact = SemanticQualityArtifact(
        name="relationship_quality_metrics_v1",
        row_count=10,
        schema_hash="abc123",
        artifact_uri="/tmp/metrics",
        run_id="run_1",
    )
    record_semantic_quality_artifact(collector, artifact=artifact)
    artifacts = collector.artifacts_snapshot()
    assert "semantic_quality_artifact_v1" in artifacts
    payload = artifacts["semantic_quality_artifact_v1"][0]
    assert payload["name"] == "relationship_quality_metrics_v1"
    assert payload["row_count"] == EXPECTED_ROW_COUNT
    assert payload["schema_hash"] == "abc123"
    assert payload["artifact_uri"] == "/tmp/metrics"
    assert payload["run_id"] == "run_1"


def test_semantic_quality_events_capture() -> None:
    """Semantic quality events should be captured in diagnostics."""
    collector = DiagnosticsCollector()
    rows = [{"entity_id": "a", "issue": "low_confidence_edges"}]
    record_semantic_quality_events(
        collector,
        name="semantic_quality_issues_v1",
        rows=rows,
    )
    events = collector.events_snapshot()
    assert "semantic_quality_issues_v1" in events
    assert events["semantic_quality_issues_v1"][0]["entity_id"] == "a"


def test_quality_issue_rows_normalize() -> None:
    """Quality issue rows should normalize into the canonical schema."""
    rows = [{"entity_id": "file_a", "issue": "missing_extraction_sources"}]
    normalized = quality_issue_rows(
        entity_kind="file",
        rows=rows,
        source_table="file_coverage_report_v1",
    )
    assert normalized == [
        {
            "entity_kind": "file",
            "entity_id": "file_a",
            "issue": "missing_extraction_sources",
            "source_table": "file_coverage_report_v1",
        }
    ]
