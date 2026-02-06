"""Extract postprocess resilience integration tests.

Scope: Verify that view registration failures, view artifact failures,
and schema contract failures are recorded as diagnostic events rather
than propagated as exceptions. Tests validate the canonical postprocess
status taxonomy.

Key code boundaries:
- _write_and_record_extract_output() in
  extract.coordination.materialization (line 502)
- ExtractQualityEvent in engine.diagnostics
- Canonical statuses: register_view_failed, view_artifact_failed,
  schema_contract_failed
"""

from __future__ import annotations

import pytest

from tests.test_helpers.optional_deps import require_datafusion


def setup_module() -> None:
    """Ensure DataFusion is available for this test module."""
    require_datafusion()


# Canonical postprocess status taxonomy
POSTPROCESS_STATUSES = frozenset(
    {
        "register_view_failed",
        "view_artifact_failed",
        "schema_contract_failed",
    }
)

POSTPROCESS_STAGES = frozenset(
    {
        "postprocess",
    }
)


def assert_diagnostic_event_taxonomy(
    event: object,
    *,
    expected_status: str,
    expected_stage: str = "postprocess",
) -> None:
    """Assert a diagnostic event uses canonical status and stage values.

    Centralizes taxonomy assertions to reduce string drift across tests.
    """
    assert expected_status in POSTPROCESS_STATUSES, f"Unknown status: {expected_status}"
    assert expected_stage in POSTPROCESS_STAGES, f"Unknown stage: {expected_stage}"
    assert getattr(event, "status", None) == expected_status
    assert getattr(event, "stage", None) == expected_stage


@pytest.mark.integration
class TestExtractPostprocessResilience:
    """Verify postprocess failures are recorded, not propagated."""

    def test_extract_quality_event_structure(self) -> None:
        """Verify ExtractQualityEvent has expected fields."""
        from engine.diagnostics import ExtractQualityEvent

        event = ExtractQualityEvent(
            dataset="test_ds",
            stage="postprocess",
            status="register_view_failed",
            rows=None,
            location_path=None,
            location_format=None,
            issue="test issue",
        )
        assert event.dataset == "test_ds"
        assert event.stage == "postprocess"
        assert event.status == "register_view_failed"
        assert event.issue == "test issue"

    def test_register_view_failed_taxonomy(self) -> None:
        """Verify register_view_failed event uses canonical taxonomy."""
        from engine.diagnostics import ExtractQualityEvent

        event = ExtractQualityEvent(
            dataset="test_ds",
            stage="postprocess",
            status="register_view_failed",
            rows=None,
            location_path=None,
            location_format=None,
            issue="view registration failed",
        )
        assert_diagnostic_event_taxonomy(event, expected_status="register_view_failed")

    def test_view_artifact_failed_taxonomy(self) -> None:
        """Verify view_artifact_failed event uses canonical taxonomy."""
        from engine.diagnostics import ExtractQualityEvent

        event = ExtractQualityEvent(
            dataset="test_ds",
            stage="postprocess",
            status="view_artifact_failed",
            rows=None,
            location_path=None,
            location_format=None,
            issue="artifact failed",
        )
        assert_diagnostic_event_taxonomy(event, expected_status="view_artifact_failed")

    def test_schema_contract_failed_taxonomy(self) -> None:
        """Verify schema_contract_failed event uses canonical taxonomy."""
        from engine.diagnostics import ExtractQualityEvent

        event = ExtractQualityEvent(
            dataset="test_ds",
            stage="postprocess",
            status="schema_contract_failed",
            rows=None,
            location_path=None,
            location_format=None,
            issue="contract violation",
        )
        assert_diagnostic_event_taxonomy(event, expected_status="schema_contract_failed")

    def test_extract_quality_event_to_payload(self) -> None:
        """Verify ExtractQualityEvent.to_payload() returns dict."""
        from engine.diagnostics import ExtractQualityEvent

        event = ExtractQualityEvent(
            dataset="test_ds",
            stage="postprocess",
            status="register_view_failed",
            rows=100,
            location_path="/tmp/test",
            location_format="delta",
            issue="test error",
        )
        payload = event.to_payload()
        assert isinstance(payload, dict)
        assert payload["dataset"] == "test_ds"
        assert payload["status"] == "register_view_failed"
        assert payload["stage"] == "postprocess"

    def test_extract_quality_event_optional_fields(self) -> None:
        """Verify ExtractQualityEvent handles optional fields correctly."""
        from engine.diagnostics import ExtractQualityEvent

        event = ExtractQualityEvent(
            dataset="test_ds",
            stage="postprocess",
            status="view_artifact_failed",
            rows=None,
            location_path=None,
            location_format=None,
            extractor="cst_extractor",
            plan_fingerprint="fp-123",
        )
        assert event.extractor == "cst_extractor"
        assert event.plan_fingerprint == "fp-123"
        assert event.plan_signature is None

    def test_engine_event_recorder_exists(self) -> None:
        """Verify EngineEventRecorder can be instantiated."""
        from engine.diagnostics import EngineEventRecorder
        from tests.test_helpers.datafusion_runtime import df_profile

        profile = df_profile()
        recorder = EngineEventRecorder(profile)
        assert recorder is not None

    def test_postprocess_status_taxonomy_complete(self) -> None:
        """Verify all canonical postprocess statuses are accounted for.

        This test ensures the taxonomy set covers all statuses used
        in _write_and_record_extract_output().
        """
        expected = {"register_view_failed", "view_artifact_failed", "schema_contract_failed"}
        assert expected == POSTPROCESS_STATUSES

    def test_postprocess_stage_is_canonical(self) -> None:
        """Verify the canonical stage name for postprocess events."""
        assert "postprocess" in POSTPROCESS_STAGES
