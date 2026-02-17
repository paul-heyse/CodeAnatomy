"""Integration tests for typed enrichment telemetry flow."""

from __future__ import annotations

from tools.cq.search.enrichment.telemetry_schema import default_enrichment_telemetry_mapping
from tools.cq.search.pipeline.smart_search_telemetry import build_enrichment_telemetry


def test_empty_enrichment_telemetry_uses_typed_schema_defaults() -> None:
    """Verify empty telemetry helper returns the typed schema container shape."""
    telemetry = default_enrichment_telemetry_mapping()

    assert isinstance(telemetry, dict)
    assert isinstance(telemetry.get("python"), dict)
    assert isinstance(telemetry.get("rust"), dict)


def test_build_enrichment_telemetry_preserves_typed_default_shape_on_empty_input() -> None:
    """Verify telemetry builder preserves typed default buckets for empty input."""
    telemetry = build_enrichment_telemetry([])

    assert isinstance(telemetry, dict)
    python_bucket = telemetry.get("python")
    rust_bucket = telemetry.get("rust")
    assert isinstance(python_bucket, dict)
    assert isinstance(rust_bucket, dict)
    assert "stages" in python_bucket
    assert "query_runtime" in python_bucket
    assert "query_runtime" in rust_bucket
