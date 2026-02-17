"""Tests for typed enrichment telemetry schema."""

from __future__ import annotations

from tools.cq.search.enrichment.telemetry_schema import (
    EnrichmentTelemetryV1,
    default_enrichment_telemetry_mapping,
)


def test_default_enrichment_telemetry_mapping_has_expected_buckets() -> None:
    """Verify default telemetry mapping initializes expected language buckets."""
    telemetry = default_enrichment_telemetry_mapping()

    assert isinstance(telemetry, dict)
    assert set(telemetry.keys()) == {"python", "rust"}
    python_bucket = telemetry["python"]
    rust_bucket = telemetry["rust"]
    assert isinstance(python_bucket, dict)
    assert isinstance(rust_bucket, dict)
    assert "stages" in python_bucket
    assert "timings_ms" in python_bucket
    assert "query_runtime" in rust_bucket


def test_enrichment_telemetry_struct_defaults_to_zero() -> None:
    """Verify typed telemetry struct starts with zeroed counters."""
    telemetry = EnrichmentTelemetryV1()

    assert telemetry.python.applied == 0
    assert telemetry.python.query_runtime.did_exceed_match_limit == 0
    assert telemetry.rust.applied == 0
    assert telemetry.rust.query_pack_tags == 0
