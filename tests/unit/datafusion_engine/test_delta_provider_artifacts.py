"""Unit tests for canonical Delta provider artifact payloads."""

from __future__ import annotations

from dataclasses import dataclass

from datafusion_engine.delta.provider_artifacts import (
    build_delta_provider_build_result,
    delta_snapshot_key_payload,
)


@dataclass(frozen=True)
class _CompatibilityStub:
    module: str = "datafusion_ext"
    entrypoint: str = "delta_provider_from_session"
    ctx_kind: str = "outer"
    probe_result: str = "ok"
    compatible: bool = True
    available: bool = True
    error: str | None = None


def test_build_delta_provider_build_result_emits_snapshot_identity_and_fingerprint() -> None:
    """Provider artifact payload should include deterministic snapshot identity fields."""
    result = build_delta_provider_build_result(
        table_uri="s3a://Example-Bucket/path/table",
        dataset_format="delta",
        provider_kind="delta",
        dataset_name="events",
        provider_mode="delta_table_provider",
        strict_native_provider_enabled=True,
        strict_native_provider_violation=False,
        ffi_table_provider=True,
        scan_files_requested=False,
        scan_files_count=0,
        predicate="id > 1",
        compatibility=_CompatibilityStub(),
        registration_path="provider",
        delta_version=11,
        delta_storage_options={"region": "us-east-1"},
        delta_log_storage_options={"aws_endpoint": "http://localhost:4566"},
        delta_scan_effective={"scan_files_requested": False},
        delta_scan_snapshot={"schema_hash": "abc"},
        delta_scan_identity_hash="cafef00d",
        delta_snapshot={"version": 11},
        delta_pruning_applied=True,
        delta_pruned_files=3,
        include_event_metadata=True,
        run_id="run-123",
    )

    payload = result.as_payload()
    snapshot_key = payload.get("snapshot_key")
    assert isinstance(snapshot_key, dict)
    assert snapshot_key["canonical_uri"] == "s3://example-bucket/path/table"
    assert snapshot_key["resolved_version"] == 11
    fingerprint = payload.get("storage_profile_fingerprint")
    assert isinstance(fingerprint, str)
    assert len(fingerprint) == 16
    assert payload["probe_result"] == "ok"
    assert payload["entrypoint"] == "delta_provider_from_session"
    assert payload["strict_native_provider_enabled"] is True
    assert payload["event_time_unix_ms"] > 0


def test_delta_snapshot_key_payload_normalizes_aliases() -> None:
    """Snapshot key payload should canonicalize URI aliases and versions."""
    payload = delta_snapshot_key_payload(
        table_uri="s3n://Example-Bucket/path/table",
        snapshot={"version": "7"},
    )
    assert payload == {
        "canonical_uri": "s3://example-bucket/path/table",
        "resolved_version": 7,
    }
