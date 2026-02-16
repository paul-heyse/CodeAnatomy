"""Unit tests for canonical Delta provider artifact payloads."""

from __future__ import annotations

from dataclasses import dataclass, field

from datafusion_engine.delta.capabilities import DeltaExtensionCompatibility
from datafusion_engine.delta.provider_artifacts import (
    DeltaProviderBuildRequest,
    RegistrationProviderArtifactInput,
    ServiceProviderArtifactInput,
    build_delta_provider_build_result,
    delta_snapshot_key_payload,
    provider_build_request_from_registration_context,
    provider_build_request_from_service_context,
)

RESOLVED_DELTA_VERSION = 11
FINGERPRINT_LENGTH = 16
PRUNED_FILE_COUNT = 2


def _compatibility_stub() -> DeltaExtensionCompatibility:
    return DeltaExtensionCompatibility(
        available=True,
        compatible=True,
        error=None,
        entrypoint="delta_provider_from_session",
        module="datafusion_ext",
        ctx_kind="outer",
        probe_result="ok",
    )


@dataclass(frozen=True)
class _RegistrationContextStub:
    dataset_name: str | None = "events"
    provider_mode: str = "delta_table_provider"
    ffi_table_provider: bool = True
    strict_native_provider_enabled: bool | None = True
    strict_native_provider_violation: bool | None = False
    delta_scan: object | None = None
    delta_scan_effective: dict[str, object] | None = None
    delta_scan_snapshot: object | None = field(default_factory=lambda: {"schema_hash": "abc"})
    delta_scan_identity_hash: str | None = "cafef00d"
    snapshot: dict[str, object] | None = None
    registration_path: str = "provider"
    predicate: str | None = "id > 1"
    predicate_error: str | None = None
    add_actions: tuple[dict[str, object], ...] | None = None


@dataclass(frozen=True)
class _ServiceLocationStub:
    path: str = "s3://bucket/events"
    format: str = "delta"
    delta_version: int | None = 7
    delta_timestamp: str | None = None
    delta_log_storage_options: dict[str, str] | None = None
    storage_options: dict[str, str] | None = None


@dataclass(frozen=True)
class _ServiceResolutionStub:
    provider_kind: str = "delta"
    delta_scan_options: object | None = None
    delta_scan_effective: dict[str, object] | None = None
    delta_scan_snapshot: object | None = None
    delta_scan_identity_hash: str | None = None
    delta_snapshot: dict[str, object] | None = None
    predicate_error: str | None = None
    add_actions: tuple[dict[str, object], ...] | None = None


@dataclass(frozen=True)
class _ServiceRequestStub:
    location: _ServiceLocationStub
    resolution: _ServiceResolutionStub
    name: str | None = "events"
    predicate: str | None = "id > 1"
    scan_files: tuple[str, ...] | None = ("part-0001.parquet",)


def test_build_delta_provider_build_result_emits_snapshot_identity_and_fingerprint() -> None:
    """Provider artifact payload should include deterministic snapshot identity fields."""
    request = DeltaProviderBuildRequest(
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
        compatibility=_compatibility_stub(),
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
    result = build_delta_provider_build_result(request)

    payload = result.as_payload()
    snapshot_key = payload.get("snapshot_key")
    assert isinstance(snapshot_key, dict)
    assert snapshot_key["canonical_uri"] == "s3://example-bucket/path/table"
    assert snapshot_key["resolved_version"] == RESOLVED_DELTA_VERSION
    fingerprint = payload.get("storage_profile_fingerprint")
    assert isinstance(fingerprint, str)
    assert len(fingerprint) == FINGERPRINT_LENGTH
    assert payload["probe_result"] == "ok"
    assert payload["entrypoint"] == "delta_provider_from_session"
    assert payload["strict_native_provider_enabled"] is True
    event_time = payload.get("event_time_unix_ms")
    assert isinstance(event_time, int)
    assert event_time > 0


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


def test_provider_build_request_adapter_from_registration_context() -> None:
    """Registration adapter should derive pruning and scan metadata deterministically."""
    context = _RegistrationContextStub(
        add_actions=({"path": "a.parquet"}, {"path": "b.parquet"}),
        snapshot={"version": 3},
    )
    request = provider_build_request_from_registration_context(
        RegistrationProviderArtifactInput(
            table_uri="s3://bucket/events",
            dataset_format="delta",
            provider_kind="delta",
            compatibility=_compatibility_stub(),
            context=context,
        )
    )
    assert isinstance(request, DeltaProviderBuildRequest)
    assert request.delta_pruning_applied is True
    assert request.delta_pruned_files == PRUNED_FILE_COUNT
    assert request.delta_scan_ignored is False
    assert request.provider_mode == "delta_table_provider"


def test_provider_build_request_adapter_from_service_context() -> None:
    """Service adapter should map request/resolution fields without payload drift."""
    request = _ServiceRequestStub(
        location=_ServiceLocationStub(delta_log_storage_options={"region": "us-east-1"}),
        resolution=_ServiceResolutionStub(add_actions=({"path": "a.parquet"},)),
    )
    build_request = provider_build_request_from_service_context(
        ServiceProviderArtifactInput(
            request=request,
            compatibility=_compatibility_stub(),
            provider_mode="delta_table_provider",
            strict_native_provider_enabled=True,
            strict_native_provider_violation=False,
            include_event_metadata=True,
            run_id="run-123",
        )
    )
    assert build_request.scan_files_requested is True
    assert build_request.scan_files_count == 1
    assert build_request.delta_pruned_files == 1
    assert build_request.strict_native_provider_enabled is True
    assert build_request.run_id == "run-123"


def test_provider_build_request_adapter_preserves_internal_bootstrap_context() -> None:
    """Registration adapter should preserve bootstrap metadata for internal tables."""
    context = _RegistrationContextStub(
        dataset_name="datafusion_view_cache_inventory_v1",
        strict_native_provider_enabled=False,
        strict_native_provider_violation=False,
        registration_path="bootstrap",
        predicate=None,
        predicate_error=None,
        add_actions=None,
        snapshot={"version": 0},
    )
    request = provider_build_request_from_registration_context(
        RegistrationProviderArtifactInput(
            table_uri="file:///tmp/artifacts/datafusion_view_cache_inventory_v1",
            dataset_format="delta",
            provider_kind="delta",
            compatibility=_compatibility_stub(),
            context=context,
        )
    )
    assert request.dataset_name == "datafusion_view_cache_inventory_v1"
    assert request.registration_path == "bootstrap"
    assert request.strict_native_provider_enabled is False
    assert request.delta_pruning_applied is False
    assert request.delta_pruned_files is None
    assert request.delta_scan_ignored is False
    assert request.delta_snapshot == {"version": 0}


def test_provider_build_result_includes_internal_bootstrap_snapshot_identity() -> None:
    """Bootstrap registrations should emit stable snapshot identity for internal tables."""
    request = DeltaProviderBuildRequest(
        table_uri="file:///tmp/artifacts/delta_observability/semantic_delta_snapshot_artifacts_v1",
        dataset_format="delta",
        provider_kind="delta",
        dataset_name="semantic_delta_snapshot_artifacts_v1",
        provider_mode="delta_table_provider",
        strict_native_provider_enabled=False,
        strict_native_provider_violation=False,
        ffi_table_provider=True,
        compatibility=_compatibility_stub(),
        registration_path="bootstrap",
        delta_snapshot={"version": 0},
        include_event_metadata=False,
    )
    payload = build_delta_provider_build_result(request).as_payload()
    assert payload["dataset_name"] == "semantic_delta_snapshot_artifacts_v1"
    assert payload["registration_path"] == "bootstrap"
    snapshot_key = payload.get("snapshot_key")
    assert isinstance(snapshot_key, dict)
    assert snapshot_key["resolved_version"] == 0
    canonical_uri = snapshot_key["canonical_uri"]
    assert isinstance(canonical_uri, str)
    assert canonical_uri.startswith("file://")
    storage_fingerprint = payload.get("storage_profile_fingerprint")
    assert isinstance(storage_fingerprint, str)
    assert len(storage_fingerprint) == FINGERPRINT_LENGTH
    assert "event_time_unix_ms" not in payload
