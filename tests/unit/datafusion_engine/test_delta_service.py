"""DeltaService policy resolution tests."""

from __future__ import annotations

from types import SimpleNamespace

from tests.test_helpers.optional_deps import require_datafusion

require_datafusion()

import pyarrow as pa
import pytest

from datafusion_engine.delta import service as delta_service
from datafusion_engine.delta.store_policy import DeltaStorePolicy
from datafusion_engine.session.runtime import DataFusionRuntimeProfile, PolicyBundleConfig
from storage.deltalake.delta import DeltaReadRequest


def test_delta_service_table_version_resolves_store_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure DeltaService merges store policy and request options."""
    captured: dict[str, dict[str, str] | None] = {}

    def _fake_delta_table_version(
        path: str,
        *,
        storage_options: dict[str, str] | None = None,
        log_storage_options: dict[str, str] | None = None,
        gate: object | None = None,
    ) -> int:
        _ = (path, gate)
        captured["storage_options"] = storage_options
        captured["log_storage_options"] = log_storage_options
        return 7

    monkeypatch.setattr(delta_service, "delta_table_version", _fake_delta_table_version)

    policy = DeltaStorePolicy(
        storage_options={"policy": "1"},
        log_storage_options={"log": "1"},
    )
    profile = DataFusionRuntimeProfile(policies=PolicyBundleConfig(delta_store_policy=policy))
    service = profile.delta_service()

    version = service.table_version(
        path="s3://bucket/table",
        storage_options={"request": "2"},
        log_storage_options={"log": "2", "extra": "x"},
    )

    assert version == 7
    assert captured["storage_options"] == {"policy": "1", "request": "2"}
    assert captured["log_storage_options"] == {"log": "2", "extra": "x"}


def test_delta_service_read_table_attaches_runtime_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure DeltaService forwards runtime profile and store policy to reads."""
    captured: dict[str, object] = {}

    def _fake_read_delta_table(request: DeltaReadRequest) -> pa.Table:
        captured["runtime_profile"] = request.runtime_profile
        captured["storage_options"] = request.storage_options
        captured["log_storage_options"] = request.log_storage_options
        return pa.table({"id": [1]})

    monkeypatch.setattr(delta_service, "read_delta_table_eager", _fake_read_delta_table)

    policy = DeltaStorePolicy(storage_options={"policy": "1"}, log_storage_options={"log": "1"})
    profile = DataFusionRuntimeProfile(policies=PolicyBundleConfig(delta_store_policy=policy))
    service = profile.delta_service()

    request = DeltaReadRequest(
        path="/tmp/delta",
        storage_options={"request": "2"},
        log_storage_options={"log": "2"},
    )
    table = service.read_table_eager(request)

    assert table.num_rows == 1
    assert captured["runtime_profile"] is profile
    assert captured["storage_options"] == {"policy": "1", "request": "2"}
    assert captured["log_storage_options"] == {"log": "2"}


def test_delta_service_provider_artifact_payload_includes_canonical_fields() -> None:
    """Provider artifact payloads should include canonical snapshot identity metadata."""
    profile = DataFusionRuntimeProfile()
    service = profile.delta_service()
    request = SimpleNamespace(
        name="events",
        location=SimpleNamespace(
            path="s3a://Example-Bucket/path/table",
            format="delta",
            delta_version=7,
            delta_timestamp=None,
            delta_log_storage_options={"aws_endpoint": "http://localhost:4566"},
            storage_options={"region": "us-east-1"},
        ),
        resolution=SimpleNamespace(
            provider_kind="delta",
            delta_scan_options=None,
            delta_scan_effective={"fallback": True},
            delta_scan_snapshot={"scan": "v1"},
            delta_scan_identity_hash="abc123",
            delta_snapshot={"version": 7, "provider_mode": "pyarrow_dataset_degraded"},
            add_actions=[{"path": "part-000.parquet"}],
            predicate_error="predicate parse failed",
        ),
        predicate="id > 1",
        scan_files=("part-000.parquet",),
    )
    compatibility = SimpleNamespace(
        module="datafusion_ext",
        entrypoint="delta_provider_from_session",
        ctx_kind="outer",
        probe_result="ok",
        compatible=True,
        available=True,
        error=None,
    )

    payload = service._provider_artifact_payload(  # noqa: SLF001
        request=request,
        compatibility=compatibility,
    )

    assert payload["provider_mode"] == "pyarrow_dataset_degraded"
    assert payload["strict_native_provider_enabled"] is True
    assert payload["strict_native_provider_violation"] is True
    assert payload["scan_files_count"] == 1
    assert payload["delta_pruned_files"] == 1
    assert payload["probe_result"] == "ok"
    snapshot_key = payload.get("snapshot_key")
    assert isinstance(snapshot_key, dict)
    assert snapshot_key["resolved_version"] == 7
    assert snapshot_key["canonical_uri"] == "s3://example-bucket/path/table"
    fingerprint = payload.get("storage_profile_fingerprint")
    assert isinstance(fingerprint, str)
    assert len(fingerprint) == 16
