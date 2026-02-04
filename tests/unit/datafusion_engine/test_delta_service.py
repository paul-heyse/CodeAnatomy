"""DeltaService policy resolution tests."""

from __future__ import annotations

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

    monkeypatch.setattr(delta_service, "read_delta_table", _fake_read_delta_table)

    policy = DeltaStorePolicy(storage_options={"policy": "1"}, log_storage_options={"log": "1"})
    profile = DataFusionRuntimeProfile(policies=PolicyBundleConfig(delta_store_policy=policy))
    service = profile.delta_service()

    request = DeltaReadRequest(
        path="/tmp/delta",
        storage_options={"request": "2"},
        log_storage_options={"log": "2"},
    )
    table = service.read_table(request)

    assert table.num_rows == 1
    assert captured["runtime_profile"] is profile
    assert captured["storage_options"] == {"policy": "1", "request": "2"}
    assert captured["log_storage_options"] == {"log": "2"}
