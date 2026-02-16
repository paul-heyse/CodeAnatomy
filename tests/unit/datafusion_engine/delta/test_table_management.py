# ruff: noqa: D100, D103, ANN001
from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.delta import table_management
from datafusion_engine.delta.control_plane import DeltaProviderRequest, DeltaSnapshotRequest


def test_provider_bundle_delegates(monkeypatch) -> None:
    monkeypatch.setattr(
        table_management, "delta_provider_from_session", lambda *_args, **_kwargs: "bundle"
    )
    request = DeltaProviderRequest(
        table_uri="s3://bucket/table",
        storage_options=None,
        version=None,
        timestamp=None,
        delta_scan=None,
    )
    assert table_management.provider_bundle(SessionContext(), request=request) == "bundle"


def test_snapshot_info_delegates(monkeypatch) -> None:
    monkeypatch.setattr(table_management, "delta_snapshot_info", lambda _req: {"version": 1})
    request = DeltaSnapshotRequest(
        table_uri="s3://bucket/table",
        storage_options=None,
        version=None,
        timestamp=None,
    )
    assert table_management.snapshot_info(request) == {"version": 1}
