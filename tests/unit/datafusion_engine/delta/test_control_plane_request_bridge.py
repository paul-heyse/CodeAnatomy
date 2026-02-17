"""Tests for control-plane MessagePack request bridges."""

from __future__ import annotations

import msgspec
import pytest
from datafusion import SessionContext

from datafusion_engine.delta import control_plane_maintenance, control_plane_mutation
from datafusion_engine.delta.control_plane_core import (
    DeltaOptimizeRequest,
    DeltaWriteRequest,
)

_DELTA_VERSION = 7
_TARGET_SIZE_BYTES = 1_048_576


def test_mutation_bridge_uses_messagepack_request_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mutation bridge encodes control-plane requests as MessagePack."""
    ctx = SessionContext()
    captured: dict[str, object] = {}

    def _entrypoint(_ctx: object, payload: bytes) -> dict[str, object]:
        captured["payload"] = payload
        return {"operation": "write"}

    monkeypatch.setattr(
        control_plane_mutation,
        "_require_internal_entrypoint",
        lambda name: (
            _entrypoint
            if name == "delta_write_ipc_request"
            else (_ for _ in ()).throw(AssertionError(name))
        ),
    )
    monkeypatch.setattr(control_plane_mutation, "_internal_ctx", lambda *_args, **_kwargs: object())

    response = control_plane_mutation.delta_write_ipc(
        ctx,
        request=DeltaWriteRequest(
            table_uri="s3://bucket/table",
            storage_options={"region": "us-east-1"},
            version=_DELTA_VERSION,
            timestamp=None,
            data_ipc=msgspec.Raw(msgspec.msgpack.encode(b"")),
            mode="append",
            schema_mode=None,
            partition_columns=None,
            target_file_size=None,
            extra_constraints=None,
            gate=None,
            commit_options=None,
        ),
    )

    assert response["operation"] == "write"
    payload = captured.get("payload")
    assert isinstance(payload, bytes)
    decoded = msgspec.msgpack.decode(payload, type=dict)
    assert decoded["table_uri"] == "s3://bucket/table"
    assert decoded["version"] == _DELTA_VERSION


def test_maintenance_bridge_uses_messagepack_request_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Maintenance bridge encodes control-plane requests as MessagePack."""
    ctx = SessionContext()
    captured: dict[str, object] = {}

    def _entrypoint(_ctx: object, payload: bytes) -> dict[str, object]:
        captured["payload"] = payload
        return {"operation": "optimize"}

    monkeypatch.setattr(
        control_plane_maintenance,
        "_require_internal_entrypoint",
        lambda name: (
            _entrypoint
            if name == "delta_optimize_compact_request_payload"
            else (_ for _ in ()).throw(AssertionError(name))
        ),
    )
    monkeypatch.setattr(
        control_plane_maintenance, "_internal_ctx", lambda *_args, **_kwargs: object()
    )

    response = control_plane_maintenance.delta_optimize_compact(
        ctx,
        request=DeltaOptimizeRequest(
            table_uri="s3://bucket/table",
            storage_options=None,
            version=None,
            timestamp=None,
            target_size=_TARGET_SIZE_BYTES,
            z_order_cols=["id"],
            gate=None,
            commit_options=None,
        ),
    )

    assert response["operation"] == "optimize"
    payload = captured.get("payload")
    assert isinstance(payload, bytes)
    decoded = msgspec.msgpack.decode(payload, type=dict)
    assert decoded["table_uri"] == "s3://bucket/table"
    assert decoded["target_size"] == _TARGET_SIZE_BYTES
