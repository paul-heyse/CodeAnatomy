"""Tests for Rust-bridge Delta write orchestration."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest
from datafusion import SessionContext

from datafusion_engine.io import delta_write_handler
from datafusion_engine.io.write_core import WriteFormat, WriteMode, WriteRequest
from storage.deltalake import DeltaWriteResult

if TYPE_CHECKING:
    from datafusion_engine.io.write_core import DeltaWriteSpec
    from datafusion_engine.io.write_pipeline import WritePipeline
    from datafusion_engine.session.streaming import StreamingExecutionResult


class _DeltaService:
    @staticmethod
    def table_version(**_kwargs: object) -> int | None:
        return None


class _DeltaOps:
    @staticmethod
    def delta_service() -> _DeltaService:
        return _DeltaService()


class _RuntimeProfile:
    def __init__(self) -> None:
        self.delta_ops = _DeltaOps()


class _Pipeline:
    def __init__(self) -> None:
        self.ctx = SessionContext()
        self.runtime_profile = None


EXPECTED_VERSION_A = 9
EXPECTED_VERSION_B = 4


def _spec() -> DeltaWriteSpec:
    return cast(
        "DeltaWriteSpec",
        SimpleNamespace(
            table_uri="/tmp/delta_table",
            storage_options=None,
            log_storage_options=None,
            mode="append",
            feature_gate=None,
            commit_metadata={"operation": "write"},
            commit_key="dataset_a",
            commit_app_id="app",
            commit_version=1,
            commit_run=None,
            table_properties={"delta.feature.x": "enabled"},
        ),
    )


def test_write_delta_uses_rust_bridge(monkeypatch: pytest.MonkeyPatch) -> None:
    """write_delta should consume normalized payload from execute_delta_write_bridge."""
    pipeline = _Pipeline()
    request = WriteRequest(
        source="SELECT 1",
        destination="/tmp/delta_table",
        format=WriteFormat.DELTA,
        mode=WriteMode.APPEND,
    )
    bridge_called: dict[str, bool] = {"called": False}

    def _require(_profile: object, *, operation: str) -> _RuntimeProfile:
        assert operation == "delta writes"
        return _RuntimeProfile()

    monkeypatch.setattr(delta_write_handler, "_require_runtime_profile", _require)
    monkeypatch.setattr(
        delta_write_handler, "_validate_delta_protocol_support", lambda **_kwargs: None
    )
    monkeypatch.setattr(
        delta_write_handler, "run_post_write_maintenance", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        delta_write_handler, "record_delta_mutation", lambda *_args, **_kwargs: None
    )

    def _bridge(
        _pipeline: object,
        _result: object,
        *,
        spec: object,
    ) -> delta_write_handler.DeltaBridgeWriteResult:
        bridge_called["called"] = True
        _ = spec
        return delta_write_handler.DeltaBridgeWriteResult(
            delta_result=DeltaWriteResult(
                path="/tmp/delta_table",
                version=EXPECTED_VERSION_A,
                report={"ok": True},
            ),
            final_version=EXPECTED_VERSION_A,
            mutation_report={"ok": True, "version": EXPECTED_VERSION_A},
            enabled_features={"delta.feature.x": "enabled"},
            constraint_status="skipped",
        )

    monkeypatch.setattr(delta_write_handler, "execute_delta_write_bridge", _bridge)

    outcome = delta_write_handler.write_delta(
        cast("WritePipeline", pipeline),
        cast("StreamingExecutionResult", object()),
        request=request,
        spec=_spec(),
    )

    assert bridge_called["called"] is True
    assert outcome.delta_result.version == EXPECTED_VERSION_A
    assert outcome.enabled_features.get("delta.feature.x") == "enabled"
    assert outcome.constraint_status == "skipped"


def test_write_delta_bootstrap_returns_bridge_delta_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """write_delta_bootstrap should return the DeltaWriteResult from bridge execution."""
    pipeline = _Pipeline()

    monkeypatch.setattr(
        delta_write_handler,
        "execute_delta_write_bridge",
        lambda *_args, **_kwargs: delta_write_handler.DeltaBridgeWriteResult(
            delta_result=DeltaWriteResult(
                path="/tmp/delta_table",
                version=EXPECTED_VERSION_B,
                report={"ok": True},
            ),
            final_version=EXPECTED_VERSION_B,
            mutation_report={"ok": True, "version": EXPECTED_VERSION_B},
            enabled_features={},
            constraint_status="skipped",
        ),
    )

    result = delta_write_handler.write_delta_bootstrap(
        cast("WritePipeline", pipeline),
        cast("StreamingExecutionResult", object()),
        spec=_spec(),
    )
    assert result.version == EXPECTED_VERSION_B
