"""Tests for Delta write handler adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from datafusion_engine.io.delta_write_handler import DeltaWriteHandler

if TYPE_CHECKING:
    from datafusion_engine.io.delta_write_handler import _DeltaBootstrapPipeline
    from datafusion_engine.io.write_core import DeltaWriteSpec
    from datafusion_engine.session.streaming import StreamingExecutionResult
    from storage.deltalake.delta_read import DeltaWriteResult


class _Pipeline:
    def __init__(self) -> None:
        self.called = False

    def write_delta_bootstrap(
        self,
        _result: StreamingExecutionResult,
        *,
        spec: DeltaWriteSpec,
    ) -> DeltaWriteResult:
        self.called = True
        return cast("DeltaWriteResult", f"ok:{getattr(spec, 'table_uri', 'spec')}")


def test_delta_write_handler_delegates() -> None:
    """Delta write handler should delegate to pipeline bootstrap writer."""
    pipeline = _Pipeline()
    handler = DeltaWriteHandler(cast("_DeltaBootstrapPipeline", pipeline))
    result = cast("StreamingExecutionResult", object())
    spec = cast("DeltaWriteSpec", object())
    assert handler.write_bootstrap(result, spec=spec) == "ok:spec"
    assert pipeline.called is True
