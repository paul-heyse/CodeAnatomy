"""Tests for format write handler adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from datafusion_engine.io.format_write_handler import FormatWriteHandler

if TYPE_CHECKING:
    from datafusion import DataFrame

    from datafusion_engine.io.format_write_handler import _FormatWritePipeline
    from datafusion_engine.io.write_core import WriteRequest
    from datafusion_engine.session.streaming import StreamingExecutionResult


class _Pipeline:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def write_csv(self, _df: DataFrame, *, request: WriteRequest) -> None:
        self.calls.append(f"csv:{request}")

    def write_json(self, _df: DataFrame, *, request: WriteRequest) -> None:
        self.calls.append(f"json:{request}")

    def write_parquet(self, _df: DataFrame, *, request: WriteRequest) -> None:
        self.calls.append(f"parquet:{request}")

    def write_arrow(self, _result: StreamingExecutionResult, *, request: WriteRequest) -> None:
        self.calls.append(f"arrow:{request}")


def test_format_write_handler_delegates() -> None:
    """Format write handler should delegate all format-specific methods."""
    pipeline = _Pipeline()
    handler = FormatWriteHandler(cast("_FormatWritePipeline", pipeline))
    df = cast("DataFrame", object())
    result = cast("StreamingExecutionResult", object())
    handler.write_csv(df, request=cast("WriteRequest", "r1"))
    handler.write_json(df, request=cast("WriteRequest", "r2"))
    handler.write_parquet(df, request=cast("WriteRequest", "r3"))
    handler.write_arrow(result, request=cast("WriteRequest", "r4"))
    assert pipeline.calls == ["csv:r1", "json:r2", "parquet:r3", "arrow:r4"]
