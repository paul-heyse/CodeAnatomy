"""Format-specific write handlers extracted from WritePipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from datafusion import DataFrame

    from datafusion_engine.io.write_core import WriteRequest
    from datafusion_engine.session.streaming import StreamingExecutionResult


class _FormatWritePipeline(Protocol):
    def write_csv(self, df: DataFrame, *, request: WriteRequest) -> None: ...

    def write_json(self, df: DataFrame, *, request: WriteRequest) -> None: ...

    def write_parquet(self, df: DataFrame, *, request: WriteRequest) -> None: ...

    def write_arrow(self, result: StreamingExecutionResult, *, request: WriteRequest) -> None: ...


class FormatWriteHandler:
    """Adapter for non-Delta format WritePipeline operations."""

    def __init__(self, pipeline: _FormatWritePipeline) -> None:
        """Initialize the format write handler.

        Parameters
        ----------
        pipeline
            Pipeline object implementing format-specific write methods.
        """
        self._pipeline = pipeline

    def write_csv(self, df: DataFrame, *, request: WriteRequest) -> None:
        """Write DataFrame output as CSV."""
        self._pipeline.write_csv(df, request=request)

    def write_json(self, df: DataFrame, *, request: WriteRequest) -> None:
        """Write DataFrame output as JSON."""
        self._pipeline.write_json(df, request=request)

    def write_parquet(self, df: DataFrame, *, request: WriteRequest) -> None:
        """Write DataFrame output as Parquet."""
        self._pipeline.write_parquet(df, request=request)

    def write_arrow(self, result: StreamingExecutionResult, *, request: WriteRequest) -> None:
        """Write DataFrame output as Arrow IPC."""
        self._pipeline.write_arrow(result, request=request)


__all__ = ["FormatWriteHandler"]
