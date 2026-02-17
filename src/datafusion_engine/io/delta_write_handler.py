"""Delta write handler extracted from WritePipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from datafusion_engine.io.write_core import DeltaWriteSpec
    from datafusion_engine.session.streaming import StreamingExecutionResult
    from storage.deltalake.delta_read import DeltaWriteResult


class _DeltaBootstrapPipeline(Protocol):
    def write_delta_bootstrap(
        self,
        result: StreamingExecutionResult,
        *,
        spec: DeltaWriteSpec,
    ) -> DeltaWriteResult: ...


class DeltaWriteHandler:
    """Adapter for Delta-specific WritePipeline operations."""

    def __init__(self, pipeline: _DeltaBootstrapPipeline) -> None:
        """Initialize the Delta write handler.

        Parameters
        ----------
        pipeline
            Pipeline implementing Delta bootstrap writes.
        """
        self._pipeline = pipeline

    def write_bootstrap(
        self,
        result: StreamingExecutionResult,
        *,
        spec: DeltaWriteSpec,
    ) -> DeltaWriteResult:
        """Execute bootstrap write flow for Delta destinations.

        Returns:
        -------
        DeltaWriteResult
            Result payload from the Delta bootstrap write.
        """
        return self._pipeline.write_delta_bootstrap(result, spec=spec)


__all__ = ["DeltaWriteHandler"]
