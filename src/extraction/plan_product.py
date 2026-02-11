"""Plan product wrappers for streaming or materialized outputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

from core_types import DeterminismTier
from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.materialize_policy import WriterStrategy

if TYPE_CHECKING:
    from datafusion_engine.session.facade import ExecutionResult
    from datafusion_engine.views.artifacts import DataFusionViewArtifact
    from extraction.materialize_pipeline import MaterializationCacheDecision


@dataclass(frozen=True)
class PlanProduct:
    """Plan output bundle with metadata for materialization decisions."""

    plan_id: str
    schema: pa.Schema
    determinism_tier: DeterminismTier
    writer_strategy: WriterStrategy
    view_artifact: DataFusionViewArtifact | None = None
    cache_decision: MaterializationCacheDecision | None = None
    stream: RecordBatchReaderLike | None = None
    table: TableLike | None = None
    execution_result: ExecutionResult | None = None

    def value(self) -> TableLike | RecordBatchReaderLike:
        """Return the underlying table or stream value.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if self.stream is not None:
            return self.stream
        if self.table is not None:
            return self.table
        msg = "PlanProduct contains neither a stream nor a table."
        raise ValueError(msg)

    def materialize_table(self) -> TableLike:
        """Return a table, materializing from the stream when needed.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if self.table is not None:
            return self.table
        if self.stream is None:
            msg = "PlanProduct contains neither a stream nor a table."
            raise ValueError(msg)
        return self.stream.read_all()


__all__ = ["PlanProduct"]
