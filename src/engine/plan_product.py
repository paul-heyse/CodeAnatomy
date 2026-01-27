"""Plan product wrappers for streaming or materialized outputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from engine.plan_policy import WriterStrategy

if TYPE_CHECKING:
    from datafusion_engine.execution_facade import ExecutionResult
    from datafusion_engine.view_artifacts import DataFusionViewArtifact


@dataclass(frozen=True)
class PlanProduct:
    """Plan output bundle with metadata for materialization decisions."""

    plan_id: str
    schema: pa.Schema
    determinism_tier: DeterminismTier
    writer_strategy: WriterStrategy
    view_artifact: DataFusionViewArtifact | None = None
    stream: RecordBatchReaderLike | None = None
    table: TableLike | None = None
    execution_result: ExecutionResult | None = None

    def value(self) -> TableLike | RecordBatchReaderLike:
        """Return the underlying table or stream value.

        Returns
        -------
        TableLike | RecordBatchReaderLike
            The stream when available, otherwise the table.

        Raises
        ------
        ValueError
            Raised when neither a stream nor table is present.
        """
        if self.stream is not None:
            return self.stream
        if self.table is not None:
            return self.table
        msg = "PlanProduct contains neither a stream nor a table."
        raise ValueError(msg)

    def materialize_table(self) -> TableLike:
        """Return a table, materializing from the stream when needed.

        Returns
        -------
        TableLike
            Materialized Arrow table.

        Raises
        ------
        ValueError
            Raised when neither a stream nor table is present.
        """
        if self.table is not None:
            return self.table
        if self.stream is None:
            msg = "PlanProduct contains neither a stream nor a table."
            raise ValueError(msg)
        return self.stream.read_all()


__all__ = ["PlanProduct"]
