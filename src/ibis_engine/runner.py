"""Runner helpers for Ibis plans."""

from __future__ import annotations

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from ibis_engine.plan import IbisPlan


def materialize_plan(plan: IbisPlan) -> TableLike:
    """Materialize an Ibis plan to an Arrow table.

    Returns
    -------
    TableLike
        Arrow table with ordering metadata applied when available.
    """
    return plan.to_table()


def stream_plan(plan: IbisPlan, *, batch_size: int | None = None) -> RecordBatchReaderLike:
    """Return a RecordBatchReader for an Ibis plan.

    Returns
    -------
    RecordBatchReaderLike
        RecordBatchReader with ordering metadata applied when available.
    """
    return plan.to_reader(batch_size=batch_size)
