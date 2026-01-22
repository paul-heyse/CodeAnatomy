"""Streaming write helpers for incremental outputs."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa
from ibis.expr.types import Table as IbisTable

from storage.deltalake import DeltaWriteOptions, write_table_delta


@dataclass(frozen=True)
class StreamingWriteOptions:
    """Streaming options for large incremental outputs."""

    chunk_size: int = 250_000


def stream_expr_to_delta(
    expr: IbisTable,
    path: str,
    *,
    options: StreamingWriteOptions,
    write_options: DeltaWriteOptions,
) -> None:
    """Stream an Ibis expression directly to a Delta table."""
    reader = expr.to_pyarrow_batches(chunk_size=options.chunk_size)
    write_table_delta(reader, path, options=write_options)


def stream_table_to_delta(
    table: pa.Table,
    path: str,
    *,
    options: StreamingWriteOptions,
    write_options: DeltaWriteOptions,
) -> None:
    """Stream a PyArrow table to Delta using record batches."""
    batches = table.to_batches(max_chunksize=options.chunk_size)
    reader = pa.RecordBatchReader.from_batches(table.schema, batches)
    write_table_delta(reader, path, options=write_options)


__all__ = ["StreamingWriteOptions", "stream_expr_to_delta", "stream_table_to_delta"]
