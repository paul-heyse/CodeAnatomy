"""Streaming adapters for Arrow-like sources."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import cast

import pyarrow as pa
import pyarrow.dataset as ds

from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike, coerce_table_like


def _ensure_reader_schema(
    reader: pa.RecordBatchReader,
    *,
    schema: pa.Schema | None,
) -> pa.RecordBatchReader:
    if schema is None:
        return reader
    if reader.schema != schema:
        msg = "Schema negotiation is not supported"
        raise ValueError(msg)
    return reader


def _reader_from_table(table: TableLike, *, schema: pa.Schema | None) -> pa.RecordBatchReader:
    if schema is not None and table.schema != schema:
        msg = "Schema negotiation is not supported"
        raise ValueError(msg)
    return cast("pa.RecordBatchReader", table.to_reader())


def _reader_from_reader_like(
    obj: object,
    *,
    schema: pa.Schema | None,
) -> pa.RecordBatchReader | None:
    if not isinstance(obj, RecordBatchReaderLike):
        return None
    return _ensure_reader_schema(cast("pa.RecordBatchReader", obj), schema=schema)


def _reader_from_scanner(
    obj: object,
    *,
    schema: pa.Schema | None,
) -> pa.RecordBatchReader | None:
    if not isinstance(obj, ds.Scanner):
        return None
    if schema is not None:
        msg = "Schema negotiation is not supported"
        raise ValueError(msg)
    scanner = cast("ds.Scanner", obj)
    return scanner.to_reader()


def _reader_from_table_like(
    obj: object,
    *,
    schema: pa.Schema | None,
) -> pa.RecordBatchReader | None:
    if not isinstance(obj, TableLike):
        return None
    return _reader_from_table(obj, schema=schema)


def _reader_from_arrow_stream(
    obj: object,
    *,
    schema: pa.Schema | None,
) -> pa.RecordBatchReader | None:
    if not hasattr(obj, "__arrow_c_stream__"):
        return None
    return _ensure_reader_schema(
        pa.RecordBatchReader.from_stream(obj, schema=schema),
        schema=schema,
    )


def _reader_from_batches_provider(
    obj: object,
    *,
    schema: pa.Schema | None,
) -> pa.RecordBatchReader | None:
    to_batches = getattr(obj, "to_pyarrow_batches", None)
    if not callable(to_batches):
        return None
    if schema is not None:
        msg = "Schema negotiation is not supported"
        raise ValueError(msg)
    return cast("pa.RecordBatchReader", to_batches())


def _reader_from_reader_provider(
    obj: object,
    *,
    schema: pa.Schema | None,
) -> pa.RecordBatchReader | None:
    to_reader_method = getattr(obj, "to_reader", None)
    if not callable(to_reader_method):
        return None
    if schema is not None:
        msg = "Schema negotiation is not supported"
        raise ValueError(msg)
    try:
        return cast("pa.RecordBatchReader", to_reader_method())
    except TypeError:
        return None


def _reader_from_object(
    obj: object,
    *,
    schema: pa.Schema | None,
) -> pa.RecordBatchReader | None:
    resolvers = (
        _reader_from_reader_like,
        _reader_from_scanner,
        _reader_from_table_like,
        _reader_from_arrow_stream,
        _reader_from_batches_provider,
        _reader_from_reader_provider,
    )
    for resolver in resolvers:
        reader = resolver(obj, schema=schema)
        if reader is not None:
            return reader
    return None


def to_reader(obj: object, *, schema: pa.Schema | None = None) -> pa.RecordBatchReader:
    """Return a RecordBatchReader for a streaming source.

    Parameters
    ----------
    obj
        Source object to convert into a RecordBatchReader.
    schema
        Optional schema to apply when supported by the source.

    Returns
    -------
    pyarrow.RecordBatchReader
        Record batch reader for the input source.

    Raises
    ------
    TypeError
        Raised when the source cannot be coerced into a reader.
    ValueError
        Raised when schema negotiation is requested but unsupported.
    """
    try:
        reader = _reader_from_object(obj, schema=schema)
    except ValueError as exc:
        raise ValueError(str(exc)) from exc
    if reader is not None:
        return reader
    try:
        coerced = coerce_table_like(obj)
    except TypeError as exc:
        msg = f"Unsupported streaming source: {type(obj)}."
        raise TypeError(msg) from exc
    if isinstance(coerced, RecordBatchReaderLike):
        reader = cast("pa.RecordBatchReader", coerced)
        return _ensure_reader_schema(reader, schema=schema)
    return _reader_from_table(coerced, schema=schema)


@dataclass(frozen=True)
class StreamDiagnostics:
    """Diagnostics captured from streaming a RecordBatchReader.

    Attributes
    ----------
    batch_count : int
        Number of batches processed.
    row_count : int
        Total number of rows processed.
    """

    batch_count: int
    row_count: int


def emit_stream_diagnostics(
    reader: pa.RecordBatchReader,
    *,
    reporter: Callable[[StreamDiagnostics], None] | None = None,
) -> pa.RecordBatchReader:
    """Wrap a RecordBatchReader to emit batch and row count diagnostics.

    Parameters
    ----------
    reader : RecordBatchReader
        Source reader to wrap.
    reporter : Callable[[StreamDiagnostics], None] | None
        Optional callback to receive diagnostics when streaming completes.

    Returns
    -------
    pa.RecordBatchReader
        Wrapped reader that tracks batch and row counts.
    """
    if reporter is None:
        return reader

    callback = reporter

    def _batches() -> Iterable[pa.RecordBatch]:
        batch_count = 0
        row_count = 0
        try:
            for batch in reader:
                batch_count += 1
                row_count += batch.num_rows
                yield batch
        finally:
            callback(StreamDiagnostics(batch_count=batch_count, row_count=row_count))

    return pa.RecordBatchReader.from_batches(reader.schema, _batches())


__all__ = ["StreamDiagnostics", "emit_stream_diagnostics", "to_reader"]
