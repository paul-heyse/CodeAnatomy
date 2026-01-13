"""Row-based ingestion helpers for Arrow plans."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence

import pyarrow as pa

from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.schema.factories import rows_to_table as rows_to_table_factory
from arrowdsl.schema.nested_builders import nested_array_factory


def _record_batch_from_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    schema: SchemaLike,
) -> pa.RecordBatch:
    arrays = [
        nested_array_factory(field, [row.get(field.name) for row in rows]) for field in schema
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def rows_to_table(rows: Sequence[Mapping[str, object]], schema: SchemaLike) -> TableLike:
    """Build a table from row mappings or return an empty table.

    Returns
    -------
    TableLike
        Table constructed from rows or an empty table.
    """
    return rows_to_table_factory(rows, schema)


def record_batches_from_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    schema: SchemaLike,
    batch_size: int = 4096,
) -> Iterator[pa.RecordBatch]:
    """Yield RecordBatches from row mappings.

    Yields
    ------
    pyarrow.RecordBatch
        Record batches built from buffered rows.
    """
    buffer: list[Mapping[str, object]] = []
    for row in rows:
        buffer.append(row)
        if len(buffer) >= batch_size:
            yield _record_batch_from_rows(buffer, schema=schema)
            buffer.clear()
    if buffer:
        yield _record_batch_from_rows(buffer, schema=schema)


def reader_from_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    schema: SchemaLike,
    batch_size: int = 4096,
) -> RecordBatchReaderLike:
    """Build a RecordBatchReader from row mappings.

    Returns
    -------
    pyarrow.RecordBatchReader
        Reader streaming record batches.
    """
    batches = record_batches_from_rows(rows, schema=schema, batch_size=batch_size)
    return pa.RecordBatchReader.from_batches(schema, batches)


def plan_from_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    schema: SchemaLike,
    batch_size: int = 4096,
    label: str = "",
) -> Plan:
    """Create a Plan from row mappings via a RecordBatchReader.

    Returns
    -------
    Plan
        Plan backed by a record batch reader.
    """
    reader = reader_from_rows(rows, schema=schema, batch_size=batch_size)
    return Plan.from_reader(reader, label=label)


__all__ = [
    "plan_from_rows",
    "reader_from_rows",
    "record_batches_from_rows",
    "rows_to_table",
]
