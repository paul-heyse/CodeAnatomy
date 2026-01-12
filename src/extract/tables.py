"""Shared table construction helpers for extractors."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.plan_helpers import (
    flatten_struct_field,
    project_columns,
    query_for_schema,
)
from arrowdsl.schema.schema import (
    SchemaTransform,
    empty_table,
    projection_for_schema,
)


def rows_to_table(rows: Sequence[Mapping[str, object]], schema: SchemaLike) -> TableLike:
    """Build a table from row mappings or return an empty table.

    Returns
    -------
    TableLike
        Table constructed from rows or an empty table.
    """
    if not rows:
        return empty_table(schema)
    return pa.Table.from_pylist(list(rows), schema=schema)


def align_table(table: TableLike, *, schema: SchemaLike) -> TableLike:
    """Align a table to a target schema.

    Returns
    -------
    TableLike
        Aligned table.
    """
    return SchemaTransform(schema=schema).apply(table)


def iter_record_batches(
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
            yield pa.RecordBatch.from_pylist(buffer, schema=schema)
            buffer.clear()
    if buffer:
        yield pa.RecordBatch.from_pylist(buffer, schema=schema)


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
    batches = iter_record_batches(rows, schema=schema, batch_size=batch_size)
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


def align_plan(
    plan: Plan,
    *,
    schema: SchemaLike,
    available: Sequence[str] | None = None,
    ctx: ExecutionContext | None = None,
) -> Plan:
    """Return a plan aligned to the target schema via projection.

    Returns
    -------
    Plan
        Plan projecting/casting columns to the schema.
    """
    if available is None:
        available = schema.names if ctx is None else plan.schema(ctx=ctx).names
    safe_cast = True if ctx is None else ctx.safe_cast
    exprs, names = projection_for_schema(schema, available=available, safe_cast=safe_cast)
    return plan.project(exprs, names, ctx=ctx)


__all__ = [
    "align_plan",
    "align_table",
    "flatten_struct_field",
    "iter_record_batches",
    "plan_from_rows",
    "project_columns",
    "query_for_schema",
    "reader_from_rows",
    "rows_to_table",
]
