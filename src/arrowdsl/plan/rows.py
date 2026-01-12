"""Row-based ingestion helpers for Arrow plans."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import cast

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.core.interop import ArrayLike, FieldLike, RecordBatchReaderLike, SchemaLike, TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.schema.arrays import (
    list_view_array_from_lists,
    map_array_from_pairs,
    struct_array_from_dicts,
)
from arrowdsl.schema.schema import empty_table


def _map_pairs(values: Sequence[object | None]) -> list[Sequence[tuple[object, object]] | None]:
    pairs: list[Sequence[tuple[object, object]] | None] = []
    for value in values:
        if value is None:
            pairs.append(None)
        elif isinstance(value, Mapping):
            pairs.append(list(value.items()))
        elif isinstance(value, (list, tuple)):
            pairs.append(value)
        else:
            pairs.append(None)
    return pairs


def _array_from_values(values: Sequence[object | None], field: FieldLike) -> ArrayLike:
    dtype = field.type
    if patypes.is_struct(dtype):
        return struct_array_from_dicts(
            cast("Sequence[Mapping[str, object] | None]", values),
            struct_type=cast("pa.StructType", dtype),
        )
    if patypes.is_map(dtype):
        map_type = cast("pa.MapType", dtype)
        key_type = getattr(map_type, "key_type", map_type.key_field.type)
        item_type = getattr(map_type, "item_type", map_type.item_field.type)
        return map_array_from_pairs(
            _map_pairs(values),
            key_type=key_type,
            item_type=item_type,
        )
    if patypes.is_large_list_view(dtype) or patypes.is_list_view(dtype):
        list_type = cast("pa.ListViewType", dtype)
        return list_view_array_from_lists(
            cast("Sequence[Sequence[object] | None]", values),
            value_type=list_type.value_type,
            large=patypes.is_large_list_view(dtype),
        )
    return pa.array(values, type=dtype)


def _record_batch_from_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    schema: SchemaLike,
) -> pa.RecordBatch:
    arrays = [_array_from_values([row.get(field.name) for row in rows], field) for field in schema]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def rows_to_table(rows: Sequence[Mapping[str, object]], schema: SchemaLike) -> TableLike:
    """Build a table from row mappings or return an empty table.

    Returns
    -------
    TableLike
        Table constructed from rows or an empty table.
    """
    if not rows:
        return empty_table(schema)
    arrays = [_array_from_values([row.get(field.name) for row in rows], field) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


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
