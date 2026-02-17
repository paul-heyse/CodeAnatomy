"""Arrow coercion helpers for tables, schemas, and record-batch readers."""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import pyarrow as pa
import pyarrow.types as patypes

from datafusion_engine.arrow.interop import RecordBatchReader, RecordBatchReaderLike


def storage_type(data_type: pa.DataType) -> pa.DataType:
    """Normalize an Arrow type to its storage-safe equivalent.

    Returns:
    -------
    pyarrow.DataType
        Storage-compatible Arrow type.
    """
    if isinstance(data_type, pa.ExtensionType):
        return storage_type(data_type.storage_type)
    result: pa.DataType = data_type
    if patypes.is_string_view(data_type):
        result = pa.string()
    elif patypes.is_binary_view(data_type):
        result = pa.binary()
    elif patypes.is_list_view(data_type):
        value_field = pa.field(
            data_type.value_field.name,
            storage_type(data_type.value_field.type),
            nullable=data_type.value_field.nullable,
            metadata=data_type.value_field.metadata,
        )
        result = pa.list_(value_field)
    elif patypes.is_large_list_view(data_type):
        value_field = pa.field(
            data_type.value_field.name,
            storage_type(data_type.value_field.type),
            nullable=data_type.value_field.nullable,
            metadata=data_type.value_field.metadata,
        )
        result = pa.large_list(value_field)
    elif patypes.is_struct(data_type):
        result = pa.struct(
            [
                pa.field(
                    field.name,
                    storage_type(field.type),
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
                for field in data_type
            ]
        )
    elif patypes.is_list(data_type):
        value_field = pa.field(
            data_type.value_field.name,
            storage_type(data_type.value_field.type),
            nullable=data_type.value_field.nullable,
            metadata=data_type.value_field.metadata,
        )
        result = pa.list_(value_field)
    elif patypes.is_large_list(data_type):
        value_field = pa.field(
            data_type.value_field.name,
            storage_type(data_type.value_field.type),
            nullable=data_type.value_field.nullable,
            metadata=data_type.value_field.metadata,
        )
        result = pa.large_list(value_field)
    elif patypes.is_map(data_type):
        key_field = pa.field(
            data_type.key_field.name,
            storage_type(data_type.key_field.type),
            nullable=data_type.key_field.nullable,
            metadata=data_type.key_field.metadata,
        )
        item_field = pa.field(
            data_type.item_field.name,
            storage_type(data_type.item_field.type),
            nullable=data_type.item_field.nullable,
            metadata=data_type.item_field.metadata,
        )
        result = pa.map_(key_field, item_field, keys_sorted=data_type.keys_sorted)
    return result


def storage_schema(schema: pa.Schema) -> pa.Schema:
    """Return schema with all fields normalized to storage-safe Arrow types."""
    fields = [
        pa.field(
            field.name,
            storage_type(field.type),
            nullable=field.nullable,
            metadata=field.metadata,
        )
        for field in schema
    ]
    return pa.schema(fields, metadata=schema.metadata)


def coerce_table_to_storage(table: pa.Table) -> pa.Table:
    """Cast a table to its storage-safe schema when needed.

    Returns:
    -------
    pyarrow.Table
        Table using storage-compatible schema types.
    """
    target = storage_schema(table.schema)
    if table.schema == target:
        return table
    return table.cast(target)


def coerce_to_recordbatch_reader(value: object) -> RecordBatchReaderLike | None:
    """Coerce value to a ``RecordBatchReaderLike`` when possible.

    Returns:
    -------
    RecordBatchReaderLike | None
        Reader-like value when coercion succeeds, otherwise ``None``.
    """
    if value is None:
        return None
    resolved: RecordBatchReaderLike | None = None
    if isinstance(value, RecordBatchReader):
        resolved = value
    elif isinstance(value, pa.RecordBatch):
        record_batch = cast("pa.RecordBatch", value)
        resolved = cast(
            "RecordBatchReaderLike",
            pa.RecordBatchReader.from_batches(record_batch.schema, [record_batch]),
        )
    else:
        schema = getattr(value, "schema", None)
        to_batches = getattr(value, "to_batches", None)
        if schema is not None and callable(to_batches):
            resolved = cast(
                "RecordBatchReaderLike",
                pa.RecordBatchReader.from_batches(schema, to_batches()),
            )
        elif isinstance(value, Sequence):
            batches = [batch for batch in value if isinstance(batch, pa.RecordBatch)]
            if batches:
                resolved = cast(
                    "RecordBatchReaderLike",
                    pa.RecordBatchReader.from_batches(batches[0].schema, batches),
                )
    return resolved


def _table_from_record_batch_sequence(value: object) -> pa.Table | None:
    if not isinstance(value, Sequence):
        return None
    batches = [batch for batch in value if isinstance(batch, pa.RecordBatch)]
    if not batches:
        return None
    return pa.Table.from_batches(batches, schema=batches[0].schema)


def _table_from_to_arrow_table(value: object) -> pa.Table | None:
    to_arrow_table_fn = getattr(value, "to_arrow_table", None)
    if not callable(to_arrow_table_fn):
        return None
    resolved = to_arrow_table_fn()
    if isinstance(resolved, pa.Table):
        return resolved
    return None


def _table_from_batches(value: object) -> pa.Table | None:
    to_batches = getattr(value, "to_batches", None)
    schema = getattr(value, "schema", None)
    if not callable(to_batches) or not isinstance(schema, pa.Schema):
        return None
    batches = to_batches()
    if not isinstance(batches, Sequence):
        return None
    resolved_batches = [batch for batch in batches if isinstance(batch, pa.RecordBatch)]
    if resolved_batches:
        return pa.Table.from_batches(resolved_batches, schema=schema)
    return pa.Table.from_pylist([], schema=schema)


def to_arrow_table(value: object) -> pa.Table:
    """Coerce supported DataFusion/Arrow values to ``pyarrow.Table``.

    Returns:
    -------
    pyarrow.Table
        Coerced Arrow table.

    Raises:
        TypeError: If the input cannot be converted to an Arrow table.
    """
    resolved: pa.Table | None = None
    if isinstance(value, pa.Table):
        resolved = value
    elif isinstance(value, pa.RecordBatch):
        record_batch = cast("pa.RecordBatch", value)
        resolved = pa.Table.from_batches([record_batch], schema=record_batch.schema)
    elif isinstance(value, RecordBatchReader):
        resolved = cast("pa.RecordBatchReader", value).read_all()
    if resolved is None:
        resolved = _table_from_record_batch_sequence(value)
    if resolved is None:
        resolved = _table_from_to_arrow_table(value)
    if resolved is None:
        resolved = _table_from_batches(value)
    if resolved is not None:
        return resolved
    msg = f"value must be Table/RecordBatch/RecordBatchReader; got {type(value).__name__}"
    raise TypeError(msg)


def ensure_arrow_table(value: object, *, label: str = "value") -> pa.Table:
    """Return value as ``pyarrow.Table`` or raise with contextual label.

    Raises:
        TypeError: If the input cannot be converted to an Arrow table.
    """
    try:
        return to_arrow_table(value)
    except TypeError as exc:
        msg = f"{label} must be Table/RecordBatch/RecordBatchReader; got {type(value).__name__}"
        raise TypeError(msg) from exc


__all__ = [
    "coerce_table_to_storage",
    "coerce_to_recordbatch_reader",
    "ensure_arrow_table",
    "storage_schema",
    "storage_type",
    "to_arrow_table",
]
