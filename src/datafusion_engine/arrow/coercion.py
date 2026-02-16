"""PyArrow type coercion utilities."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.arrow.interop import RecordBatchReaderLike, coerce_table_like


def storage_type(data_type: pa.DataType) -> pa.DataType:
    """Recursively unwrap ExtensionType to storage type.

    Parameters
    ----------
    data_type
        PyArrow data type, possibly containing ExtensionTypes.

    Returns:
    -------
    pa.DataType
        The underlying storage type with ExtensionTypes unwrapped.
    """
    resolved = data_type
    if isinstance(data_type, pa.ExtensionType):
        resolved = storage_type(data_type.storage_type)
    elif pa.types.is_string_view(data_type):
        resolved = pa.string()
    elif pa.types.is_binary_view(data_type):
        resolved = pa.binary()
    elif pa.types.is_struct(data_type):
        fields = [
            pa.field(field.name, storage_type(field.type), field.nullable, field.metadata)
            for field in data_type
        ]
        resolved = pa.struct(fields)
    elif pa.types.is_list(data_type) or pa.types.is_list_view(data_type):
        value_field = data_type.value_field
        value_type = storage_type(value_field.type)
        resolved = pa.list_(
            pa.field(
                value_field.name,
                value_type,
                value_field.nullable,
                value_field.metadata,
            )
        )
    elif pa.types.is_large_list(data_type) or pa.types.is_large_list_view(data_type):
        value_field = data_type.value_field
        value_type = storage_type(value_field.type)
        resolved = pa.large_list(
            pa.field(
                value_field.name,
                value_type,
                value_field.nullable,
                value_field.metadata,
            )
        )
    elif pa.types.is_map(data_type):
        resolved = pa.map_(
            storage_type(data_type.key_type),
            storage_type(data_type.item_type),
            keys_sorted=data_type.keys_sorted,
        )
    return resolved


def storage_schema(schema: pa.Schema) -> pa.Schema:
    """Convert schema to storage representation.

    Parameters
    ----------
    schema
        PyArrow schema, possibly containing ExtensionTypes.

    Returns:
    -------
    pa.Schema
        Schema with all ExtensionTypes unwrapped to storage types.
    """
    fields = [
        pa.field(field.name, storage_type(field.type), field.nullable, field.metadata)
        for field in schema
    ]
    return pa.schema(fields, metadata=schema.metadata)


def coerce_table_to_storage(table: pa.Table) -> pa.Table:
    """Coerce table to use storage types only.

    Parameters
    ----------
    table
        PyArrow table, possibly containing ExtensionTypes.

    Returns:
    -------
    pa.Table
        Table with all columns cast to storage types.
    """
    target_schema = storage_schema(table.schema)
    if table.schema.equals(target_schema):
        return table
    return table.cast(target_schema)


def to_arrow_table(value: object) -> pa.Table:
    """Convert various Arrow-like inputs to a Table.

    Parameters
    ----------
    value
        Table-like input or RecordBatchReader-like input.

    Returns:
    -------
    pa.Table
        Converted table.
    """
    resolved = coerce_table_like(value, requested_schema=None)
    if isinstance(resolved, RecordBatchReaderLike):
        return resolved.read_all()
    if isinstance(resolved, pa.Table):
        return resolved
    return pa.Table.from_pydict(resolved.to_pydict())


def ensure_arrow_table(value: object, *, label: str = "input") -> pa.Table:
    """Convert table-like input into a PyArrow table.

    Returns:
        pa.Table: Converted PyArrow table.

    Raises:
        TypeError: If *value* cannot be converted into a table.
    """
    try:
        return to_arrow_table(value)
    except TypeError as exc:
        msg = f"{label} must be Table/RecordBatch/RecordBatchReader, got {type(value).__name__}"
        raise TypeError(msg) from exc


__all__ = [
    "coerce_table_to_storage",
    "ensure_arrow_table",
    "storage_schema",
    "storage_type",
    "to_arrow_table",
]
