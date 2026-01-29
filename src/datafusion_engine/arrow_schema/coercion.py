"""PyArrow type coercion utilities."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.arrow_interop import RecordBatchReaderLike, coerce_table_like


def storage_type(data_type: pa.DataType) -> pa.DataType:
    """Recursively unwrap ExtensionType to storage type.

    Parameters
    ----------
    data_type
        PyArrow data type, possibly containing ExtensionTypes.

    Returns
    -------
    pa.DataType
        The underlying storage type with ExtensionTypes unwrapped.
    """
    if isinstance(data_type, pa.ExtensionType):
        return storage_type(data_type.storage_type)
    if pa.types.is_struct(data_type):
        fields = [
            pa.field(field.name, storage_type(field.type), field.nullable, field.metadata)
            for field in data_type
        ]
        return pa.struct(fields)
    if pa.types.is_list(data_type):
        value_field = data_type.value_field
        value_type = storage_type(value_field.type)
        return pa.list_(
            pa.field(
                value_field.name,
                value_type,
                value_field.nullable,
                value_field.metadata,
            )
        )
    if pa.types.is_large_list(data_type):
        value_field = data_type.value_field
        value_type = storage_type(value_field.type)
        return pa.large_list(
            pa.field(
                value_field.name,
                value_type,
                value_field.nullable,
                value_field.metadata,
            )
        )
    if pa.types.is_map(data_type):
        return pa.map_(
            storage_type(data_type.key_type),
            storage_type(data_type.item_type),
            keys_sorted=data_type.keys_sorted,
        )
    return data_type


def storage_schema(schema: pa.Schema) -> pa.Schema:
    """Convert schema to storage representation.

    Parameters
    ----------
    schema
        PyArrow schema, possibly containing ExtensionTypes.

    Returns
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

    Returns
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

    Returns
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


__all__ = [
    "coerce_table_to_storage",
    "storage_schema",
    "storage_type",
    "to_arrow_table",
]
