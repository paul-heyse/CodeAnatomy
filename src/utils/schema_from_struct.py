"""Build top-level Arrow schemas from struct values."""

from __future__ import annotations

import pyarrow as pa


def _resolve_struct(value: pa.StructType | pa.Field | pa.DataType) -> pa.StructType:
    if isinstance(value, pa.StructType):
        return value
    dtype: pa.DataType = value.type if isinstance(value, pa.Field) else value
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        dtype = dtype.value_type
    if not pa.types.is_struct(dtype):
        msg = f"Expected struct-compatible value, received {dtype!r}."
        raise TypeError(msg)
    return dtype


def schema_from_struct(value: pa.StructType | pa.Field | pa.DataType) -> pa.Schema:
    """Return a ``pa.Schema`` from a struct type (or struct-bearing field)."""
    struct_type = _resolve_struct(value)
    fields = [
        pa.field(
            field.name,
            field.type,
            nullable=field.nullable,
            metadata=field.metadata,
        )
        for field in struct_type
    ]
    return pa.schema(fields)


__all__ = ["schema_from_struct"]
