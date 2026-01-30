"""PyArrow schema field builder utilities."""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa


def string_field(name: str, *, nullable: bool = True) -> pa.Field:
    """Create a string field.

    Returns
    -------
    pa.Field
        Configured string field.
    """
    return pa.field(name, pa.string(), nullable=nullable)


def int64_field(name: str, *, nullable: bool = True) -> pa.Field:
    """Create an int64 field.

    Returns
    -------
    pa.Field
        Configured int64 field.
    """
    return pa.field(name, pa.int64(), nullable=nullable)


def int32_field(name: str, *, nullable: bool = True) -> pa.Field:
    """Create an int32 field.

    Returns
    -------
    pa.Field
        Configured int32 field.
    """
    return pa.field(name, pa.int32(), nullable=nullable)


def float64_field(name: str, *, nullable: bool = True) -> pa.Field:
    """Create a float64 field.

    Returns
    -------
    pa.Field
        Configured float64 field.
    """
    return pa.field(name, pa.float64(), nullable=nullable)


def bool_field(name: str, *, nullable: bool = True) -> pa.Field:
    """Create a boolean field.

    Returns
    -------
    pa.Field
        Configured boolean field.
    """
    return pa.field(name, pa.bool_(), nullable=nullable)


def binary_field(name: str, *, nullable: bool = True) -> pa.Field:
    """Create a binary field.

    Returns
    -------
    pa.Field
        Configured binary field.
    """
    return pa.field(name, pa.binary(), nullable=nullable)


def timestamp_field(
    name: str,
    *,
    unit: str = "us",
    tz: str | None = None,
    nullable: bool = True,
) -> pa.Field:
    """Create a timestamp field.

    Returns
    -------
    pa.Field
        Configured timestamp field.
    """
    return pa.field(name, pa.timestamp(unit, tz=tz), nullable=nullable)


def list_field(
    name: str,
    value_type: pa.DataType,
    *,
    nullable: bool = True,
) -> pa.Field:
    """Create a list field.

    Returns
    -------
    pa.Field
        Configured list field.
    """
    return pa.field(name, pa.list_(value_type), nullable=nullable)


def struct_field(
    name: str,
    fields: Sequence[pa.Field],
    *,
    nullable: bool = True,
) -> pa.Field:
    """Create a struct field.

    Returns
    -------
    pa.Field
        Configured struct field.
    """
    return pa.field(name, pa.struct(fields), nullable=nullable)


__all__ = [
    "binary_field",
    "bool_field",
    "float64_field",
    "int32_field",
    "int64_field",
    "list_field",
    "string_field",
    "struct_field",
    "timestamp_field",
]
