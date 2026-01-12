"""Struct field helpers."""

from __future__ import annotations

import pyarrow as pa


def flatten_struct_field(field: pa.Field) -> list[pa.Field]:
    """Flatten a struct field into child fields with parent-name prefixes.

    Returns
    -------
    list[pyarrow.Field]
        Flattened fields with parent-name prefixes.
    """
    return list(field.flatten())


__all__ = ["flatten_struct_field"]
