"""Shared Arrow schema builders for common patterns."""

from __future__ import annotations

import pyarrow as pa

_DEFAULT_VERSION_TYPE = pa.int32()


def map_entry_type(*, with_kind: bool = True) -> pa.StructType:
    """Return a map entry struct type.

    Parameters
    ----------
    with_kind
        When True, include a value_kind field for type-tagging.

    Returns
    -------
    pa.StructType
        Struct type for map entries.
    """
    fields: list[pa.Field] = [pa.field("key", pa.string(), nullable=False)]
    if with_kind:
        fields.append(pa.field("value_kind", pa.string(), nullable=False))
    fields.append(pa.field("value", pa.string(), nullable=True))
    return pa.struct(fields)


def versioned_entries_schema(
    entry_type: pa.DataType,
    *,
    version_type: pa.DataType | None = None,
    entries_nullable: bool = True,
) -> pa.Schema:
    """Return a versioned entries schema.

    Returns
    -------
    pa.Schema
        Schema with version and entries fields.
    """
    resolved_version = version_type or _DEFAULT_VERSION_TYPE
    return pa.schema(
        [
            pa.field("version", resolved_version, nullable=False),
            pa.field("entries", pa.list_(entry_type), nullable=entries_nullable),
        ]
    )


def string_list_schema(*, version_type: pa.DataType | None = None) -> pa.Schema:
    """Return a versioned string list schema.

    Returns
    -------
    pa.Schema
        Schema for versioned string lists.
    """
    return versioned_entries_schema(pa.string(), version_type=version_type)


__all__ = [
    "map_entry_type",
    "string_list_schema",
    "versioned_entries_schema",
]
