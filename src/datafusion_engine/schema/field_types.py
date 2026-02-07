"""Field type resolution for schema derivation from extract metadata.

Provide the bridge between string type descriptors in metadata templates
and concrete ``pyarrow.DataType`` objects used in static schemas.
"""

from __future__ import annotations

import pyarrow as pa

# ---------------------------------------------------------------------------
# Canonical identity field types
# ---------------------------------------------------------------------------

_IDENTITY_FIELD_TYPES: dict[str, pa.DataType] = {
    "file_id": pa.string(),
    "path": pa.string(),
    "file_sha256": pa.string(),
    "repo": pa.string(),
    "index_id": pa.string(),
    "index_path": pa.string(),
}

# ---------------------------------------------------------------------------
# Type hint string -> DataType resolver
# ---------------------------------------------------------------------------

_TYPE_HINT_MAP: dict[str, pa.DataType] = {
    "string": pa.string(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "float64": pa.float64(),
    "bool": pa.bool_(),
    "map<string, string>": pa.map_(pa.string(), pa.string()),
}


def resolve_field_type(field_name: str, type_hint: str) -> pa.DataType:
    """Resolve a field name and type hint string to a ``pa.DataType``.

    The resolution order is:

    1. Identity field names are always ``pa.string()``.
    2. Exact match against the type hint map (scalars and maps).
    3. Parameterized type hints (``list<struct>``, ``struct``) return
       placeholder types; callers that need deep struct detail must
       consult the static schema or ``nested_shapes`` metadata.

    Args:
        field_name: Column name (used for identity-field lookup).
        type_hint: String type descriptor from ``ExtractMetadata.field_types``.

    Returns:
        Resolved PyArrow data type.

    Raises:
        ValueError: If *type_hint* cannot be resolved.
    """
    # 1. Identity columns always resolve to string.
    if field_name in _IDENTITY_FIELD_TYPES:
        return _IDENTITY_FIELD_TYPES[field_name]

    # 2. Exact scalar / map match.
    if type_hint in _TYPE_HINT_MAP:
        return _TYPE_HINT_MAP[type_hint]

    # 3. Parameterized composite types.
    if type_hint == "list<struct>":
        # Placeholder list-of-struct; the element struct is opaque here.
        return pa.list_(pa.struct([]))

    if type_hint == "struct":
        # Placeholder struct; child fields are opaque here.
        return pa.struct([])

    msg = f"Cannot resolve field type for {field_name!r} with hint {type_hint!r}."
    raise ValueError(msg)


__all__ = [
    "resolve_field_type",
]
