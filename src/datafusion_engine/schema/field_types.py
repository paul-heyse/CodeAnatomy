"""Field type resolution for schema derivation from extract metadata.

Provide the bridge between string type descriptors in metadata templates
and concrete ``pyarrow.DataType`` objects used in static schemas.
"""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.schema.type_resolution import resolve_arrow_type

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

    # 2. Parameterized composite types.
    if type_hint == "list<struct>":
        # Placeholder list-of-struct; the element struct is opaque here.
        return pa.list_(pa.struct([]))

    if type_hint == "struct":
        # Placeholder struct; child fields are opaque here.
        return pa.struct([])

    # 3. Canonical scalar/map hint resolution.
    try:
        return resolve_arrow_type(type_hint)
    except ValueError as exc:
        msg = f"Cannot resolve field type for {field_name!r} with hint {type_hint!r}."
        raise ValueError(msg) from exc


__all__ = [
    "resolve_field_type",
]
