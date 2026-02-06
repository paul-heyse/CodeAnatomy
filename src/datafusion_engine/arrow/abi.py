"""Schema ABI fingerprinting and payload helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast

import pyarrow as pa
import pyarrow.types as patypes

from core_types import JsonDict
from datafusion_engine.arrow.interop import DataTypeLike, SchemaLike
from datafusion_engine.arrow.semantic import register_semantic_extension_types
from serde_msgspec import dumps_msgpack, loads_msgpack

SCHEMA_ABI_VERSION = 1


def _resolve_schema(schema: SchemaLike) -> pa.Schema:
    register_semantic_extension_types()
    if isinstance(schema, pa.Schema):
        return schema
    to_pyarrow = getattr(schema, "to_pyarrow", None)
    if callable(to_pyarrow):
        resolved = to_pyarrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Schema must be a pyarrow.Schema derived from DataFusion."
    raise TypeError(msg)


def schema_abi_payload(schema: SchemaLike) -> JsonDict:
    """Return a canonical ABI payload for a schema.

    Returns:
    -------
    dict[str, object]
        Deterministic schema ABI payload.
    """
    resolved = _resolve_schema(schema)
    return {
        "abi_version": SCHEMA_ABI_VERSION,
        "fields": [_field_to_dict(field) for field in resolved],
        "metadata": _decode_metadata(resolved.metadata),
    }


def _field_to_dict(field: object) -> JsonDict:
    dtype = getattr(field, "type", None)
    payload: JsonDict = {
        "name": getattr(field, "name", ""),
        "type": str(dtype),
        "nullable": bool(getattr(field, "nullable", True)),
        "metadata": _decode_metadata(getattr(field, "metadata", None)),
        "extension": _extension_info(dtype),
    }
    flattened = _flattened_fields(field, dtype=dtype)
    if flattened:
        payload["flattened_fields"] = flattened
    return payload


def _flattened_fields(field: object, *, dtype: DataTypeLike | None) -> list[JsonDict] | None:
    if dtype is None or not patypes.is_struct(dtype):
        return None
    flatten = getattr(field, "flatten", None)
    if not callable(flatten):
        return None
    flattened = cast("Sequence[object]", flatten())
    return [_field_to_dict(child) for child in flattened]


def _decode_metadata(metadata: Mapping[bytes, bytes] | None) -> JsonDict:
    if not metadata:
        return {}
    items = sorted(metadata.items(), key=lambda item: item[0])
    return {
        key.decode("utf-8", errors="replace"): value.decode("utf-8", errors="replace")
        for key, value in items
    }


def _extension_info(dtype: DataTypeLike | None) -> JsonDict | None:
    if dtype is None:
        return None
    extension_name = getattr(dtype, "extension_name", None)
    storage_type = getattr(dtype, "storage_type", None)
    if extension_name is None and storage_type is None:
        return None
    return {
        "extension_name": str(extension_name) if extension_name is not None else None,
        "storage_type": str(storage_type) if storage_type is not None else None,
    }


def schema_to_dict(schema: SchemaLike) -> JsonDict:
    """Return a JSON-ready ABI payload for a schema.

    Returns:
    -------
    JsonDict
        JSON-ready ABI payload.
    """
    return schema_abi_payload(schema)


def schema_to_msgpack(schema: SchemaLike) -> bytes:
    """Serialize a schema ABI payload to MessagePack bytes.

    Returns:
    -------
    bytes
        MessagePack-encoded schema payload.
    """
    return dumps_msgpack(schema_abi_payload(schema))


def schema_from_msgpack(payload: bytes) -> pa.Schema:
    """Deserialize an Arrow schema from MessagePack bytes.

    Args:
        payload: Description.

    Returns:
        pa.Schema: Result.

    Raises:
        TypeError: If the operation cannot be completed.
    """
    decoded = loads_msgpack(payload, target_type=object, strict=False)
    if isinstance(decoded, pa.Schema):
        return decoded
    msg = "MessagePack payload is not a pyarrow.Schema."
    raise TypeError(msg)


__all__ = [
    "schema_abi_payload",
    "schema_from_msgpack",
    "schema_to_dict",
    "schema_to_msgpack",
]
