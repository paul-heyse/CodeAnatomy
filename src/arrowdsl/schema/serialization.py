"""Schema serialization helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence

from arrowdsl.core.interop import DataTypeLike, SchemaLike
from core_types import JsonDict


def schema_to_dict(schema: SchemaLike) -> JsonDict:
    """Serialize an Arrow schema to a plain dictionary.

    Returns
    -------
    dict[str, object]
        JSON-serializable schema representation.
    """
    return {
        "fields": [_field_to_dict(field) for field in schema],
        "metadata": _decode_metadata(schema.metadata),
    }


def _field_to_dict(field: object) -> JsonDict:
    dtype = getattr(field, "type", None)
    return {
        "name": getattr(field, "name", ""),
        "type": str(dtype),
        "nullable": bool(getattr(field, "nullable", True)),
        "metadata": _decode_metadata(getattr(field, "metadata", None)),
        "extension": _extension_info(dtype),
    }


def _decode_metadata(metadata: Mapping[bytes, bytes] | None) -> JsonDict:
    if not metadata:
        return {}
    return {
        key.decode("utf-8", errors="replace"): value.decode("utf-8", errors="replace")
        for key, value in metadata.items()
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


def schema_fingerprint(schema: SchemaLike) -> str:
    """Compute a stable schema fingerprint hash.

    Returns
    -------
    str
        SHA-256 fingerprint of the schema.
    """
    payload = json.dumps(schema_to_dict(schema), sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def dataset_fingerprint(
    *,
    plan_hash: str,
    schema_fingerprint: str,
    profile_hash: str,
    writer_strategy: str,
    input_fingerprints: Sequence[str] = (),
) -> str:
    """Compute a stable fingerprint for a materialized dataset.

    Returns
    -------
    str
        SHA-256 fingerprint for the dataset identity payload.
    """
    payload = {
        "plan_hash": plan_hash,
        "schema_fingerprint": schema_fingerprint,
        "profile_hash": profile_hash,
        "writer_strategy": writer_strategy,
        "input_fingerprints": sorted(input_fingerprints),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = ["dataset_fingerprint", "schema_fingerprint", "schema_to_dict"]
