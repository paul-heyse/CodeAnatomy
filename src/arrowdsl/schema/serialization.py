"""Schema serialization helpers."""

from __future__ import annotations

import hashlib
import json

from arrowdsl.core.interop import SchemaLike
from core_types import JsonDict


def schema_to_dict(schema: SchemaLike) -> JsonDict:
    """Serialize an Arrow schema to a plain dictionary.

    Returns
    -------
    dict[str, object]
        JSON-serializable schema representation.
    """
    return {
        "fields": [
            {"name": field.name, "type": str(field.type), "nullable": bool(field.nullable)}
            for field in schema
        ]
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


__all__ = ["schema_fingerprint", "schema_to_dict"]
