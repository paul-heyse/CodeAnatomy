"""Schema serialization helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence

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
