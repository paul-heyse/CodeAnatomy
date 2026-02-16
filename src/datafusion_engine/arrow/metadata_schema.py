"""Schema-level metadata helpers."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion_engine.arrow.interop import SchemaLike
from datafusion_engine.arrow.metadata import schema_identity_from_metadata


def schema_identity(schema: SchemaLike) -> Mapping[str, str]:
    """Return schema-identity metadata mapping when available."""
    metadata = schema.metadata if hasattr(schema, "metadata") else None
    identity = dict(schema_identity_from_metadata(metadata))
    if metadata:
        identity_hash = metadata.get(b"schema.identity_hash")
        if identity_hash is not None:
            identity["schema.identity_hash"] = identity_hash.decode(
                "utf-8",
                errors="replace",
            )
    return identity


__all__ = ["schema_identity"]
