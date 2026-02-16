"""Read helpers for Arrow schema metadata."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion_engine.arrow.interop import SchemaLike
from datafusion_engine.arrow.metadata import metadata_payload


def read_metadata_payload(schema: SchemaLike) -> Mapping[str, object]:
    """Return normalized metadata payload for a schema."""
    return metadata_payload(schema)


__all__ = ["read_metadata_payload"]
