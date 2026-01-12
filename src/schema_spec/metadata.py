"""Shared schema metadata constants and helpers."""

from __future__ import annotations

SCHEMA_META_NAME = b"schema_name"
SCHEMA_META_VERSION = b"schema_version"


def schema_metadata(name: str, version: int) -> dict[bytes, bytes]:
    """Return schema metadata for name/version tagging.

    Parameters
    ----------
    name:
        Schema name to record.
    version:
        Schema version to record.

    Returns
    -------
    dict[bytes, bytes]
        Metadata mapping suitable for ``pyarrow.Schema.with_metadata``.
    """
    return {
        SCHEMA_META_NAME: name.encode("utf-8"),
        SCHEMA_META_VERSION: str(version).encode("utf-8"),
    }
