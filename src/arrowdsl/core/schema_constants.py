"""Shared schema and provenance constants."""

from __future__ import annotations

KEY_FIELDS_META = b"key_fields"
REQUIRED_NON_NULL_META = b"required_non_null"
SCHEMA_META_NAME = b"schema_name"
SCHEMA_META_VERSION = b"schema_version"

PROVENANCE_COLS: tuple[str, ...] = (
    "prov_filename",
    "prov_fragment_index",
    "prov_batch_index",
    "prov_last_in_fragment",
)

PROVENANCE_SOURCE_FIELDS: dict[str, str] = {
    "prov_filename": "__filename",
    "prov_fragment_index": "__fragment_index",
    "prov_batch_index": "__batch_index",
    "prov_last_in_fragment": "__last_in_fragment",
}

__all__ = [
    "KEY_FIELDS_META",
    "PROVENANCE_COLS",
    "PROVENANCE_SOURCE_FIELDS",
    "REQUIRED_NON_NULL_META",
    "SCHEMA_META_NAME",
    "SCHEMA_META_VERSION",
]
