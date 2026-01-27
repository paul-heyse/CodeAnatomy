"""Shared schema and provenance constants."""

from __future__ import annotations

DEFAULT_VALUE_META = b"default_value"
KEY_FIELDS_META = b"key_fields"
REQUIRED_NON_NULL_META = b"required_non_null"
REQUIRED_FUNCTIONS_META = b"required_functions"
OPTIONAL_FUNCTIONS_META = b"optional_functions"
REQUIRED_FUNCTION_SIGNATURES_META = b"required_function_signatures"
REQUIRED_FUNCTION_SIGNATURE_TYPES_META = b"required_function_signature_types"
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
    "DEFAULT_VALUE_META",
    "KEY_FIELDS_META",
    "OPTIONAL_FUNCTIONS_META",
    "PROVENANCE_COLS",
    "PROVENANCE_SOURCE_FIELDS",
    "REQUIRED_FUNCTIONS_META",
    "REQUIRED_FUNCTION_SIGNATURES_META",
    "REQUIRED_FUNCTION_SIGNATURE_TYPES_META",
    "REQUIRED_NON_NULL_META",
    "SCHEMA_META_NAME",
    "SCHEMA_META_VERSION",
]
