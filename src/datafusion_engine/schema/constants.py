"""Shared schema-level constants for DataFusion engine schemas."""

from __future__ import annotations

from datafusion_engine.arrow.metadata import function_requirements_metadata_spec

ENGINE_FUNCTION_REQUIREMENTS = function_requirements_metadata_spec(
    required=(
        "array_agg",
        "array_distinct",
        "array_sort",
        "array_to_string",
        "bool_or",
        "coalesce",
        "col_to_byte",
        "concat_ws",
        "prefixed_hash64",
        "row_number",
        "sha256",
        "stable_hash64",
        "stable_hash128",
        "stable_id",
    ),
).schema_metadata

__all__ = ["ENGINE_FUNCTION_REQUIREMENTS"]
