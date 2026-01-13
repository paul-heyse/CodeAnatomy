"""Shared Arrow structs for spec tables."""

from __future__ import annotations

import pyarrow as pa

SORT_KEY_STRUCT = pa.struct(
    [
        pa.field("column", pa.string(), nullable=False),
        pa.field("order", pa.string(), nullable=False),
    ]
)

DEDUPE_STRUCT = pa.struct(
    [
        pa.field("keys", pa.list_(pa.string()), nullable=False),
        pa.field("tie_breakers", pa.list_(SORT_KEY_STRUCT), nullable=True),
        pa.field("strategy", pa.string(), nullable=False),
    ]
)

VALIDATION_STRUCT = pa.struct(
    [
        pa.field("strict", pa.string(), nullable=False),
        pa.field("coerce", pa.bool_(), nullable=False),
        pa.field("max_errors", pa.int64(), nullable=True),
        pa.field("emit_invalid_rows", pa.bool_(), nullable=False),
        pa.field("emit_error_table", pa.bool_(), nullable=False),
    ]
)

DATASET_REF_STRUCT = pa.struct(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("label", pa.string(), nullable=False),
        pa.field("query_json", pa.string(), nullable=True),
    ]
)

__all__ = [
    "DATASET_REF_STRUCT",
    "DEDUPE_STRUCT",
    "SORT_KEY_STRUCT",
    "VALIDATION_STRUCT",
]
