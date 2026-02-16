"""Encoding and source-text helpers for CQ search shared runtime."""

from __future__ import annotations

from tools.cq.search._shared.core import (
    decode_mapping,
    encode_mapping,
    line_col_to_byte_offset,
    node_text,
    sg_node_text,
    source_hash,
    truncate,
)

__all__ = [
    "decode_mapping",
    "encode_mapping",
    "line_col_to_byte_offset",
    "node_text",
    "sg_node_text",
    "source_hash",
    "truncate",
]
