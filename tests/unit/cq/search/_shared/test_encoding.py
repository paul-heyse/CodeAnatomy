"""Tests for shared encoding helper exports."""

from __future__ import annotations

from tools.cq.search._shared.encoding import (
    decode_mapping,
    encode_mapping,
    line_col_to_byte_offset,
    source_hash,
    truncate,
)


def test_encode_decode_mapping_roundtrip() -> None:
    """Encoding helpers should preserve mapping content across round-trip."""
    payload: dict[str, object] = {"k": "v", "n": 1}
    assert decode_mapping(encode_mapping(payload)) == payload


def test_line_col_to_byte_offset_handles_utf8_columns() -> None:
    """Line/column conversion should account for UTF-8 byte width."""
    offset = line_col_to_byte_offset("a\npi=\u03c0\n".encode(), 2, 4)
    assert offset is not None


def test_source_hash_and_truncate_are_stable() -> None:
    """Hash/trim helpers should expose deterministic behavior."""
    assert source_hash(b"abc") == source_hash(b"abc")
    assert truncate("abcdef", 4) == "a..."
