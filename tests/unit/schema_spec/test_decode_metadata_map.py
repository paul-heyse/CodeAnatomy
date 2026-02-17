"""Tests for canonical metadata map decode helper."""

from __future__ import annotations

from schema_spec.arrow_types import decode_metadata_map


def test_decode_metadata_map_decodes_utf8_pairs() -> None:
    """Metadata map decoder converts UTF-8 byte pairs into str mapping."""
    decoded = decode_metadata_map({b"a": b"b"})
    assert decoded == {"a": "b"}
