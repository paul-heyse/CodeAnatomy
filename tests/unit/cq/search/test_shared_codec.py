"""Tests for shared msgspec codec helpers."""

from __future__ import annotations

from tools.cq.search._shared.core import decode_mapping, encode_mapping


def test_encode_decode_mapping_roundtrip() -> None:
    payload: dict[str, object] = {"a": 1, "b": "two"}
    encoded = encode_mapping(payload)
    decoded = decode_mapping(encoded)
    assert decoded == payload
