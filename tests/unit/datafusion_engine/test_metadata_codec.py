"""Tests for MessagePack metadata codec helpers."""

from __future__ import annotations

import msgspec

from datafusion_engine.arrow_schema.metadata_codec import (
    decode_metadata_list,
    decode_metadata_map,
    decode_metadata_scalar_map,
    encode_metadata_list,
    encode_metadata_map,
    encode_metadata_scalar_map,
)


def test_metadata_map_roundtrip() -> None:
    """Round-trip metadata map payloads."""
    payload = encode_metadata_map({"b": "2", "a": "1"})
    assert decode_metadata_map(payload) == {"a": "1", "b": "2"}


def test_metadata_map_empty_payloads() -> None:
    """Handle empty metadata map payloads."""
    assert decode_metadata_map(None) == {}
    assert decode_metadata_map(b"") == {}


def test_metadata_list_roundtrip() -> None:
    """Round-trip metadata list payloads."""
    payload = encode_metadata_list(["alpha", "beta"])
    assert decode_metadata_list(payload) == ["alpha", "beta"]


def test_metadata_list_empty_payloads() -> None:
    """Handle empty metadata list payloads."""
    assert decode_metadata_list(None) == []
    assert decode_metadata_list(b"") == []


def test_metadata_scalar_map_roundtrip() -> None:
    """Round-trip metadata scalar map payloads."""
    payload = encode_metadata_scalar_map(
        {
            "int": 42,
            "float": 3.5,
            "bool": True,
            "bytes": b"blob",
            "none": None,
        }
    )
    assert decode_metadata_scalar_map(payload) == {
        "int": 42,
        "float": 3.5,
        "bool": True,
        "bytes": b"blob",
        "none": None,
    }


def test_metadata_codec_rejects_non_mapping_payloads() -> None:
    """Reject non-mapping payloads for metadata maps."""
    payload = msgspec.msgpack.encode(["not", "a", "mapping"])
    import pytest

    with pytest.raises(TypeError, match="metadata_map payload"):
        decode_metadata_map(payload)
