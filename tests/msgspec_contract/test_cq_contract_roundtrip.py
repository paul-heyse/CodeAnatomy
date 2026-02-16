"""Roundtrip tests for canonical CQ contract codec."""

from __future__ import annotations

from tools.cq.core.contract_codec import (
    decode_json,
    decode_msgpack,
    encode_json,
    encode_msgpack,
)


def test_contract_codec_json_roundtrip() -> None:
    """Test contract codec json roundtrip."""
    payload = {"query": "foo", "count": 2}
    decoded = decode_json(encode_json(payload, indent=None))
    assert decoded == payload


def test_contract_codec_msgpack_roundtrip() -> None:
    """Test contract codec msgpack roundtrip."""
    payload = {"query": "foo", "count": 2}
    decoded = decode_msgpack(encode_msgpack(payload))
    assert decoded == payload
