"""Tests for shared typed cache codecs."""

from __future__ import annotations

from tools.cq.core.cache.typed_codecs import (
    convert_mapping_typed,
    decode_msgpack_typed,
    encode_msgpack_payload,
)
from tools.cq.core.structs import CqStruct


class _CodecPayload(CqStruct, frozen=True):
    value: int


def test_decode_msgpack_typed_roundtrip() -> None:
    encoded = encode_msgpack_payload(_CodecPayload(value=7))
    assert isinstance(encoded, bytes)
    decoded = decode_msgpack_typed(encoded, type_=_CodecPayload)
    assert decoded == _CodecPayload(value=7)


def test_convert_mapping_typed_handles_mapping_input() -> None:
    decoded = convert_mapping_typed({"value": 9}, type_=_CodecPayload)
    assert decoded == _CodecPayload(value=9)
