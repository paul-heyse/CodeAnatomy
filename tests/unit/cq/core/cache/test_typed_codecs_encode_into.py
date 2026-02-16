"""Tests for encode-into cache codec behavior."""

from __future__ import annotations

from tools.cq.core.cache.typed_codecs import encode_msgpack_into
from tools.cq.core.structs import CqStruct


class _EncodeIntoPayload(CqStruct, frozen=True):
    text: str


def test_encode_msgpack_into_appends_bytes() -> None:
    """Test encode msgpack into appends bytes."""
    buffer = bytearray(b"prefix")
    written = encode_msgpack_into(_EncodeIntoPayload(text="ok"), buffer=buffer)
    assert written > 0
    assert buffer.startswith(b"prefix")
