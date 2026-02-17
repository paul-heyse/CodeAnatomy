"""Serialization helpers for cache payloads."""

from __future__ import annotations

import msgspec

__all__ = ["decode_cache_payload", "encode_cache_payload"]


def encode_cache_payload(payload: object) -> bytes:
    """Encode arbitrary payload for cache storage.

    Returns:
        bytes: MessagePack-encoded payload bytes.
    """
    return msgspec.msgpack.encode(payload)


def decode_cache_payload(payload: bytes) -> object:
    """Decode cache payload bytes to Python object.

    Returns:
        object: Decoded payload value.
    """
    return msgspec.msgpack.decode(payload)
