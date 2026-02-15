"""Reusable cache payload codecs for fragment-oriented storage."""

from __future__ import annotations

from functools import lru_cache

import msgspec

_MSGPACK_ENCODER = msgspec.msgpack.Encoder()


def is_fragment_cache_payload(payload: object) -> bool:
    """Return whether payload looks like a supported cache artifact encoding."""
    return isinstance(payload, (dict, bytes, bytearray, memoryview))


@lru_cache(maxsize=32)
def _msgpack_decoder[T](*, type_: type[T]) -> msgspec.msgpack.Decoder[T]:
    return msgspec.msgpack.Decoder(type=type_)


def decode_fragment_payload[T](payload: object, *, type_: type[T]) -> T | None:
    """Decode cached payload into target msgspec contract type.

    Returns:
        T | None: Decoded payload on success, otherwise ``None``.
    """
    if isinstance(payload, (bytes, bytearray, memoryview)):
        try:
            return _msgpack_decoder(type_=type_).decode(payload)
        except (RuntimeError, TypeError, ValueError):
            return None
    if not isinstance(payload, dict):
        return None
    try:
        return msgspec.convert(payload, type=type_)
    except (RuntimeError, TypeError, ValueError):
        return None


def encode_fragment_payload(payload: object) -> object:
    """Encode payload into cache-safe built-ins.

    Returns:
        object: msgspec-compatible built-in value.
    """
    try:
        return _MSGPACK_ENCODER.encode(payload)
    except (RuntimeError, TypeError, ValueError):
        return msgspec.to_builtins(payload)


__all__ = [
    "decode_fragment_payload",
    "encode_fragment_payload",
    "is_fragment_cache_payload",
]
