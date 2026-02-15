"""Reusable cache payload codecs for fragment-oriented storage."""

from __future__ import annotations

from tools.cq.core.cache.typed_codecs import (
    convert_mapping_typed,
    decode_msgpack_typed,
    encode_msgpack_payload,
)


def is_fragment_cache_payload(payload: object) -> bool:
    """Return whether payload looks like a supported cache artifact encoding."""
    return isinstance(payload, (dict, bytes, bytearray, memoryview))


def decode_fragment_payload[T](payload: object, *, type_: type[T]) -> T | None:
    """Decode cached payload into target msgspec contract type.

    Returns:
        T | None: Decoded payload on success, otherwise ``None``.
    """
    decoded = decode_msgpack_typed(payload, type_=type_)
    if decoded is not None:
        return decoded
    if not isinstance(payload, dict):
        return None
    return convert_mapping_typed(payload, type_=type_)


def encode_fragment_payload(payload: object) -> object:
    """Encode payload into cache-safe built-ins.

    Returns:
        object: msgspec-compatible built-in value.
    """
    return encode_msgpack_payload(payload)


__all__ = [
    "decode_fragment_payload",
    "encode_fragment_payload",
    "is_fragment_cache_payload",
]
