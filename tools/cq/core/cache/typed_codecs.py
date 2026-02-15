"""Shared typed cache codecs for msgpack and mapping payload boundaries."""

from __future__ import annotations

from functools import lru_cache

import msgspec

from tools.cq.core.typed_boundary import BoundaryDecodeError, convert_lax

_MSGPACK_ENCODER = msgspec.msgpack.Encoder()


@lru_cache(maxsize=64)
def _decoder[T](type_: type[T]) -> msgspec.msgpack.Decoder[T]:
    return msgspec.msgpack.Decoder(type=type_)


def decode_msgpack_typed[T](payload: object, *, type_: type[T]) -> T | None:
    """Decode msgpack payload into a typed struct or container.

    Returns:
        Decoded typed payload when possible, otherwise ``None``.
    """
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        return None
    try:
        return _decoder(type_).decode(payload)
    except (RuntimeError, TypeError, ValueError):
        return None


def convert_mapping_typed[T](payload: object, *, type_: type[T]) -> T | None:
    """Convert mapping-like payload into a typed contract.

    Returns:
        Converted typed payload when possible, otherwise ``None``.
    """
    try:
        return convert_lax(payload, type_=type_)
    except BoundaryDecodeError:
        return None


def encode_msgpack_payload(payload: object) -> bytes | object:
    """Encode payload to msgpack, falling back to builtins on failure.

    Returns:
        Encoded bytes on success, or builtins payload fallback on failure.
    """
    try:
        return _MSGPACK_ENCODER.encode(payload)
    except (RuntimeError, TypeError, ValueError):
        return msgspec.to_builtins(payload)


def encode_msgpack_into(payload: object, *, buffer: bytearray) -> int:
    """Encode payload into a provided buffer.

    Returns:
        Number of bytes appended to ``buffer``.
    """
    scratch = bytearray()
    _MSGPACK_ENCODER.encode_into(payload, scratch)
    buffer.extend(scratch)
    return len(scratch)


__all__ = [
    "convert_mapping_typed",
    "decode_msgpack_typed",
    "encode_msgpack_into",
    "encode_msgpack_payload",
]
