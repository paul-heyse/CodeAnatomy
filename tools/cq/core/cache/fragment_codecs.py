"""Reusable cache payload codecs for fragment-oriented storage."""

from __future__ import annotations

from typing import TypeVar

import msgspec

T = TypeVar("T")


def decode_fragment_payload(payload: object, *, type_: type[T]) -> T | None:
    """Decode cached payload into target msgspec contract type.

    Returns:
        T | None: Decoded payload on success, otherwise ``None``.
    """
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
    return msgspec.to_builtins(payload)


__all__ = [
    "decode_fragment_payload",
    "encode_fragment_payload",
]
