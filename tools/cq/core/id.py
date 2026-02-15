"""Deterministic identity helpers for CQ contracts and cache keys."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence

import msgspec


def canonicalize_payload(value: object) -> object:
    """Return a deterministic built-in representation of a payload value.

    Returns:
        object: Canonicalized payload representation.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        normalized: object = value
    elif isinstance(value, Mapping):
        items = sorted((str(key), canonicalize_payload(val)) for key, val in value.items())
        normalized = dict(items)
    elif isinstance(value, (set, frozenset)):
        normalized = [canonicalize_payload(item) for item in value]
        normalized = sorted(normalized, key=msgspec.json.encode)
    elif isinstance(value, tuple):
        normalized = tuple(canonicalize_payload(item) for item in value)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        normalized = [canonicalize_payload(item) for item in value]
    else:
        try:
            builtins = msgspec.to_builtins(value)
        except (RuntimeError, TypeError, ValueError):
            normalized = (
                canonicalize_payload(vars(value)) if hasattr(value, "__dict__") else repr(value)
            )
        else:
            normalized = canonicalize_payload(builtins)
    return normalized


def stable_digest(value: object) -> str:
    """Return SHA-256 digest of canonicalized payload value.

    Returns:
        str: Full hex SHA-256 digest.
    """
    canonical = canonicalize_payload(value)
    return hashlib.sha256(msgspec.json.encode(canonical)).hexdigest()


def stable_digest24(value: object) -> str:
    """Return 24-char stable digest for compact deterministic IDs.

    Returns:
        str: 24-character deterministic digest prefix.
    """
    return stable_digest(value)[:24]


__all__ = ["canonicalize_payload", "stable_digest", "stable_digest24"]
