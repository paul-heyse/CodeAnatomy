"""Canonical payload normalization for deterministic hashing."""

from __future__ import annotations

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


__all__ = ["canonicalize_payload"]
