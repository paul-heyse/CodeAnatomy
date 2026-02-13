"""Public serialization helpers for CQ msgspec contracts."""

from __future__ import annotations

from collections.abc import Iterable
from typing import cast

import msgspec


def to_public_dict(value: msgspec.Struct) -> dict[str, object]:
    """Serialize a msgspec struct to a builtins mapping.

    Returns:
        Builtins dictionary representation of the struct.

    Raises:
        TypeError: If the serialized payload is not a dictionary.
    """
    payload = msgspec.to_builtins(
        value,
        builtin_types=(str, int, float, bool, type(None), list, dict),
    )
    if isinstance(payload, dict):
        return cast("dict[str, object]", payload)
    msg = f"Expected dict payload, got {type(payload).__name__}"
    raise TypeError(msg)


def to_public_list(values: Iterable[msgspec.Struct]) -> list[dict[str, object]]:
    """Serialize msgspec struct iterable to list of builtins mappings.

    Returns:
        List of builtins dictionaries.
    """
    return [to_public_dict(value) for value in values]


__all__ = ["to_public_dict", "to_public_list"]
