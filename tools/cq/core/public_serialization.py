"""Public serialization helpers for CQ msgspec contracts."""

from __future__ import annotations

from collections.abc import Iterable

import msgspec

from tools.cq.core.contract_codec import to_public_dict as _to_public_dict
from tools.cq.core.contract_codec import to_public_list as _to_public_list


def to_public_dict(value: msgspec.Struct) -> dict[str, object]:
    """Serialize a msgspec struct to a builtins mapping.

    Returns:
        Builtins dictionary representation of the struct.

    Raises:
        TypeError: If the serialized payload is not a dictionary.
    """
    return _to_public_dict(value)


def to_public_list(values: Iterable[msgspec.Struct]) -> list[dict[str, object]]:
    """Serialize msgspec struct iterable to list of builtins mappings.

    Returns:
        List of builtins dictionaries.
    """
    return _to_public_list(values)


__all__ = ["to_public_dict", "to_public_list"]
