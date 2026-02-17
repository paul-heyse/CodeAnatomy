"""Deterministic serialization for front-door insight contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, cast

import msgspec

if TYPE_CHECKING:
    from tools.cq.core.front_door_assembly import FrontDoorInsightV1


def _to_full_builtins(value: object) -> object:
    if isinstance(value, msgspec.Struct):
        field_names = type(value).__struct_fields__
        return {
            field_name: _to_full_builtins(getattr(value, field_name)) for field_name in field_names
        }
    if isinstance(value, Mapping):
        return {
            str(key): _to_full_builtins(mapped_value)
            for key, mapped_value in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_to_full_builtins(item) for item in value]
    return msgspec.to_builtins(value, order="deterministic", str_keys=True)


def to_public_front_door_insight_dict(insight: FrontDoorInsightV1) -> dict[str, object]:
    """Convert front-door insight contract to deterministic builtins mapping.

    Returns:
    -------
    dict[str, object]
        Deterministic JSON-serializable mapping.

    Raises:
        TypeError: If ``msgspec.to_builtins`` returns a non-dictionary payload.
    """
    payload = _to_full_builtins(insight)
    if isinstance(payload, dict):
        return cast("dict[str, object]", payload)
    msg = f"Expected dict payload, got {type(payload).__name__}"
    raise TypeError(msg)


__all__ = ["to_public_front_door_insight_dict"]
