"""Key/value parsing helpers for CLI options."""

from __future__ import annotations

import json
from typing import cast

from core_types import JsonValue


def parse_kv_pairs(values: tuple[str, ...]) -> dict[str, str]:
    """Parse key=value pairs from CLI repeatable options.

    Args:
        values: Description.

    Returns:
        dict[str, str]: Result.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    parsed: dict[str, str] = {}
    for item in values:
        key, sep, value = item.partition("=")
        if not sep or not key:
            msg = f"Expected key=value, got {item!r}."
            raise ValueError(msg)
        parsed[key] = value
    return parsed


def parse_kv_pairs_json(values: tuple[str, ...]) -> dict[str, JsonValue]:
    """Parse key=json pairs from CLI repeatable options.

    Args:
        values: Description.

    Returns:
        dict[str, JsonValue]: Result.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    parsed: dict[str, JsonValue] = {}
    for item in values:
        key, sep, value = item.partition("=")
        if not sep or not key:
            msg = f"Expected key=json, got {item!r}."
            raise ValueError(msg)
        parsed[key] = cast("JsonValue", json.loads(value))
    return parsed


__all__ = ["parse_kv_pairs", "parse_kv_pairs_json"]
