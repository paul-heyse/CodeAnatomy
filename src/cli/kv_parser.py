"""Key/value parsing helpers for CLI options."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from core_types import JsonValue


def parse_kv_pairs(values: tuple[str, ...]) -> dict[str, str]:
    """Parse key=value pairs from CLI repeatable options.

    Parameters
    ----------
    values
        Tuple of key=value strings.

    Returns
    -------
    dict[str, str]
        Parsed key/value mapping.

    Raises
    ------
    ValueError
        Raised when a value is not formatted as key=value.
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

    Parameters
    ----------
    values
        Tuple of key=json strings.

    Returns
    -------
    dict[str, object]
        Parsed key/value mapping with JSON values.

    Raises
    ------
    ValueError
        Raised when a value is not formatted as key=json.
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
