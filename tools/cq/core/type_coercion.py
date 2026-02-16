"""Shared type coercion utilities for CQ core."""

from __future__ import annotations


def coerce_float_strict(value: object) -> float:
    """Coerce value to float.

    Parameters
    ----------
    value : object
        Value to coerce.

    Returns:
    -------
    float
        Coerced float value.

    Raises:
        TypeError: If ``value`` is not an ``int`` or ``float``.
    """
    if isinstance(value, bool):
        raise TypeError
    if isinstance(value, (int, float)):
        return float(value)
    raise TypeError


def coerce_float_optional(value: object) -> float | None:
    """Coerce value to float or return ``None`` when unavailable.

    Returns:
    -------
    float | None
        Coerced float value or ``None``.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def coerce_str(value: object) -> str:
    """Coerce value to string.

    Parameters
    ----------
    value : object
        Value to coerce.

    Returns:
    -------
    str
        Coerced string value.

    Raises:
        TypeError: If ``value`` is not a string.
    """
    if isinstance(value, str):
        return value
    raise TypeError


__all__ = [
    "coerce_float_optional",
    "coerce_float_strict",
    "coerce_str",
]
