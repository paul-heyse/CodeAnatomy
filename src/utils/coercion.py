"""Canonical type coercion helpers."""

from __future__ import annotations

from collections.abc import Sequence

_TRUE_VALUES = frozenset({"true", "1", "yes", "on", "y"})
_FALSE_VALUES = frozenset({"false", "0", "no", "off", "n", ""})


def coerce_int(value: object, *, label: str = "value") -> int:
    """Coerce a value to ``int`` or raise ``TypeError``.

    Returns:
    -------
    int
        Coerced integer value.

    Raises:
        TypeError: If *value* cannot be coerced to ``int``.
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        if raw:
            try:
                return int(raw)
            except ValueError:
                pass
    msg = f"{label}: cannot coerce {type(value).__name__} to int"
    raise TypeError(msg)


def coerce_opt_int(value: object, *, label: str = "value") -> int | None:
    """Coerce to ``int`` when non-``None``, else return ``None``.

    Returns:
    -------
    int | None
        Coerced integer when provided; otherwise ``None``.
    """
    if value is None:
        return None
    return coerce_int(value, label=label)


def coerce_int_or_none(value: object) -> int | None:
    """Best-effort integer coercion that returns ``None`` when invalid.

    Returns:
    -------
    int | None
        Coerced integer when possible; otherwise ``None``.
    """
    try:
        return coerce_opt_int(value)
    except TypeError:
        return None


def coerce_bool(value: object, *, default: bool, label: str = "value") -> bool:
    """Coerce a value to ``bool`` with a default fallback for ``None``.

    Returns:
    -------
    bool
        Coerced boolean value.

    Raises:
        TypeError: If *value* cannot be coerced to ``bool``.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in _TRUE_VALUES:
            return True
        if lowered in _FALSE_VALUES:
            return False
    msg = f"{label}: cannot coerce {type(value).__name__} to bool"
    raise TypeError(msg)


def coerce_opt_str(value: object) -> str | None:
    """Return non-empty string value or ``None``.

    Returns:
    -------
    str | None
        Non-empty string value, or ``None``.
    """
    if isinstance(value, str) and value:
        return value
    return None


def coerce_str_tuple(value: object) -> tuple[str, ...]:
    """Coerce an iterable value to a tuple of stringified items.

    Returns:
    -------
    tuple[str, ...]
        Tuple containing normalized string items.
    """
    if value is None:
        return ()
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(str(item) for item in value if item is not None)
    return ()


__all__ = [
    "coerce_bool",
    "coerce_int",
    "coerce_int_or_none",
    "coerce_opt_int",
    "coerce_opt_str",
    "coerce_str_tuple",
]
