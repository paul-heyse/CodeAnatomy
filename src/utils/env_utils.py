"""Unified environment variable resolution utilities."""

from __future__ import annotations

import logging
import os
from enum import Enum
from typing import Literal, overload

_LOGGER = logging.getLogger(__name__)

OnInvalid = Literal["default", "none", "false"]

_TRUE_VALUES = frozenset({"1", "true", "yes", "y"})
_FALSE_VALUES = frozenset({"0", "false", "no", "n"})
# -----------------------------------------------------------------------------
# String Helpers
# -----------------------------------------------------------------------------


def env_value(name: str) -> str | None:
    """Return stripped env var value, or None if empty/not set.

    Parameters
    ----------
    name
        Environment variable name.

    Returns
    -------
    str | None
        Stripped value or None.
    """
    raw = os.environ.get(name)
    if raw is None:
        return None
    stripped = raw.strip()
    return stripped if stripped else None


def env_text(
    name: str,
    *,
    default: str | None = None,
    strip: bool = True,
    allow_empty: bool = False,
) -> str | None:
    """Return an environment variable string with optional normalization.

    Parameters
    ----------
    name
        Environment variable name.
    default
        Default value if not set or empty (unless allow_empty is True).
    strip
        Whether to strip whitespace from the value.
    allow_empty
        Whether to return empty strings instead of the default.

    Returns
    -------
    str | None
        Parsed value, or default/None when missing.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip() if strip else raw
    if not value and not allow_empty:
        return default
    return value


# -----------------------------------------------------------------------------
# List Parsing
# -----------------------------------------------------------------------------


def env_list(
    name: str,
    *,
    default: list[str] | None = None,
    separator: str = ",",
    strip: bool = True,
) -> list[str]:
    """Parse environment variable as list of strings.

    Parameters
    ----------
    name
        Environment variable name.
    default
        Default value if not set.
    separator
        Separator character (default: comma).
    strip
        Whether to strip whitespace from each item.

    Returns
    -------
    list[str]
        Parsed list or default.
    """
    raw = env_value(name)
    if raw is None:
        return default or []
    items = raw.split(separator)
    if not strip:
        return [item for item in items if item]
    return [item.strip() for item in items if item.strip()]


# -----------------------------------------------------------------------------
# Enum Parsing
# -----------------------------------------------------------------------------


@overload
def env_enum[TEnum: Enum](name: str, enum_type: type[TEnum]) -> TEnum | None: ...


@overload
def env_enum[TEnum: Enum](name: str, enum_type: type[TEnum], *, default: TEnum) -> TEnum: ...


@overload
def env_enum[TEnum: Enum](
    name: str,
    enum_type: type[TEnum],
    *,
    default: TEnum | None,
) -> TEnum | None: ...


def env_enum[TEnum: Enum](
    name: str,
    enum_type: type[TEnum],
    *,
    default: TEnum | None = None,
) -> TEnum | None:
    """Parse environment variable as enum value.

    Parameters
    ----------
    name
        Environment variable name.
    enum_type
        Enum class to convert to.
    default
        Default value if not set or invalid.

    Returns
    -------
    TEnum | None
        Parsed enum value or default.
    """
    raw = env_value(name)
    if raw is None:
        return default
    value = raw.strip()
    value_lower = value.lower()
    try:
        return enum_type(value_lower)
    except (ValueError, KeyError, TypeError):
        for member in enum_type:
            if member.name.lower() == value_lower:
                return member
            if isinstance(member.value, str) and member.value.lower() == value_lower:
                return member
        return default


# -----------------------------------------------------------------------------
# Boolean Parsing
# -----------------------------------------------------------------------------


@overload
def env_bool(name: str) -> bool | None: ...


@overload
def env_bool(name: str, *, default: bool) -> bool: ...


@overload
def env_bool(
    name: str,
    *,
    default: bool,
    on_invalid: OnInvalid,
    log_invalid: bool = False,
) -> bool: ...


@overload
def env_bool(
    name: str,
    *,
    default: bool | None,
    on_invalid: OnInvalid,
    log_invalid: bool = False,
) -> bool | None: ...


def env_bool(
    name: str,
    *,
    default: bool | None = None,
    on_invalid: OnInvalid = "default",
    log_invalid: bool = False,
) -> bool | None:
    """Parse environment variable as boolean.

    Parameters
    ----------
    name
        Environment variable name.
    default
        Default value if not set. If None, returns None when unset.
    on_invalid
        Behavior when an invalid value is provided.
    log_invalid
        Whether to log invalid values.

    Returns
    -------
    bool | None
        Parsed boolean or default/None.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    if log_invalid:
        _LOGGER.warning("Invalid boolean for %s: %r", name, raw)
    if on_invalid == "none":
        return None
    if on_invalid == "false":
        return False
    return default


# -----------------------------------------------------------------------------
# Strict Boolean Parsing
# -----------------------------------------------------------------------------


def env_bool_strict(name: str, *, default: bool, log_invalid: bool = True) -> bool:
    """Parse environment variable as strict boolean ('true'/'false' only).

    Parameters
    ----------
    name
        Environment variable name.
    default
        Default value if not set or invalid.
    log_invalid
        Whether to log invalid values.

    Returns
    -------
    bool
        Parsed boolean or default.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if not value:
        return default
    if value == "true":
        return True
    if value == "false":
        return False
    if log_invalid:
        _LOGGER.warning("Invalid boolean for %s: %r", name, raw)
    return default


# -----------------------------------------------------------------------------
# Integer Parsing
# -----------------------------------------------------------------------------


@overload
def env_int(name: str) -> int | None: ...


@overload
def env_int(name: str, *, default: int) -> int: ...


@overload
def env_int(name: str, *, default: int | None) -> int | None: ...


def env_int(name: str, *, default: int | None = None) -> int | None:
    """Parse environment variable as integer with error logging.

    Parameters
    ----------
    name
        Environment variable name.
    default
        Default value if not set or invalid.

    Returns
    -------
    int | None
        Parsed integer or default/None.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        _LOGGER.warning("Invalid integer for %s: %r", name, raw)
        return default


# -----------------------------------------------------------------------------
# Float Parsing
# -----------------------------------------------------------------------------


@overload
def env_float(name: str) -> float | None: ...


@overload
def env_float(name: str, *, default: float) -> float: ...


def env_float(name: str, *, default: float | None = None) -> float | None:
    """Parse environment variable as float with error logging.

    Parameters
    ----------
    name
        Environment variable name.
    default
        Default value if not set or invalid.

    Returns
    -------
    float | None
        Parsed float or default/None.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw.strip())
    except ValueError:
        _LOGGER.warning("Invalid float for %s: %r", name, raw)
        return default


def env_truthy(value: str | None) -> bool:
    """Return True when the raw env value is 'true' (case-insensitive).

    Returns
    -------
    bool
        True when the value is the literal "true".
    """
    return value is not None and value.strip().lower() == "true"


__all__ = [
    "env_bool",
    "env_bool_strict",
    "env_enum",
    "env_float",
    "env_int",
    "env_list",
    "env_truthy",
    "env_value",
]
