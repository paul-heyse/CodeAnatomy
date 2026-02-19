"""Value coercion utilities with tolerant and strict variants."""

from __future__ import annotations

from collections.abc import Mapping, Sequence


class CoercionError(ValueError):
    """Raised when strict coercion fails."""

    def __init__(self, value: object, target_type: str, reason: str | None = None) -> None:
        """__init__."""
        self.value = value
        self.target_type = target_type
        message = f"Cannot coerce {type(value).__name__} to {target_type}"
        if reason:
            message = f"{message}: {reason}"
        super().__init__(message)


def _coerce_int_error_message(value: object, *, label: str | None) -> str:
    resolved_label = label or "value"
    return f"{resolved_label}: cannot coerce {type(value).__name__} to int"


def _coerce_int_from_str(value: str, *, strict: bool, label: str | None) -> int | None:
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return int(stripped)
    except ValueError as exc:
        if strict:
            raise TypeError(_coerce_int_error_message(value, label=label)) from exc
        return None


def coerce_int(value: object, *, label: str | None = None) -> int | None:
    """Coerce value to int, returning None for unconvertible values.

    Returns:
    -------
    int | None
        Coerced integer or None if conversion fails.

    Raises:
        TypeError: If ``label`` is provided and ``value`` cannot be coerced to ``int``.
    """
    strict = label is not None
    converted: int | None = None
    if value is None:
        converted = None
    elif isinstance(value, bool):
        converted = int(value) if strict else None
    elif isinstance(value, int):
        converted = value
    elif isinstance(value, float):
        converted = int(value)
    elif isinstance(value, str):
        converted = _coerce_int_from_str(value, strict=strict, label=label)
    if converted is not None or not strict:
        return converted
    raise TypeError(_coerce_int_error_message(value, label=label))


def coerce_float(value: object) -> float | None:
    """Coerce value to float, returning None for unconvertible values.

    Returns:
    -------
    float | None
        Coerced float or None if conversion fails.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                return float(stripped)
            except ValueError:
                return None
    return None


def coerce_bool(
    value: object,
    *,
    default: bool | None = None,
    label: str = "value",
) -> bool | None:
    """Coerce value to bool, returning None for unconvertible values.

    Returns:
    -------
    bool | None
        Coerced bool or None if conversion fails.

    Raises:
        TypeError: If a non-coercible value is provided while ``default`` is set.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        lower = value.strip().lower()
        if lower in {"true", "1", "yes", "on", "y"}:
            return True
        if lower in {"false", "0", "no", "off", "n", ""}:
            return False
    if default is not None:
        msg = f"{label}: cannot coerce {type(value).__name__} to bool"
        raise TypeError(msg)
    return None


def coerce_str(value: object) -> str | None:
    """Coerce value to string, returning None for None.

    Returns:
    -------
    str | None
        Coerced string or None if value is None.
    """
    if value is None:
        return None
    return str(value)


def coerce_str_list(value: object) -> list[str]:
    """Coerce value to list of non-empty strings.

    Returns:
    -------
    list[str]
        List of non-empty strings.
    """
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [str(item) for item in value if str(item).strip()]
    return []


def coerce_str_tuple(value: object) -> tuple[str, ...]:
    """Coerce value to tuple of non-empty strings.

    Returns:
    -------
    tuple[str, ...]
        Tuple of non-empty strings.
    """
    return tuple(coerce_str_list(value))


def coerce_opt_int(value: object, *, label: str = "value") -> int | None:
    """Coerce to int when value is non-None, else return None.

    Returns:
    -------
    int | None
        Coerced integer or ``None`` when ``value`` is ``None``.
    """
    if value is None:
        return None
    return coerce_int(value, label=label)


def coerce_int_or_none(value: object) -> int | None:
    """Best-effort integer coercion that returns ``None`` when invalid.

    Returns:
    -------
    int | None
        Coerced integer when valid, otherwise ``None``.
    """
    try:
        return coerce_opt_int(value)
    except TypeError:
        return None


def coerce_opt_str(value: object) -> str | None:
    """Return non-empty string values, or None."""
    if isinstance(value, str) and value:
        return value
    return None


def coerce_mapping_list(value: object) -> Sequence[Mapping[str, object]] | None:
    """Coerce value to sequence of mappings.

    Returns:
    -------
    Sequence[Mapping[str, object]] | None
        Sequence of mappings or None if conversion fails.
    """
    if value is None:
        return None
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [dict(entry) for entry in value if isinstance(entry, Mapping)]
    return None


def raise_for_int(value: object, *, context: str = "") -> int:
    """Coerce value to int, raising CoercionError on failure.

    Args:
        value: Value to coerce.
        context: Optional coercion context label.

    Returns:
        int: Result.

    Raises:
        CoercionError: If coercion to int fails.
    """
    result = coerce_int(value)
    if result is None:
        raise CoercionError(value, "int", context or None)
    return result


def raise_for_float(value: object, *, context: str = "") -> float:
    """Coerce value to float, raising CoercionError on failure.

    Args:
        value: Value to coerce.
        context: Optional coercion context label.

    Returns:
        float: Result.

    Raises:
        CoercionError: If coercion to float fails.
    """
    result = coerce_float(value)
    if result is None:
        raise CoercionError(value, "float", context or None)
    return result


def raise_for_bool(value: object, *, context: str = "") -> bool:
    """Coerce value to bool, raising CoercionError on failure.

    Args:
        value: Value to coerce.
        context: Optional coercion context label.

    Returns:
        bool: Result.

    Raises:
        CoercionError: If coercion to bool fails.
    """
    result = coerce_bool(value)
    if result is None:
        raise CoercionError(value, "bool", context or None)
    return result


def raise_for_str(value: object, *, context: str = "") -> str:
    """Coerce value to str, raising CoercionError when value is None.

    Args:
        value: Value to coerce.
        context: Optional coercion context label.

    Returns:
        str: Result.

    Raises:
        CoercionError: If coercion to str fails.
    """
    if value is None:
        raise CoercionError(value, "str", context or "value is None")
    return str(value)


__all__ = [
    "CoercionError",
    "coerce_bool",
    "coerce_float",
    "coerce_int",
    "coerce_int_or_none",
    "coerce_mapping_list",
    "coerce_opt_int",
    "coerce_opt_str",
    "coerce_str",
    "coerce_str_list",
    "coerce_str_tuple",
    "raise_for_bool",
    "raise_for_float",
    "raise_for_int",
    "raise_for_str",
]
