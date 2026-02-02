"""Validators for cq CLI parameters."""

from __future__ import annotations

from pathlib import Path


def validate_path_exists(path: Path, *, name: str = "path") -> Path:
    """Validate that a path exists.

    Parameters
    ----------
    path
        Path to validate.
    name
        Parameter name for error message.

    Returns
    -------
    Path
        Validated path.

    Raises
    ------
    ValueError
        If path does not exist.
    """
    if not path.exists():
        msg = f"{name} does not exist: {path}"
        raise ValueError(msg)
    return path


def validate_positive_int(value: int, *, name: str = "value") -> int:
    """Validate that an integer is positive.

    Parameters
    ----------
    value
        Value to validate.
    name
        Parameter name for error message.

    Returns
    -------
    int
        Validated value.

    Raises
    ------
    ValueError
        If value is not positive.
    """
    if value <= 0:
        msg = f"{name} must be positive, got: {value}"
        raise ValueError(msg)
    return value


def validate_target_spec(value: str) -> tuple[str, str]:
    """Validate and parse a target spec string.

    Parameters
    ----------
    value
        Target spec string like 'function:foo'.

    Returns
    -------
    tuple[str, str]
        Tuple of (kind, target_value).

    Raises
    ------
    ValueError
        If format is invalid.
    """
    if ":" not in value:
        msg = "Target spec must be in the form kind:value (e.g., function:foo)"
        raise ValueError(msg)

    kind, target_value = value.split(":", maxsplit=1)
    kind = kind.strip().lower()
    target_value = target_value.strip()

    valid_kinds = {"function", "class", "method", "module", "path"}
    if kind not in valid_kinds:
        msg = f"Invalid target kind: {kind}. Must be one of: {', '.join(sorted(valid_kinds))}"
        raise ValueError(msg)

    if not target_value:
        msg = "Target value cannot be empty"
        raise ValueError(msg)

    return kind, target_value
