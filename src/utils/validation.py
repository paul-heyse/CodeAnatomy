"""Type, instance, and collection validation utilities."""

from __future__ import annotations

from collections.abc import Callable, Container, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    import pyarrow as pa


T = TypeVar("T")


def ensure_mapping(
    value: object,
    *,
    label: str,
    error_type: type[Exception] = TypeError,
) -> Mapping[str, object]:
    """Validate that value is a Mapping.

    Parameters
    ----------
    value
        Value to validate.
    label
        Descriptive label for error messages.
    error_type
        Exception type to raise on validation failure.

    Returns
    -------
    Mapping[str, object]
        The validated mapping.
    """
    if not isinstance(value, Mapping):
        msg = f"{label} must be a Mapping, got {type(value).__name__}"
        raise error_type(msg)
    return value


def ensure_sequence(
    value: object,
    *,
    label: str,
    item_type: type[object] | tuple[type[object], ...] | None = None,
) -> Sequence[object]:
    """Validate that value is a Sequence.

    Parameters
    ----------
    value
        Value to validate.
    label
        Descriptive label for error messages.
    item_type
        Optional type to validate sequence items against.

    Returns
    -------
    Sequence[object]
        The validated sequence.

    Raises
    ------
    TypeError
        If value is not a Sequence or items don't match item_type.
    """
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        msg = f"{label} must be a Sequence, got {type(value).__name__}"
        raise TypeError(msg)
    if item_type is not None:
        for index, item in enumerate(value):
            if not isinstance(item, item_type):
                type_name = (
                    item_type.__name__
                    if isinstance(item_type, type)
                    else " | ".join(t.__name__ for t in item_type)
                )
                msg = f"{label}[{index}] must be {type_name}, got {type(item).__name__}"
                raise TypeError(msg)
    return value


def ensure_callable(
    value: object,
    *,
    label: str,
) -> Callable[..., object]:
    """Validate that value is callable.

    Parameters
    ----------
    value
        Value to validate.
    label
        Descriptive label for error messages.

    Returns
    -------
    Callable[..., object]
        The validated callable.

    Raises
    ------
    TypeError
        If value is not callable.
    """
    if not callable(value):
        msg = f"{label} must be callable, got {type(value).__name__}"
        raise TypeError(msg)
    return value


def ensure_table(value: object, *, label: str = "input") -> pa.Table:
    """Convert table-like input into a PyArrow Table.

    Parameters
    ----------
    value
        Arrow-like input to convert.
    label
        Descriptive label for error messages.

    Returns
    -------
    pa.Table
        Converted table.

    Raises
    ------
    TypeError
        Raised when conversion fails.
    """
    from datafusion_engine.arrow_schema.coercion import to_arrow_table

    try:
        return to_arrow_table(value)
    except TypeError as exc:
        msg = f"{label} must be Table/RecordBatch/RecordBatchReader, got {type(value).__name__}"
        raise TypeError(msg) from exc


def ensure_not_empty[T](
    value: Sequence[T],
    *,
    label: str = "value",
) -> Sequence[T]:
    """Ensure sequence is not empty.

    Parameters
    ----------
    value
        Sequence to validate.
    label
        Label for error messages.

    Returns
    -------
    Sequence[T]
        The validated sequence.

    Raises
    ------
    ValueError
        Raised when sequence is empty.
    """
    if not value:
        msg = f"{label} must not be empty"
        raise ValueError(msg)
    return value


def ensure_subset(
    items: Iterable[T],
    universe: Container[T],
    *,
    label: str = "items",
) -> None:
    """Ensure all items are in the universe.

    Parameters
    ----------
    items
        Items to validate.
    universe
        Container of allowed values.
    label
        Label for error messages.

    Raises
    ------
    ValueError
        Raised when items contain invalid values.
    """
    extra = [item for item in items if item not in universe]
    if extra:
        msg = f"{label} contains invalid values: {extra}"
        raise ValueError(msg)


def ensure_unique[T](
    items: Iterable[T],
    *,
    label: str = "items",
) -> list[T]:
    """Ensure all items are unique.

    Parameters
    ----------
    items
        Items to validate.
    label
        Label for error messages.

    Returns
    -------
    list[T]
        Deduplicated list of items.

    Raises
    ------
    ValueError
        Raised when duplicates are present.
    """
    seen: set[T] = set()
    duplicates: list[T] = []
    result: list[T] = []
    for item in items:
        if item in seen:
            duplicates.append(item)
        else:
            seen.add(item)
            result.append(item)
    if duplicates:
        msg = f"{label} contains duplicates: {duplicates}"
        raise ValueError(msg)
    return result


def find_missing(required: Iterable[T], available: Container[T]) -> list[T]:
    """Find items in required that are not in available.

    Parameters
    ----------
    required
        Items that should be present.
    available
        Container to check against.

    Returns
    -------
    list[T]
        List of missing items (empty if all present).
    """
    return [item for item in required if item not in available]


def validate_required_items(
    required: Iterable[T],
    available: Container[T],
    *,
    item_label: str = "items",
    error_type: type[Exception] = ValueError,
) -> None:
    """Validate that all required items are available.

    Parameters
    ----------
    required
        Items that must be present.
    available
        Container to check against.
    item_label
        Label for items in error messages.
    error_type
        Exception type to raise on validation failure.
    """
    missing = find_missing(required, available)
    if missing:
        msg = f"Missing required {item_label}: {missing}"
        raise error_type(msg)


__all__ = [
    "ensure_callable",
    "ensure_mapping",
    "ensure_not_empty",
    "ensure_sequence",
    "ensure_subset",
    "ensure_table",
    "ensure_unique",
    "find_missing",
    "validate_required_items",
]
