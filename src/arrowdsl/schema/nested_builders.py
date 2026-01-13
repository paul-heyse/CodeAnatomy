"""Nested array builders with type-aware fallbacks."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import pyarrow as pa

from arrowdsl.core.interop import ArrayLike, DataTypeLike


def _safe_array(values: Sequence[object | None], *, dtype: DataTypeLike) -> ArrayLike:
    try:
        return pa.array(values, type=dtype)
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError):
        coerced: list[object | None] = []
        for value in values:
            if value is None:
                coerced.append(None)
                continue
            try:
                _ = pa.scalar(value, type=dtype)
            except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError):
                coerced.append(None)
            else:
                coerced.append(value)
        return pa.array(coerced, type=dtype)


def list_view_array_from_lists(
    values: Sequence[object | None],
    *,
    value_type: DataTypeLike,
    large: bool = True,
) -> ArrayLike:
    """Build a list_view array from Python list values.

    Returns
    -------
    ArrayLike
        List view array with the requested element type.
    """
    normalized: list[list[object] | None] = []
    for value in values:
        if value is None:
            normalized.append(None)
        elif isinstance(value, (list, tuple)):
            normalized.append(list(value))
        else:
            normalized.append(None)
    list_type = pa.large_list_view(value_type) if large else pa.list_view(value_type)
    return pa.array(normalized, type=list_type)


def map_array_from_pairs(
    values: Sequence[object | None],
    *,
    key_type: DataTypeLike,
    item_type: DataTypeLike,
    keys_sorted: bool | None = None,
) -> ArrayLike:
    """Build a map array from key/value pair sequences.

    Returns
    -------
    ArrayLike
        Map array with the provided key/value types.
    """
    normalized: list[list[tuple[object, object]] | None] = []
    for value in values:
        if value is None:
            normalized.append(None)
        elif isinstance(value, Mapping):
            normalized.append(list(value.items()))
        elif isinstance(value, (list, tuple)):
            normalized.append(list(value))
        else:
            normalized.append(None)
    map_type = (
        pa.map_(key_type, item_type, keys_sorted=keys_sorted)
        if keys_sorted is not None
        else pa.map_(key_type, item_type)
    )
    return pa.array(normalized, type=map_type)


def struct_array_from_dicts(
    values: Sequence[object | None],
    *,
    struct_type: DataTypeLike | None = None,
) -> ArrayLike:
    """Build a struct array from mapping values.

    Returns
    -------
    ArrayLike
        Struct array with inferred or explicit type.
    """
    normalized: list[Mapping[str, object] | None] = []
    for value in values:
        if value is None:
            normalized.append(None)
        elif isinstance(value, Mapping):
            normalized.append(value)
        else:
            normalized.append(None)
    return pa.array(normalized, type=struct_type)


def union_array_from_values(
    values: Sequence[object | None],
    *,
    union_type: DataTypeLike,
) -> ArrayLike:
    """Build a union array with fallback for malformed values.

    Returns
    -------
    ArrayLike
        Union array with malformed values coerced to nulls.
    """
    return _safe_array(values, dtype=union_type)


def dictionary_array_from_values(
    values: Sequence[object | None],
    *,
    dictionary_type: DataTypeLike,
) -> ArrayLike:
    """Build a dictionary array with fallback for malformed values.

    Returns
    -------
    ArrayLike
        Dictionary array with malformed values coerced to nulls.
    """
    return _safe_array(values, dtype=dictionary_type)


__all__ = [
    "dictionary_array_from_values",
    "list_view_array_from_lists",
    "map_array_from_pairs",
    "struct_array_from_dicts",
    "union_array_from_values",
]
