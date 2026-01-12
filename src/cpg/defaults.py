"""Default-filling helpers for CPG builders."""

from __future__ import annotations

from arrowdsl.core.interop import ArrayLike, ChunkedArrayLike, pc

type ValuesLike = ArrayLike | ChunkedArrayLike


def fill_nulls(values: ValuesLike, *, default: object) -> ValuesLike:
    """Fill null values with a default.

    Returns
    -------
    ValuesLike
        Values with nulls replaced by the default.
    """
    if values.null_count == 0:
        return values
    return pc.fill_null(values, fill_value=default)


def fill_nulls_float(values: ValuesLike, *, default: float) -> ValuesLike:
    """Fill null float values with a default.

    Returns
    -------
    ValuesLike
        Values with nulls replaced by the default.
    """
    return fill_nulls(values, default=default)


def fill_nulls_string(values: ValuesLike, *, default: str) -> ValuesLike:
    """Fill null string values with a default.

    Returns
    -------
    ValuesLike
        Values with nulls replaced by the default.
    """
    return fill_nulls(values, default=default)


__all__ = ["ValuesLike", "fill_nulls", "fill_nulls_float", "fill_nulls_string"]
