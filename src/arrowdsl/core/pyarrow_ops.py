"""PyArrow compute helpers used outside DataFusion execution paths."""

from __future__ import annotations

from typing import cast

from arrowdsl.core.interop import ArrayLike, ChunkedArrayLike, TableLike, pc


def distinct_sorted(values: ArrayLike | ChunkedArrayLike) -> ArrayLike:
    """Return distinct values sorted ascending.

    Returns
    -------
    ArrayLike
        Sorted unique values.

    Raises
    ------
    RuntimeError
        Raised when required pyarrow.compute helpers are unavailable.
    """
    unique_fn = getattr(pc, "unique", None)
    if unique_fn is None:
        msg = "pyarrow.compute.unique is unavailable."
        raise RuntimeError(msg)
    unique = cast("ArrayLike", unique_fn(values))
    sort_options = getattr(pc, "SortOptions", None)
    if sort_options is None:
        msg = "pyarrow.compute.SortOptions is unavailable."
        raise RuntimeError(msg)
    options = sort_options(null_placement="at_end")
    indices = pc.sort_indices(unique, options=options)
    return pc.take(unique, indices)


def flatten_list_struct_field(
    table: TableLike,
    *,
    list_col: str,
    field: str,
) -> ArrayLike:
    """Flatten a list<struct> column and return a field array.

    Returns
    -------
    ArrayLike
        Flattened field values.
    """
    flattened = pc.list_flatten(table[list_col])
    return pc.struct_field(flattened, field)


__all__ = [
    "distinct_sorted",
    "flatten_list_struct_field",
]
