"""Validity mask helpers for Arrow tables and expressions."""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from arrowdsl.core.array_iter import iter_arrays
from arrowdsl.core.interop import ArrayLike, ChunkedArrayLike, TableLike


def _false_mask(num_rows: int) -> ArrayLike:
    """Return a boolean mask of all False values.

    Returns
    -------
    ArrayLike
        Boolean mask with all values set to ``False``.
    """
    return pa.array([False] * num_rows, type=pa.bool_())


def valid_mask_array(
    values: Sequence[ArrayLike | ChunkedArrayLike],
) -> ArrayLike | ChunkedArrayLike:
    """Return a validity mask for a sequence of arrays.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Boolean mask where all inputs are valid.

    Raises
    ------
    ValueError
        Raised when no arrays are provided.
    """
    if not values:
        msg = "valid_mask_array requires at least one array."
        raise ValueError(msg)
    mask_values = [all(value is not None for value in row) for row in iter_arrays(values)]
    return pa.array(mask_values, type=pa.bool_())


def valid_mask_for_columns(table: TableLike, cols: Sequence[str]) -> ArrayLike | ChunkedArrayLike:
    """Return a validity mask for columns, treating missing columns as invalid.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Boolean mask where all referenced columns are valid.

    Raises
    ------
    ValueError
        Raised when no column names are provided.
    """
    if not cols:
        msg = "valid_mask_for_columns requires at least one column."
        raise ValueError(msg)
    for name in cols:
        if name not in table.column_names:
            return _false_mask(table.num_rows)
    arrays = [table[name] for name in cols]
    return valid_mask_array(arrays)


__all__ = [
    "valid_mask_array",
    "valid_mask_for_columns",
]
