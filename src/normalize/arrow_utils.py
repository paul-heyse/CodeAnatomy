"""Shared Arrow helper utilities for normalization."""

from __future__ import annotations

from typing import cast

from arrowdsl.core.interop import ArrayLike, ChunkedArrayLike, TableLike, pc
from arrowdsl.plan.joins import JoinConfig, left_join


def trimmed_non_empty_utf8(
    values: ArrayLike,
) -> tuple[ArrayLike | ChunkedArrayLike, ArrayLike | ChunkedArrayLike]:
    """Return (trimmed, non-empty) mask for UTF-8 string arrays.

    Returns
    -------
    tuple[ArrayLike, ArrayLike]
        Trimmed values and non-empty mask.
    """
    trimmed = compute_array("utf8_trim_whitespace", [values])
    mask = pc.and_(
        pc.is_valid(trimmed),
        pc.greater(compute_array("utf8_length", [trimmed]), 0),
    )
    return trimmed, mask


def filter_non_empty_utf8(
    table: TableLike, column: str
) -> tuple[TableLike, ArrayLike | ChunkedArrayLike]:
    """Filter a table to rows with non-empty UTF-8 strings in column.

    Returns
    -------
    tuple[TableLike, ArrayLike]
        Filtered table and trimmed values.
    """
    trimmed, mask = trimmed_non_empty_utf8(table[column])
    return table.filter(mask), compute_array("filter", [trimmed, mask])


def join_code_unit_meta(table: TableLike, code_units: TableLike) -> TableLike:
    """Left-join code unit metadata (file_id/path) onto a table.

    Returns
    -------
    TableLike
        Table with code unit metadata columns.
    """
    if "code_unit_id" not in table.column_names or "code_unit_id" not in code_units.column_names:
        return table
    meta_cols = [
        col for col in ("code_unit_id", "file_id", "path") if col in code_units.column_names
    ]
    if len(meta_cols) <= 1:
        return table
    meta = code_units.select(meta_cols)
    right_cols = tuple(col for col in meta_cols if col != "code_unit_id")
    return left_join(
        table,
        meta,
        config=JoinConfig.from_sequences(
            left_keys=("code_unit_id",),
            right_keys=("code_unit_id",),
            left_output=tuple(table.column_names),
            right_output=right_cols,
        ),
        use_threads=True,
    )


def compute_array(
    function_name: str,
    args: list[object],
) -> ArrayLike | ChunkedArrayLike:
    """Call a compute function and return an array-like result.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Array-like compute result.
    """
    return cast("ArrayLike | ChunkedArrayLike", pc.call_function(function_name, args))


__all__ = [
    "compute_array",
    "filter_non_empty_utf8",
    "join_code_unit_meta",
    "trimmed_non_empty_utf8",
]
