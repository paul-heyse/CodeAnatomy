"""Shared Arrow helper utilities for normalization."""

from __future__ import annotations

from arrowdsl.core.interop import TableLike
from arrowdsl.plan.joins import JoinConfig, left_join


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


__all__ = [
    "join_code_unit_meta",
]
