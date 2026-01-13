"""Shared Arrow helper utilities for normalization."""

from __future__ import annotations

from arrowdsl.core.interop import TableLike
from arrowdsl.plan.joins import code_unit_meta_config, left_join


def join_code_unit_meta(table: TableLike, code_units: TableLike) -> TableLike:
    """Left-join code unit metadata (file_id/path) onto a table.

    Returns
    -------
    TableLike
        Table with code unit metadata columns.
    """
    config = code_unit_meta_config(table.column_names, code_units.column_names)
    if config is None:
        return table
    return left_join(table, code_units, config=config, use_threads=True)


__all__ = [
    "join_code_unit_meta",
]
