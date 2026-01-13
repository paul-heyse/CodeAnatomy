"""Adapters for building Arrow spec tables from Python structures."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

import pyarrow as pa


def table_from_rows(schema: pa.Schema, rows: Iterable[Mapping[str, Any]]) -> pa.Table:
    """Build a spec table from row dictionaries.

    Returns
    -------
    pa.Table
        Arrow table using the provided schema.
    """
    return pa.Table.from_pylist([dict(row) for row in rows], schema=schema)


def rows_from_table(table: pa.Table) -> list[dict[str, Any]]:
    """Return rows from a spec table.

    Returns
    -------
    list[dict[str, Any]]
        List of row dictionaries.
    """
    return table.to_pylist()


__all__ = ["rows_from_table", "table_from_rows"]
