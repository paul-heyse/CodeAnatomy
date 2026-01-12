"""Shared table construction helpers for extractors."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import arrowdsl.core.interop as pa
from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.schema.schema import SchemaTransform, empty_table


def rows_to_table(rows: Sequence[Mapping[str, object]], schema: SchemaLike) -> TableLike:
    """Build a table from row mappings or return an empty table.

    Returns
    -------
    TableLike
        Table constructed from rows or an empty table.
    """
    if not rows:
        return empty_table(schema)
    return pa.Table.from_pylist(list(rows), schema=schema)


def align_table(table: TableLike, *, schema: SchemaLike) -> TableLike:
    """Align a table to a target schema.

    Returns
    -------
    TableLike
        Aligned table.
    """
    return SchemaTransform(schema=schema).apply(table)


def build_and_align(rows: Sequence[Mapping[str, object]], *, schema: SchemaLike) -> TableLike:
    """Build a table from rows and align it to the schema.

    Returns
    -------
    TableLike
        Aligned table.
    """
    table = rows_to_table(rows, schema)
    return align_table(table, schema=schema)
