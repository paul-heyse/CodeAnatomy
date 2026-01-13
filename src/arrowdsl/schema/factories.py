"""Table construction factories for ArrowDSL."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.schema.builders import (
    empty_table,
    table_from_arrays,
    table_from_rows,
    table_from_schema,
)


def rows_to_table(rows: Sequence[Mapping[str, object]], schema: SchemaLike) -> TableLike:
    """Build a table from row mappings or return an empty schema table.

    Returns
    -------
    TableLike
        Table constructed from row mappings or an empty table.
    """
    if not rows:
        return empty_table(schema)
    return table_from_rows(schema, rows)


__all__ = [
    "empty_table",
    "rows_to_table",
    "table_from_arrays",
    "table_from_rows",
    "table_from_schema",
]
