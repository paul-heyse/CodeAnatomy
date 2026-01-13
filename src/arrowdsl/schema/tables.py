"""Schema-aligned table construction helpers."""

from __future__ import annotations

from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.schema.builders import empty_table, table_from_arrays, table_from_schema


def empty_table_from_schema(schema: SchemaLike) -> TableLike:
    """Return an empty table aligned to the provided schema.

    Returns
    -------
    TableLike
        Empty table with the schema.
    """
    return empty_table(schema)


__all__ = ["empty_table_from_schema", "table_from_arrays", "table_from_schema"]
