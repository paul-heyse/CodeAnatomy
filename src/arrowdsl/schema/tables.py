"""Schema-aligned table construction helpers."""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa

from arrowdsl.core.interop import ArrayLike, SchemaLike, TableLike
from arrowdsl.schema.columns import table_from_schema


def table_from_arrays(
    schema: SchemaLike,
    *,
    columns: Mapping[str, ArrayLike],
    num_rows: int,
) -> TableLike:
    """Build a table from arrays aligned to the provided schema.

    Returns
    -------
    TableLike
        Table aligned to the schema with typed nulls for missing columns.
    """
    return table_from_schema(schema, columns=columns, num_rows=num_rows)


def empty_table_from_schema(schema: SchemaLike) -> TableLike:
    """Return an empty table aligned to the provided schema.

    Returns
    -------
    TableLike
        Empty table with the schema.
    """
    return pa.Table.from_arrays([pa.array([], type=field.type) for field in schema], schema=schema)


__all__ = ["empty_table_from_schema", "table_from_arrays"]
