"""Table-level helpers for CPG builders."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.schema.schema import encoding_columns_from_metadata
from datafusion_engine.runtime import align_table_to_schema as datafusion_align_table_to_schema


def align_table_to_schema(
    table: TableLike,
    *,
    schema: SchemaLike,
    keep_extra_columns: bool = False,
) -> TableLike:
    """Align a table to a target schema.

    Returns
    -------
    TableLike
        Table aligned to the provided schema.
    """
    return datafusion_align_table_to_schema(
        table,
        schema=schema,
        keep_extra_columns=keep_extra_columns,
    )


def assert_schema_metadata(table: TableLike, *, schema: SchemaLike) -> None:
    """Raise when schema metadata does not match the target schema.

    Raises
    ------
    ValueError
        Raised when the schema metadata does not match.
    """
    table_schema = pa.schema(table.schema)
    expected_schema = pa.schema(schema)
    if not table_schema.equals(expected_schema, check_metadata=True):
        msg = "Schema metadata mismatch after finalize."
        raise ValueError(msg)


__all__ = [
    "align_table_to_schema",
    "assert_schema_metadata",
    "encoding_columns_from_metadata",
]
