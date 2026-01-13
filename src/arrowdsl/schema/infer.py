"""Arrow-native schema inference helpers."""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import arrowdsl.core.interop as pa
from arrowdsl.core.interop import ArrayLike, DataTypeLike, ScalarLike, SchemaLike, TableLike, pc
from arrowdsl.schema.unify import unify_schemas


def best_fit_type(array: ArrayLike, candidates: Sequence[DataTypeLike]) -> DataTypeLike:
    """Return the most specific candidate type that preserves validity.

    Returns
    -------
    pyarrow.DataType
        Best-fit type for the array.
    """
    total_rows = len(array)
    for dtype in candidates:
        casted = pc.cast(array, dtype, safe=False)
        valid = pc.is_valid(casted)
        total = pc.call_function("sum", [pc.cast(valid, pa.int64())])
        value = cast("int | float | bool | None", cast("ScalarLike", total).as_py())
        if value is None:
            continue
        count = int(value)
        if count == total_rows:
            return dtype
    return array.type


def infer_schema_from_tables(
    tables: Sequence[TableLike],
    *,
    promote_options: str = "permissive",
    prefer_nested: bool = True,
) -> SchemaLike:
    """Infer a unified schema from tables using Arrow evolution rules.

    Returns
    -------
    SchemaLike
        Unified schema for the input tables.
    """
    schemas = [table.schema for table in tables if table is not None]
    return unify_schemas(
        schemas,
        promote_options=promote_options,
        prefer_nested=prefer_nested,
    )


__all__ = [
    "best_fit_type",
    "infer_schema_from_tables",
]
