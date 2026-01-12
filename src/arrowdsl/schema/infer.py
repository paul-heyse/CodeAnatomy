"""Arrow-native schema inference helpers."""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import arrowdsl.core.interop as pa
from arrowdsl.core.interop import ArrayLike, DataTypeLike, ScalarLike, SchemaLike, TableLike, pc
from arrowdsl.schema.schema import SchemaEvolutionSpec


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
) -> SchemaLike:
    """Infer a unified schema from tables using Arrow evolution rules.

    Returns
    -------
    SchemaLike
        Unified schema for the input tables.
    """
    evolution = SchemaEvolutionSpec(promote_options=promote_options)
    return evolution.unify_schema([table for table in tables if table is not None])


__all__ = [
    "best_fit_type",
    "infer_schema_from_tables",
]
