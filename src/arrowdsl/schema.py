"""Shared schema alignment utilities."""

from __future__ import annotations

from typing import Literal, TypedDict

import arrowdsl.pyarrow_core as pa
from arrowdsl.compute import pc
from arrowdsl.pyarrow_protocols import ArrayLike, FieldLike, SchemaLike, TableLike

type CastErrorPolicy = Literal["unsafe", "keep", "raise"]


class AlignmentInfo(TypedDict):
    """Alignment metadata for schema casting and column selection."""

    input_cols: list[str]
    input_rows: int
    missing_cols: list[str]
    dropped_cols: list[str]
    casted_cols: list[str]
    output_rows: int


def _cast_column(
    col: ArrayLike,
    field: FieldLike,
    *,
    safe_cast: bool,
    on_error: CastErrorPolicy,
) -> tuple[ArrayLike, bool]:
    """Cast a column to a field type, returning the casted flag.

    Returns
    -------
    tuple[pyarrow.Array | pyarrow.ChunkedArray, bool]
        Casted column and cast success flag.

    Raises
    ------
    ArrowInvalid
        Raised when casting fails and ``on_error="raise"``.
    ArrowTypeError
        Raised when casting fails and ``on_error="raise"``.
    """
    if col.type == field.type:
        return col, False
    try:
        return pc.cast(col, field.type, safe=safe_cast), True
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        if on_error == "unsafe":
            return pc.cast(col, field.type, safe=False), True
        if on_error == "keep":
            return col, False
        raise


def align_to_schema(
    table: TableLike,
    *,
    schema: SchemaLike,
    safe_cast: bool,
    on_error: CastErrorPolicy = "unsafe",
    keep_extra_columns: bool = False,
) -> tuple[TableLike, AlignmentInfo]:
    """Align and cast a table to a target schema.

    Parameters
    ----------
    table:
        Input table.
    schema:
        Target schema.
    safe_cast:
        When ``True``, allow safe casts only.
    on_error:
        Behavior when casting fails ("unsafe", "keep", "raise").
    keep_extra_columns:
        When ``True``, retain extra columns after alignment.

    Returns
    -------
    tuple[pyarrow.Table, AlignmentInfo]
        Aligned table and alignment metadata.

    """
    info: AlignmentInfo = {
        "input_cols": list(table.column_names),
        "input_rows": int(table.num_rows),
        "missing_cols": [],
        "dropped_cols": [],
        "casted_cols": [],
        "output_rows": 0,
    }

    target_names = [field.name for field in schema]
    missing = [name for name in target_names if name not in table.column_names]
    extra = [name for name in table.column_names if name not in target_names]

    arrays: list[ArrayLike] = []
    for field in schema:
        if field.name in table.column_names:
            col, casted = _cast_column(
                table[field.name],
                field,
                safe_cast=safe_cast,
                on_error=on_error,
            )
            if casted:
                info["casted_cols"].append(field.name)
            arrays.append(col)
        else:
            arrays.append(pa.nulls(table.num_rows, type=field.type))

    aligned = pa.Table.from_arrays(arrays, schema=schema)
    info["missing_cols"] = missing
    info["dropped_cols"] = extra
    info["output_rows"] = int(aligned.num_rows)

    if keep_extra_columns:
        for name in table.column_names:
            if name not in aligned.column_names:
                aligned = aligned.append_column(name, table[name])

    return aligned, info
