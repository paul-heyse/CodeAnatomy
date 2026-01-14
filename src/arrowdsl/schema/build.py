"""Table and column builders for ArrowDSL schemas."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.compute.macros import CoalesceExpr, ColumnExpr, ColumnOrNullExpr, ConstExpr, FieldExpr
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    DataTypeLike,
    FieldLike,
    SchemaLike,
    TableLike,
    pc,
)
from arrowdsl.schema.nested_builders import (
    build_list,
    build_list_of_structs,
    build_list_view,
    build_struct,
    dense_union_array,
    dictionary_array_from_indices,
    dictionary_array_from_values,
    list_array_from_lists,
    list_view_array_from_lists,
    map_array_from_pairs,
    nested_array_factory,
    sparse_union_array,
    struct_array_from_dicts,
    union_array_from_tagged_values,
    union_array_from_values,
)


def const_array(n: int, value: object, *, dtype: DataTypeLike | None = None) -> ArrayLike:
    """Return a constant array of length ``n`` with the given value.

    Returns
    -------
    ArrayLike
        Array of repeated values.
    """
    scalar = pa.scalar(value) if dtype is None else pa.scalar(value, type=dtype)
    return pa.array([value] * n, type=scalar.type)


def set_or_append_column(table: TableLike, name: str, values: ArrayLike) -> TableLike:
    """Set a column by name, appending if it does not exist.

    Returns
    -------
    TableLike
        Updated table.
    """
    if name in table.column_names:
        idx = table.schema.get_field_index(name)
        return table.set_column(idx, name, values)
    return table.append_column(name, values)


@dataclass(frozen=True)
class ColumnDefaultsSpec:
    """Specification for adding default columns when missing."""

    defaults: tuple[tuple[str, ColumnExpr], ...]
    overwrite: bool = False

    def apply(self, table: TableLike) -> TableLike:
        """Apply default column values to a table.

        Returns
        -------
        TableLike
            Table with defaults applied.
        """
        out = table
        for name, expr in self.defaults:
            if not self.overwrite and name in out.column_names:
                continue
            values = expr.materialize(out)
            out = set_or_append_column(out, name, values)
        return out


def list_view_type(value_type: DataTypeLike, *, large: bool = False) -> DataTypeLike:
    """Return a list_view type (large_list_view when requested).

    Returns
    -------
    DataTypeLike
        List view data type.
    """
    return pa.large_list_view(value_type) if large else pa.list_view(value_type)


def map_type(
    key_type: DataTypeLike,
    item_type: DataTypeLike,
    *,
    keys_sorted: bool | None = None,
) -> DataTypeLike:
    """Return a map type for the provided key/value types.

    Returns
    -------
    DataTypeLike
        Map data type.
    """
    if keys_sorted is None:
        return pa.map_(key_type, item_type)
    return pa.map_(key_type, item_type, keys_sorted=keys_sorted)


def struct_type(fields: Sequence[FieldLike] | Mapping[str, DataTypeLike]) -> DataTypeLike:
    """Return a struct type built from fields or name/type mappings.

    Returns
    -------
    DataTypeLike
        Struct data type.
    """
    if isinstance(fields, Mapping):
        return pa.struct(fields)
    return pa.struct(list(fields))


def column_or_null(
    table: TableLike,
    col: str,
    dtype: DataTypeLike,
) -> ArrayLike:
    """Return a column array or a typed null array when missing.

    Returns
    -------
    ArrayLike
        Column array or a typed null array.
    """
    return ColumnOrNullExpr(name=col, dtype=dtype).materialize(table)


def maybe_dictionary(
    values: ArrayLike | ChunkedArrayLike,
    dtype: DataTypeLike,
) -> ArrayLike | ChunkedArrayLike:
    """Return dictionary-encoded values when requested by dtype.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Dictionary-encoded values when needed.
    """
    if patypes.is_dictionary(values.type):
        return values
    if patypes.is_dictionary(dtype):
        return pc.dictionary_encode(values)
    return values


def pick_first(
    table: TableLike,
    cols: Sequence[str],
    *,
    default_type: DataTypeLike,
) -> ArrayLike:
    """Return the first available column or a typed null array.

    Returns
    -------
    ArrayLike
        Column array or typed nulls.
    """
    for col in cols:
        if col in table.column_names:
            return table[col]
    return pa.nulls(table.num_rows, type=default_type)


def resolve_string_col(table: TableLike, col: str, *, default_value: str) -> ArrayLike:
    """Resolve a string column with nulls filled.

    Returns
    -------
    ArrayLike
        Column array with nulls filled.
    """
    arr = pick_first(table, [col], default_type=pa.string())
    if arr.null_count == 0:
        return arr
    return pc.fill_null(arr, fill_value=default_value)


def resolve_float_col(table: TableLike, col: str, *, default_value: float) -> ArrayLike:
    """Resolve a float column with nulls filled.

    Returns
    -------
    ArrayLike
        Column array with nulls filled.
    """
    arr = pick_first(table, [col], default_type=pa.float32())
    if arr.null_count == 0:
        return arr
    return pc.fill_null(arr, fill_value=default_value)


def table_from_schema(
    schema: SchemaLike,
    *,
    columns: Mapping[str, ArrayLike],
    num_rows: int,
) -> TableLike:
    """Build a table from a schema and a column mapping.

    Returns
    -------
    TableLike
        Table with missing columns filled as typed nulls.
    """
    arrays = [columns.get(field.name, pa.nulls(num_rows, type=field.type)) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def table_from_rows(
    schema: SchemaLike,
    rows: Iterable[Mapping[str, object]],
) -> TableLike:
    """Build a table from row mappings aligned to the provided schema.

    Returns
    -------
    TableLike
        Table built from row mappings.
    """
    row_list = [dict(row) for row in rows]
    arrays = [
        nested_array_factory(field, [row.get(field.name) for row in row_list]) for field in schema
    ]
    return pa.Table.from_arrays(arrays, schema=pa.schema(schema))


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


def empty_table(schema: SchemaLike) -> TableLike:
    """Return an empty table with the provided schema.

    Returns
    -------
    TableLike
        Empty table with the schema.
    """
    return pa.Table.from_arrays([pa.array([], type=field.type) for field in schema], schema=schema)


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
    "CoalesceExpr",
    "ColumnDefaultsSpec",
    "ColumnExpr",
    "ColumnOrNullExpr",
    "ConstExpr",
    "FieldExpr",
    "build_list",
    "build_list_of_structs",
    "build_list_view",
    "build_struct",
    "const_array",
    "dense_union_array",
    "dictionary_array_from_indices",
    "dictionary_array_from_values",
    "empty_table",
    "list_array_from_lists",
    "list_view_array_from_lists",
    "list_view_type",
    "map_array_from_pairs",
    "map_type",
    "maybe_dictionary",
    "nested_array_factory",
    "pick_first",
    "resolve_float_col",
    "resolve_string_col",
    "rows_to_table",
    "set_or_append_column",
    "sparse_union_array",
    "struct_array_from_dicts",
    "struct_type",
    "table_from_arrays",
    "table_from_rows",
    "table_from_schema",
    "union_array_from_tagged_values",
    "union_array_from_values",
]
