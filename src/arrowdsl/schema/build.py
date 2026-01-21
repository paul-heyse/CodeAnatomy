"""Table and column builders for ArrowDSL schemas."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol, cast

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    ComputeExpression,
    DataTypeLike,
    FieldLike,
    SchemaLike,
    TableLike,
    ensure_expression,
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
from arrowdsl.schema.types import list_view_type, map_type


def _resolve_schema(schema: SchemaLike) -> pa.Schema:
    if isinstance(schema, pa.Schema):
        return schema
    to_pyarrow = getattr(schema, "to_pyarrow", None)
    if callable(to_pyarrow):
        resolved = to_pyarrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Schema must be a pyarrow.Schema derived from DataFusion."
    raise TypeError(msg)


class ColumnExpr(Protocol):
    """Protocol for column expressions used by defaults."""

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return a concrete array for the provided table."""
        ...


@dataclass(frozen=True)
class ConstExpr:
    """Column expression representing a constant literal."""

    value: object
    dtype: DataTypeLike | None = None

    def to_expression(self) -> ComputeExpression:
        """Return the compute expression for the constant value.

        Returns
        -------
        ComputeExpression
            Expression representing the constant value.
        """
        scalar_value = self.value if self.dtype is None else pa.scalar(self.value, type=self.dtype)
        return ensure_expression(pc.scalar(scalar_value))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the constant as a full-length array.

        Returns
        -------
        ArrayLike
            Array filled with the constant value.
        """
        values = [self.value] * table.num_rows
        scalar_type = pa.scalar(self.value, type=self.dtype).type
        return pa.array(values, type=scalar_type)


@dataclass(frozen=True)
class FieldExpr:
    """Column expression referencing an existing column."""

    name: str

    def to_expression(self) -> ComputeExpression:
        """Return the compute expression for the column reference.

        Returns
        -------
        ComputeExpression
            Expression referencing the column.
        """
        return pc.field(self.name)

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the column values from the table.

        Returns
        -------
        ArrayLike
            Column values from the table.
        """
        return table[self.name]


@dataclass(frozen=True)
class ColumnOrNullExpr:
    """Column expression that falls back to typed nulls when missing."""

    name: str
    dtype: DataTypeLike

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the column values or typed nulls.

        Returns
        -------
        ArrayLike
            Column values or typed nulls when missing.
        """
        if self.name not in table.column_names:
            return pa.nulls(table.num_rows, type=self.dtype)
        return pc.cast(table[self.name], self.dtype, safe=False)


@dataclass(frozen=True)
class CoalesceExpr:
    """Column expression that coalesces multiple expressions."""

    exprs: tuple[ColumnExpr, ...]

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the expression against a table.

        Returns
        -------
        ArrayLike
            Coalesced array result.
        """
        if not self.exprs:
            return pa.nulls(table.num_rows, type=pa.null())
        arrays = [expr.materialize(table) for expr in self.exprs]
        return pc.coalesce(*arrays)


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
    resolved = _resolve_schema(schema)
    arrays = [columns.get(field.name, pa.nulls(num_rows, type=field.type)) for field in resolved]
    return pa.Table.from_arrays(arrays, schema=resolved)


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
    resolved = _resolve_schema(schema)
    row_list = [dict(row) for row in rows]
    arrays = [
        nested_array_factory(field, [row.get(field.name) for row in row_list]) for field in resolved
    ]
    return pa.Table.from_arrays(arrays, schema=resolved)


def iter_rows_from_table(table: pa.Table) -> Iterator[Mapping[str, object]]:
    """Yield row dictionaries from a table without row-wise list materialization.

    Yields
    ------
    Mapping[str, object]
        Row mappings from the table.
    """
    struct_array = table.to_struct_array()
    names = table.column_names
    null_row = dict.fromkeys(names)
    for item in struct_array:
        row = item.as_py()
        if row is None:
            yield dict(null_row)
        else:
            yield dict(cast("Mapping[str, object]", row))


def rows_from_table(table: pa.Table) -> list[dict[str, object]]:
    """Return row dictionaries from a table without row-wise list materialization.

    Returns
    -------
    list[dict[str, object]]
        Row dictionaries from the table.
    """
    return [dict(row) for row in iter_rows_from_table(table)]


def table_from_row_dicts(rows: Sequence[Mapping[str, object]]) -> TableLike:
    """Build a table from row mappings with inferred schema.

    Returns
    -------
    TableLike
        Table constructed from row mappings.
    """
    row_list = [dict(row) for row in rows]
    if not row_list:
        return pa.table({})
    names: list[str] = []
    seen: set[str] = set()
    for row in row_list:
        for key in row:
            if key not in seen:
                names.append(key)
                seen.add(key)
    columns: dict[str, ArrayLike] = {}
    for name in names:
        columns[name] = pa.array([row.get(name) for row in row_list])
    return pa.table(columns)


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
    "iter_rows_from_table",
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
    "rows_from_table",
    "rows_to_table",
    "set_or_append_column",
    "sparse_union_array",
    "struct_array_from_dicts",
    "struct_type",
    "table_from_arrays",
    "table_from_row_dicts",
    "table_from_rows",
    "table_from_schema",
    "union_array_from_tagged_values",
    "union_array_from_values",
]
