"""Column helpers and nested array builders."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, cast

import pyarrow as pa

from arrowdsl.compute.expr import ExprSpec
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    ComputeExpression,
    DataTypeLike,
    ListArrayLike,
    StructArrayLike,
    TableLike,
    ensure_expression,
    pc,
)


class ColumnExpr(ExprSpec, Protocol):
    """Alias for ExprSpec retained for compatibility."""


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
        scalar = self.value if self.dtype is None else pa.scalar(self.value, type=self.dtype)
        return ensure_expression(pc.scalar(scalar))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the constant as a full-length array.

        Returns
        -------
        ArrayLike
            Array filled with the constant value.
        """
        scalar = (
            pa.scalar(self.value) if self.dtype is None else pa.scalar(self.value, type=self.dtype)
        )
        return pa.array([self.value] * table.num_rows, type=scalar.type)

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for constant expressions.
        """
        return self is not None


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

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for field references.
        """
        return self is not None


@dataclass(frozen=True)
class CastExpr:
    """Column expression casting another expression to a target type."""

    expr: ColumnExpr
    dtype: DataTypeLike
    safe: bool = True

    def to_expression(self) -> ComputeExpression:
        """Return the compute expression for the cast.

        Returns
        -------
        ComputeExpression
            Expression casting the input expression.
        """
        return ensure_expression(pc.cast(self.expr.to_expression(), self.dtype, safe=self.safe))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the cast result from the table.

        Returns
        -------
        ArrayLike
            Casted array values.
        """
        return pc.cast(self.expr.materialize(table), self.dtype, safe=self.safe)

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` when the inner expression is scalar-safe.
        """
        return self.expr.is_scalar()


@dataclass(frozen=True)
class NullFillExpr:
    """Column expression that fills nulls with a constant value."""

    expr: ColumnExpr
    fill_value: object
    dtype: DataTypeLike | None = None

    def to_expression(self) -> ComputeExpression:
        """Return the compute expression for the fill-null operation.

        Returns
        -------
        ComputeExpression
            Expression filling nulls.
        """
        fill = (
            pa.scalar(self.fill_value)
            if self.dtype is None
            else pa.scalar(self.fill_value, type=self.dtype)
        )
        return ensure_expression(pc.fill_null(self.expr.to_expression(), fill_value=fill))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the fill-null result from the table.

        Returns
        -------
        ArrayLike
            Array with nulls filled.
        """
        fill = (
            pa.scalar(self.fill_value)
            if self.dtype is None
            else pa.scalar(self.fill_value, type=self.dtype)
        )
        return pc.fill_null(self.expr.materialize(table), fill_value=fill)

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` when the inner expression is scalar-safe.
        """
        return self.expr.is_scalar()


@dataclass(frozen=True)
class CoalesceExpr:
    """Column expression that coalesces multiple expressions."""

    exprs: tuple[ColumnExpr, ...]

    def to_expression(self) -> ComputeExpression:
        """Return the compute expression for the coalesce operation.

        Returns
        -------
        ComputeExpression
            Expression coalescing the inputs.
        """
        exprs = [expr.to_expression() for expr in self.exprs]
        return ensure_expression(pc.coalesce(*exprs))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the coalesced values from the table.

        Returns
        -------
        ArrayLike
            Coalesced array values.
        """
        arrays = [expr.materialize(table) for expr in self.exprs]
        out = arrays[0]
        for arr in arrays[1:]:
            out = pc.coalesce(out, arr)
        return out

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` when all expressions are scalar-safe.
        """
        return all(expr.is_scalar() for expr in self.exprs)


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


def add_const_column(
    table: TableLike,
    name: str,
    value: object,
    *,
    data_type: DataTypeLike | None = None,
) -> TableLike:
    """Append a constant-valued column if missing.

    Returns
    -------
    TableLike
        Table with the column appended if missing.
    """
    if name in table.column_names:
        return table
    expr = ConstExpr(value=value, dtype=data_type)
    return table.append_column(name, expr.materialize(table))


def coalesce_string(table: TableLike, cols: Sequence[str], *, out_col: str) -> TableLike:
    """Coalesce multiple columns into a single string column.

    Returns
    -------
    TableLike
        Table with the coalesced column.
    """
    if out_col in table.column_names:
        return table
    exprs = tuple(FieldExpr(name=col) for col in cols)
    expr = CoalesceExpr(exprs=exprs).materialize(table)
    return table.append_column(out_col, expr)


def select_columns(table: TableLike, cols: Sequence[str]) -> TableLike:
    """Select the subset of columns that exist.

    Returns
    -------
    TableLike
        Table with selected columns.
    """
    keep = [col for col in cols if col in table.column_names]
    return table.select(keep) if keep else table


def build_struct(fields: dict[str, ArrayLike], *, mask: ArrayLike | None = None) -> StructArrayLike:
    """Build a struct array from named child arrays.

    Returns
    -------
    StructArrayLike
        Struct array built from child arrays.
    """
    names = list(fields.keys())
    arrays: list[ArrayLike] = []
    for name in names:
        arr = fields[name]
        if isinstance(arr, ChunkedArrayLike):
            arr = arr.combine_chunks()
        elif hasattr(arr, "combine_chunks"):
            arr = cast("ChunkedArrayLike", arr).combine_chunks()
        arrays.append(arr)
    if mask is not None:
        if isinstance(mask, ChunkedArrayLike):
            mask = mask.combine_chunks()
        elif hasattr(mask, "combine_chunks"):
            mask = cast("ChunkedArrayLike", mask).combine_chunks()
    return pa.StructArray.from_arrays(arrays, names=names, mask=mask)


def build_list(offsets: ArrayLike, values: ArrayLike) -> ListArrayLike:
    """Build a list array from offsets and flat values.

    Returns
    -------
    ListArrayLike
        List array built from offsets and values.
    """
    return pa.ListArray.from_arrays(offsets, values)


def build_list_view(
    offsets: ArrayLike,
    sizes: ArrayLike,
    values: ArrayLike,
    *,
    list_type: DataTypeLike | None = None,
    mask: ArrayLike | None = None,
) -> ListArrayLike:
    """Build a list_view array from offsets, sizes, and flat values.

    Returns
    -------
    ListArrayLike
        List view array built from offsets and sizes.
    """
    return pa.ListViewArray.from_arrays(
        offsets,
        sizes,
        values,
        type=list_type,
        mask=mask,
    )


def build_map(
    offsets: ArrayLike,
    keys: ArrayLike,
    items: ArrayLike,
) -> ArrayLike:
    """Build a map array from offsets and flattened keys/items.

    Returns
    -------
    ArrayLike
        Map array built from offsets and flattened key/item arrays.
    """
    return pa.MapArray.from_arrays(offsets, keys, items)


def build_sparse_union(
    type_ids: ArrayLike,
    children: list[ArrayLike],
) -> ArrayLike:
    """Build a sparse union array from type ids and child arrays.

    Returns
    -------
    ArrayLike
        Sparse union array.
    """
    return pa.UnionArray.from_sparse(type_ids, children)


def build_dense_union(
    type_ids: ArrayLike,
    offsets: ArrayLike,
    children: list[ArrayLike],
) -> ArrayLike:
    """Build a dense union array from type ids, offsets, and child arrays.

    Returns
    -------
    ArrayLike
        Dense union array.
    """
    return pa.UnionArray.from_dense(type_ids, offsets, children)


def build_list_of_structs(
    offsets: ArrayLike,
    struct_fields: dict[str, ArrayLike],
) -> ListArrayLike:
    """Build a list<struct<...>> array from offsets and child arrays.

    Returns
    -------
    ListArrayLike
        List array with struct elements.
    """
    struct_arr = build_struct(struct_fields)
    return build_list(offsets, struct_arr)


def build_struct_array(
    fields: dict[str, ArrayLike],
    *,
    mask: ArrayLike | None = None,
) -> StructArrayLike:
    """Build a struct array from named child arrays.

    Returns
    -------
    StructArrayLike
        Struct array built from child arrays.
    """
    return build_struct(fields, mask=mask)


def build_list_array(offsets: ArrayLike, values: ArrayLike) -> ListArrayLike:
    """Build a list array from offsets and flat values.

    Returns
    -------
    ListArrayLike
        List array built from offsets and values.
    """
    return build_list(offsets, values)


def build_list_view_array(
    offsets: ArrayLike,
    sizes: ArrayLike,
    values: ArrayLike,
    *,
    list_type: DataTypeLike | None = None,
    mask: ArrayLike | None = None,
) -> ListArrayLike:
    """Build a list_view array from offsets, sizes, and flat values.

    Returns
    -------
    ListArrayLike
        List view array built from offsets and sizes.
    """
    return build_list_view(offsets, sizes, values, list_type=list_type, mask=mask)


def build_map_array(
    offsets: ArrayLike,
    keys: ArrayLike,
    items: ArrayLike,
) -> ArrayLike:
    """Build a map array from offsets and flattened key/item arrays.

    Returns
    -------
    ArrayLike
        Map array built from offsets and flattened key/item arrays.
    """
    return build_map(offsets, keys, items)


def build_sparse_union_array(type_ids: ArrayLike, children: list[ArrayLike]) -> ArrayLike:
    """Build a sparse union array from type ids and child arrays.

    Returns
    -------
    ArrayLike
        Sparse union array.
    """
    return build_sparse_union(type_ids, children)


def build_dense_union_array(
    type_ids: ArrayLike,
    offsets: ArrayLike,
    children: list[ArrayLike],
) -> ArrayLike:
    """Build a dense union array from type ids, offsets, and child arrays.

    Returns
    -------
    ArrayLike
        Dense union array.
    """
    return build_dense_union(type_ids, offsets, children)


__all__ = [
    "CastExpr",
    "CoalesceExpr",
    "ColumnDefaultsSpec",
    "ColumnExpr",
    "ConstExpr",
    "FieldExpr",
    "NullFillExpr",
    "add_const_column",
    "build_dense_union",
    "build_dense_union_array",
    "build_list",
    "build_list_array",
    "build_list_of_structs",
    "build_list_view",
    "build_list_view_array",
    "build_map",
    "build_map_array",
    "build_sparse_union",
    "build_sparse_union_array",
    "build_struct",
    "build_struct_array",
    "coalesce_string",
    "const_array",
    "select_columns",
    "set_or_append_column",
]
