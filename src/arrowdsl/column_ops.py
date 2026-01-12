"""Shared column expression helpers for Arrow tables."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import arrowdsl.pyarrow_core as pa
from arrowdsl.compute import pc
from arrowdsl.pyarrow_protocols import (
    ArrayLike,
    ComputeExpression,
    DataTypeLike,
    TableLike,
    ensure_expression,
)


class ColumnExpr(Protocol):
    """Protocol for column expressions used in plan or kernel lanes."""

    def to_expression(self) -> ComputeExpression:
        """Return the compute expression for plan-lane projection."""
        ...

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the expression against a table in kernel lane."""
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
            Expression representing the constant literal.
        """
        scalar = self.value if self.dtype is None else pa.scalar(self.value, type=self.dtype)
        return ensure_expression(pc.scalar(scalar))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the constant as a full-length array.

        Parameters
        ----------
        table:
            Table providing the target row count.

        Returns
        -------
        ArrayLike
            Array filled with the constant value.
        """
        scalar = (
            pa.scalar(self.value) if self.dtype is None else pa.scalar(self.value, type=self.dtype)
        )
        return pa.array([self.value] * table.num_rows, type=scalar.type)


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

        Parameters
        ----------
        table:
            Table providing the column.

        Returns
        -------
        ArrayLike
            Column values from the table.
        """
        return table[self.name]


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

        Parameters
        ----------
        table:
            Table providing source values.

        Returns
        -------
        ArrayLike
            Casted array values.
        """
        return pc.cast(self.expr.materialize(table), self.dtype, safe=self.safe)


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
            Expression filling null values with a constant.
        """
        fill = (
            pa.scalar(self.fill_value)
            if self.dtype is None
            else pa.scalar(self.fill_value, type=self.dtype)
        )
        return ensure_expression(pc.fill_null(self.expr.to_expression(), fill_value=fill))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the fill-null result from the table.

        Parameters
        ----------
        table:
            Table providing source values.

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

        Parameters
        ----------
        table:
            Table providing source values.

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


def const_array(n: int, value: object, *, dtype: DataTypeLike | None = None) -> ArrayLike:
    """Return a constant array of length ``n`` with the given value.

    Parameters
    ----------
    n:
        Number of rows.
    value:
        Constant value to repeat.
    dtype:
        Optional Arrow type override.

    Returns
    -------
    ArrayLike
        Array of repeated values.
    """
    scalar = pa.scalar(value) if dtype is None else pa.scalar(value, type=dtype)
    return pa.array([value] * n, type=scalar.type)


def set_or_append_column(table: TableLike, name: str, values: ArrayLike) -> TableLike:
    """Set a column by name, appending if it does not exist.

    Parameters
    ----------
    table:
        Input table.
    name:
        Column name to set or append.
    values:
        Column values.

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

        Parameters
        ----------
        table:
            Input table.

        Returns
        -------
        TableLike
            Table with default columns applied.
        """
        out = table
        for name, expr in self.defaults:
            if not self.overwrite and name in out.column_names:
                continue
            values = expr.materialize(out)
            out = set_or_append_column(out, name, values)
        return out
