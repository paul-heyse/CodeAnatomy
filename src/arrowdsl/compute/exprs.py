"""Canonical expression specs for plan and kernel lanes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa

from arrowdsl.compute.expr import ExprSpec
from arrowdsl.core.interop import (
    ArrayLike,
    ComputeExpression,
    DataTypeLike,
    TableLike,
    ensure_expression,
    pc,
)


class ColumnExpr(ExprSpec, Protocol):
    """Alias for expression specs used as column expressions."""


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
        values = [self.value] * table.num_rows
        scalar_type = pa.scalar(self.value, type=self.dtype).type
        return pa.array(values, type=scalar_type)

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


__all__ = ["CoalesceExpr", "ColumnExpr", "ConstExpr", "FieldExpr"]
