"""Predicate helpers for plan and kernel lanes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.compute.expr import ExprSpec
from arrowdsl.core.interop import (
    ArrayLike,
    ComputeExpression,
    DataTypeLike,
    TableLike,
    ensure_expression,
    pc,
)

if TYPE_CHECKING:
    from arrowdsl.plan.plan import Plan


@dataclass(frozen=True)
class FilterSpec:
    """Filter specification usable in plan or kernel lanes."""

    predicate: ExprSpec

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the predicate.
        """
        return self.predicate.to_expression()

    def mask(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane boolean mask.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        return self.predicate.materialize(table)

    def apply_plan(self, plan: Plan) -> Plan:
        """Apply the filter to a plan.

        Returns
        -------
        Plan
            Filtered plan.
        """
        return plan.filter(self.to_expression())

    def apply_kernel(self, table: TableLike) -> TableLike:
        """Apply the filter to a table.

        Returns
        -------
        TableLike
            Filtered table.
        """
        return table.filter(self.mask(table))


@dataclass(frozen=True)
class IsNull:
    """Predicate testing nulls in a column."""

    col: str

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the predicate.
        """
        return ensure_expression(pc.is_null(pc.field(self.col)))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane predicate mask.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        return pc.is_null(table[self.col])

    def is_scalar(self) -> bool:
        """Return whether this predicate is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe predicates.
        """
        return self is not None


@dataclass(frozen=True)
class InSet:
    """Predicate testing membership in a static value set."""

    col: str
    values: tuple[object, ...]

    def to_expression(self) -> ComputeExpression:
        """Return the membership predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the predicate.
        """
        return ensure_expression(pc.is_in(pc.field(self.col), value_set=list(self.values)))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane predicate mask.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        return pc.is_in(table[self.col], value_set=list(self.values))

    def is_scalar(self) -> bool:
        """Return whether this predicate is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe predicates.
        """
        return self is not None


@dataclass(frozen=True)
class Not:
    """Logical NOT of a predicate."""

    pred: ExprSpec

    def to_expression(self) -> ComputeExpression:
        """Return the inverted predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the inverted predicate.
        """
        return ensure_expression(pc.invert(self.pred.to_expression()))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane inverted mask.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        return pc.invert(self.pred.materialize(table))

    def is_scalar(self) -> bool:
        """Return whether this predicate is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe predicates.
        """
        return self is not None


def null_if_empty_or_zero(expr: ComputeExpression) -> ComputeExpression:
    """Return ``expr`` with empty/zero strings normalized to null.

    Returns
    -------
    ComputeExpression
        Expression with empty/zero strings mapped to null.
    """
    empty = ensure_expression(pc.equal(expr, pc.scalar("")))
    zero = ensure_expression(pc.equal(expr, pc.scalar("0")))
    return ensure_expression(
        pc.if_else(
            pc.or_(empty, zero),
            pc.cast(pc.scalar(None), pa.string(), safe=False),
            expr,
        )
    )


def zero_expr(values: ComputeExpression, *, dtype: DataTypeLike) -> ComputeExpression:
    """Return a boolean expression testing for zero values.

    Returns
    -------
    ComputeExpression
        Boolean expression indicating zero values.
    """
    if patypes.is_dictionary(dtype):
        values = ensure_expression(pc.cast(values, pa.string(), safe=False))
        dtype = pa.string()
    if patypes.is_string(dtype) or patypes.is_large_string(dtype):
        return ensure_expression(pc.equal(values, pc.scalar("0")))
    if patypes.is_integer(dtype):
        return ensure_expression(pc.equal(values, pa.scalar(0, type=dtype)))
    if patypes.is_floating(dtype):
        return ensure_expression(pc.equal(values, pa.scalar(0.0, type=dtype)))
    values = ensure_expression(pc.cast(values, pa.string(), safe=False))
    return ensure_expression(pc.equal(values, pc.scalar("0")))


def invalid_id_expr(values: ComputeExpression, *, dtype: DataTypeLike) -> ComputeExpression:
    """Return an expression for null-or-zero identifier checks.

    Returns
    -------
    ComputeExpression
        Expression identifying null or zero identifiers.
    """
    return ensure_expression(pc.or_(pc.is_null(values), zero_expr(values, dtype=dtype)))


def bitmask_is_set_expr(values: ComputeExpression, *, mask: int) -> ComputeExpression:
    """Return an expression indicating whether a bitmask flag is set.

    Returns
    -------
    ComputeExpression
        Expression indicating whether the mask bit is set.
    """
    roles = pc.cast(values, pa.int64(), safe=False)
    hit = pc.not_equal(pc.bit_wise_and(roles, pa.scalar(mask)), pa.scalar(0))
    return ensure_expression(pc.fill_null(hit, fill_value=False))


__all__ = [
    "FilterSpec",
    "InSet",
    "IsNull",
    "Not",
    "bitmask_is_set_expr",
    "invalid_id_expr",
    "null_if_empty_or_zero",
    "zero_expr",
]
