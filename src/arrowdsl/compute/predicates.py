"""Predicate helpers for plan and kernel lanes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from arrowdsl.compute.expr import ExprSpec
from arrowdsl.core.interop import ArrayLike, ComputeExpression, TableLike, ensure_expression, pc

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


__all__ = ["FilterSpec", "InSet", "IsNull", "Not"]
