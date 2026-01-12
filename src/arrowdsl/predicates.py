"""Predicate helpers for plan and kernel lanes."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

import arrowdsl.pyarrow_core as pa
from arrowdsl.compute import pc
from arrowdsl.pyarrow_protocols import (
    ArrayLike,
    ChunkedArrayLike,
    ComputeExpression,
    TableLike,
    ensure_expression,
)

if TYPE_CHECKING:
    from arrowdsl.plan import Plan


class PredicateSpec(Protocol):
    """Protocol for predicates usable in plan or kernel lanes."""

    def to_expression(self) -> ComputeExpression:
        """Return a compute expression for plan-lane filters."""
        ...

    def mask(self, table: TableLike) -> ArrayLike:
        """Return a boolean mask for kernel-lane filtering."""
        ...


@dataclass(frozen=True)
class PredicateExpr:
    """Predicate expression that renders to plan and kernel lanes."""

    expr: ComputeExpression
    mask_fn: Callable[[TableLike], ArrayLike]

    def to_expression(self) -> ComputeExpression:
        """Return the compute expression for plan-lane filters.

        Returns
        -------
        ComputeExpression
            Expression for plan-lane filtering.
        """
        return self.expr

    def mask(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane mask for this predicate.

        Returns
        -------
        ArrayLike
            Boolean mask for kernel-lane filtering.
        """
        return self.mask_fn(table)


@dataclass(frozen=True)
class FilterSpec:
    """Filter specification usable in plan or kernel lanes."""

    predicate: PredicateSpec

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
        return self.predicate.mask(table)

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
class Equals:
    """Predicate testing equality against a literal value."""

    col: str
    value: object

    def to_expression(self) -> ComputeExpression:
        """Return the equality predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the equality predicate.
        """
        return ensure_expression(pc.equal(pc.field(self.col), pc.scalar(self.value)))

    def mask(self, table: TableLike) -> ArrayLike:
        """Return a boolean mask for the equality predicate.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        return pc.equal(table[self.col], pc.scalar(self.value))


@dataclass(frozen=True)
class InSet:
    """Predicate testing membership in a set of values."""

    col: str
    values: tuple[object, ...]

    def to_expression(self) -> ComputeExpression:
        """Return the membership predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the membership predicate.
        """
        return ensure_expression(pc.is_in(pc.field(self.col), value_set=list(self.values)))

    def mask(self, table: TableLike) -> ArrayLike:
        """Return a boolean mask for the membership predicate.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        return pc.is_in(table[self.col], value_set=list(self.values))


@dataclass(frozen=True)
class InValues:
    """Predicate testing membership in a dynamic value set."""

    col: str
    values: ArrayLike | ChunkedArrayLike | Sequence[object]
    fill_null: bool | None = None

    def to_expression(self) -> ComputeExpression:
        """Return the membership predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the membership predicate.
        """
        expr = pc.is_in(pc.field(self.col), value_set=self.values)
        if self.fill_null is not None:
            expr = pc.fill_null(expr, fill_value=self.fill_null)
        return ensure_expression(expr)

    def mask(self, table: TableLike) -> ArrayLike:
        """Return a boolean mask for the membership predicate.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        out = pc.is_in(table[self.col], value_set=self.values)
        if self.fill_null is not None:
            out = pc.fill_null(out, fill_value=self.fill_null)
        return out


@dataclass(frozen=True)
class BitmaskMatch:
    """Predicate testing whether a bitmask is set or unset."""

    col: str
    bitmask: int
    must_set: bool = True
    fill_null: bool = False

    def to_expression(self) -> ComputeExpression:
        """Return the bitmask predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the bitmask predicate.
        """
        roles = pc.cast(pc.field(self.col), pa.int64())
        hit = pc.not_equal(
            pc.bit_wise_and(roles, pa.scalar(int(self.bitmask))),
            pa.scalar(0),
        )
        hit = pc.fill_null(hit, fill_value=self.fill_null)
        if not self.must_set:
            hit = pc.invert(hit)
        return ensure_expression(hit)

    def mask(self, table: TableLike) -> ArrayLike:
        """Return a boolean mask for the bitmask predicate.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        roles = pc.cast(table[self.col], pa.int64())
        hit = pc.not_equal(
            pc.bit_wise_and(roles, pa.scalar(int(self.bitmask))),
            pa.scalar(0),
        )
        hit = pc.fill_null(hit, fill_value=self.fill_null)
        if not self.must_set:
            hit = pc.invert(hit)
        return hit


@dataclass(frozen=True)
class BoolColumn:
    """Predicate using a boolean column with null fill."""

    col: str
    fill_null: bool = False

    def to_expression(self) -> ComputeExpression:
        """Return the boolean predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the boolean predicate.
        """
        expr = pc.cast(pc.field(self.col), pa.bool_())
        expr = pc.fill_null(expr, fill_value=self.fill_null)
        return ensure_expression(expr)

    def mask(self, table: TableLike) -> ArrayLike:
        """Return a boolean mask for the predicate.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        out = pc.cast(table[self.col], pa.bool_())
        return pc.fill_null(out, fill_value=self.fill_null)


@dataclass(frozen=True)
class IsNull:
    """Predicate testing for nulls."""

    col: str

    def to_expression(self) -> ComputeExpression:
        """Return the null-check predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the null-check predicate.
        """
        return ensure_expression(pc.is_null(pc.field(self.col)))

    def mask(self, table: TableLike) -> ArrayLike:
        """Return a boolean mask for the null-check predicate.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        return pc.is_null(table[self.col])


@dataclass(frozen=True)
class And:
    """Logical AND of predicates."""

    preds: tuple[PredicateSpec, ...]

    def to_expression(self) -> ComputeExpression:
        """Return the combined AND predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the combined predicate.

        Raises
        ------
        ValueError
            Raised when no predicates are provided.
        """
        if not self.preds:
            msg = "And predicate requires at least one predicate."
            raise ValueError(msg)
        out = self.preds[0].to_expression()
        for pred in self.preds[1:]:
            out = ensure_expression(pc.and_(out, pred.to_expression()))
        return out

    def mask(self, table: TableLike) -> ArrayLike:
        """Return a boolean mask for the combined AND predicate.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.

        Raises
        ------
        ValueError
            Raised when no predicates are provided.
        """
        if not self.preds:
            msg = "And predicate requires at least one predicate."
            raise ValueError(msg)
        out = self.preds[0].mask(table)
        for pred in self.preds[1:]:
            out = pc.and_(out, pred.mask(table))
        return out


@dataclass(frozen=True)
class Or:
    """Logical OR of predicates."""

    preds: tuple[PredicateSpec, ...]

    def to_expression(self) -> ComputeExpression:
        """Return the combined OR predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the combined predicate.

        Raises
        ------
        ValueError
            Raised when no predicates are provided.
        """
        if not self.preds:
            msg = "Or predicate requires at least one predicate."
            raise ValueError(msg)
        out = self.preds[0].to_expression()
        for pred in self.preds[1:]:
            out = ensure_expression(pc.or_(out, pred.to_expression()))
        return out

    def mask(self, table: TableLike) -> ArrayLike:
        """Return a boolean mask for the combined OR predicate.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.

        Raises
        ------
        ValueError
            Raised when no predicates are provided.
        """
        if not self.preds:
            msg = "Or predicate requires at least one predicate."
            raise ValueError(msg)
        out = self.preds[0].mask(table)
        for pred in self.preds[1:]:
            out = pc.or_(out, pred.mask(table))
        return out


@dataclass(frozen=True)
class Not:
    """Logical NOT of a predicate."""

    pred: PredicateSpec

    def to_expression(self) -> ComputeExpression:
        """Return the inverted predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the inverted predicate.
        """
        return ensure_expression(pc.invert(self.pred.to_expression()))

    def mask(self, table: TableLike) -> ArrayLike:
        """Return a boolean mask for the inverted predicate.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        return pc.invert(self.pred.mask(table))
