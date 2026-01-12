"""Predicate helpers for plan and kernel lanes."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

import pyarrow as pa

from arrowdsl.compute.expr import ExprSpec
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    ComputeExpression,
    TableLike,
    ensure_expression,
    pc,
)

if TYPE_CHECKING:
    from arrowdsl.plan.plan import Plan


class PredicateSpec(ExprSpec, Protocol):
    """Alias for ExprSpec retained for compatibility."""


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
class Equals:
    """Predicate testing equality against a literal value."""

    col: str
    value: object

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the predicate.
        """
        return ensure_expression(pc.equal(pc.field(self.col), pc.scalar(self.value)))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane predicate mask.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        return pc.equal(table[self.col], pc.scalar(self.value))

    def is_scalar(self) -> bool:
        """Return whether this predicate is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe predicates.
        """
        return self is not None


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
class IsValid:
    """Predicate testing validity in a column."""

    col: str

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the predicate.
        """
        return ensure_expression(pc.is_valid(pc.field(self.col)))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane predicate mask.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        return pc.is_valid(table[self.col])

    def is_scalar(self) -> bool:
        """Return whether this predicate is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe predicates.
        """
        return self is not None


@dataclass(frozen=True)
class InList:
    """Predicate testing membership in a list of values."""

    col: str
    values: Sequence[object]

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane predicate expression.

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
            Expression representing the predicate.
        """
        expr = pc.is_in(pc.field(self.col), value_set=self.values)
        if self.fill_null is not None:
            expr = pc.fill_null(expr, fill_value=self.fill_null)
        return ensure_expression(expr)

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane predicate mask.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        out = pc.is_in(table[self.col], value_set=self.values)
        if self.fill_null is not None:
            out = pc.fill_null(out, fill_value=self.fill_null)
        return out

    def is_scalar(self) -> bool:
        """Return whether this predicate is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe predicates.
        """
        return self is not None


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
            Expression representing the predicate.
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

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane predicate mask.

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

    def is_scalar(self) -> bool:
        """Return whether this predicate is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe predicates.
        """
        return self is not None


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
            Expression representing the predicate.
        """
        expr = pc.cast(pc.field(self.col), pa.bool_())
        expr = pc.fill_null(expr, fill_value=self.fill_null)
        return ensure_expression(expr)

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane predicate mask.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        out = pc.cast(table[self.col], pa.bool_())
        return pc.fill_null(out, fill_value=self.fill_null)

    def is_scalar(self) -> bool:
        """Return whether this predicate is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe predicates.
        """
        return self is not None


@dataclass(frozen=True)
class MatchesRegex:
    """Predicate testing regex matches in a string column."""

    col: str
    pattern: str

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the predicate.
        """
        return ensure_expression(
            pc.match_substring_regex(pc.field(self.col), pc.scalar(self.pattern))
        )

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane predicate mask.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        return pc.match_substring_regex(table[self.col], pc.scalar(self.pattern))

    def is_scalar(self) -> bool:
        """Return whether this predicate is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe predicates.
        """
        return self is not None


@dataclass(frozen=True)
class StartsWith:
    """Predicate testing string prefix matches."""

    col: str
    prefix: str

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the predicate.
        """
        return ensure_expression(pc.starts_with(pc.field(self.col), pc.scalar(self.prefix)))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane predicate mask.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        return pc.starts_with(table[self.col], pc.scalar(self.prefix))

    def is_scalar(self) -> bool:
        """Return whether this predicate is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe predicates.
        """
        return self is not None


@dataclass(frozen=True)
class EndsWith:
    """Predicate testing string suffix matches."""

    col: str
    suffix: str

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the predicate.
        """
        return ensure_expression(pc.ends_with(pc.field(self.col), pc.scalar(self.suffix)))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane predicate mask.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        return pc.ends_with(table[self.col], pc.scalar(self.suffix))

    def is_scalar(self) -> bool:
        """Return whether this predicate is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe predicates.
        """
        return self is not None


@dataclass(frozen=True)
class Contains:
    """Predicate testing substring matches."""

    col: str
    needle: str

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the predicate.
        """
        return ensure_expression(pc.match_substring(pc.field(self.col), pc.scalar(self.needle)))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane predicate mask.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        return pc.match_substring(table[self.col], pc.scalar(self.needle))

    def is_scalar(self) -> bool:
        """Return whether this predicate is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe predicates.
        """
        return self is not None


@dataclass(frozen=True)
class And:
    """Logical AND of predicates."""

    preds: tuple[ExprSpec, ...]

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

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane combined mask.

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
        out = self.preds[0].materialize(table)
        for pred in self.preds[1:]:
            out = pc.and_(out, pred.materialize(table))
        return out

    def is_scalar(self) -> bool:
        """Return whether this predicate is scalar-safe.

        Returns
        -------
        bool
            ``True`` when predicates are provided.
        """
        return bool(self.preds)


@dataclass(frozen=True)
class Or:
    """Logical OR of predicates."""

    preds: tuple[ExprSpec, ...]

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

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane combined mask.

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
        out = self.preds[0].materialize(table)
        for pred in self.preds[1:]:
            out = pc.or_(out, pred.materialize(table))
        return out

    def is_scalar(self) -> bool:
        """Return whether this predicate is scalar-safe.

        Returns
        -------
        bool
            ``True`` when predicates are provided.
        """
        return bool(self.preds)


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


def _mask_is_true(mask: ArrayLike | ChunkedArrayLike) -> ArrayLike:
    """Return a mask indicating True values only.

    Returns
    -------
    ArrayLike
        Boolean mask for True values.
    """
    if isinstance(mask, ChunkedArrayLike):
        mask = mask.combine_chunks()
    return pc.equal(mask, pa.scalar(value=True))


def _mask_is_false(mask: ArrayLike | ChunkedArrayLike) -> ArrayLike:
    """Return a mask indicating False values only.

    Returns
    -------
    ArrayLike
        Boolean mask for False values.
    """
    if isinstance(mask, ChunkedArrayLike):
        mask = mask.combine_chunks()
    return pc.equal(mask, pa.scalar(value=False))


def invert_mask(mask: ArrayLike | ChunkedArrayLike) -> ArrayLike:
    """Return an inverted boolean mask, filling nulls with False.

    Returns
    -------
    ArrayLike
        Inverted mask with nulls filled as False.
    """
    return pc.fill_null(pc.invert(mask), fill_value=False)


def filter_eq(table: TableLike, *, col: str, value: object) -> TableLike:
    """Filter a table by equality.

    Returns
    -------
    TableLike
        Filtered table.
    """
    return table.filter(_mask_is_true(pc.equal(table[col], pc.scalar(value))))


def filter_ne(table: TableLike, *, col: str, value: object) -> TableLike:
    """Filter a table by inequality.

    Returns
    -------
    TableLike
        Filtered table.
    """
    return table.filter(_mask_is_true(pc.not_equal(table[col], pc.scalar(value))))


def filter_in(table: TableLike, *, col: str, values: Sequence[object]) -> TableLike:
    """Filter a table by membership in a value set.

    Returns
    -------
    TableLike
        Filtered table.
    """
    return table.filter(_mask_is_true(pc.is_in(table[col], value_set=list(values))))


def filter_not_in(table: TableLike, *, col: str, values: Sequence[object]) -> TableLike:
    """Filter a table by non-membership in a value set.

    Returns
    -------
    TableLike
        Filtered table.
    """
    return table.filter(_mask_is_false(pc.is_in(table[col], value_set=list(values))))


__all__ = [
    "And",
    "BitmaskMatch",
    "BoolColumn",
    "Contains",
    "EndsWith",
    "Equals",
    "FilterSpec",
    "InList",
    "InSet",
    "InValues",
    "IsNull",
    "IsValid",
    "MatchesRegex",
    "Not",
    "Or",
    "PredicateSpec",
    "StartsWith",
    "filter_eq",
    "filter_in",
    "filter_ne",
    "filter_not_in",
    "invert_mask",
]
