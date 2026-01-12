"""Expression helpers for building Arrow compute predicates."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

from arrowdsl.core.interop import (
    ArrayLike,
    ComputeExpression,
    ScalarLike,
    TableLike,
    ensure_expression,
    pc,
)

type ExpressionLike = str | ComputeExpression
type ScalarValue = bool | int | float | str | bytes | ScalarLike | None


class ExprSpec(Protocol):
    """Protocol for expressions usable in plan or kernel lanes."""

    def to_expression(self) -> ComputeExpression:
        """Return the compute expression for plan-lane projection or filtering.

        Returns
        -------
        ComputeExpression
            Plan-lane compute expression.
        """
        ...

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the expression against a table in kernel lane.

        Returns
        -------
        ArrayLike
            Kernel-lane array result.
        """
        ...

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe for scan projection.

        Returns
        -------
        bool
            ``True`` when the expression is scalar-safe.
        """
        ...


@dataclass(frozen=True)
class PredicateExpr:
    """Expression with plan and kernel-lane implementations."""

    expr: ComputeExpression
    materialize_fn: Callable[[TableLike], ArrayLike]

    def to_expression(self) -> ComputeExpression:
        """Return the compute expression for plan-lane filtering.

        Returns
        -------
        ComputeExpression
            Expression for plan-lane filtering.
        """
        return self.expr

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane materialization for this predicate.

        Returns
        -------
        ArrayLike
            Boolean mask for kernel-lane filtering.
        """
        return self.materialize_fn(table)

    def mask(self, table: TableLike) -> ArrayLike:
        """Compatibility alias for kernel-lane materialization.

        Returns
        -------
        ArrayLike
            Boolean mask for kernel-lane filtering.
        """
        return self.materialize(table)

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for predicate expressions.
        """
        return self.expr is not None


@dataclass(frozen=True)
class E:
    """Expression macros for QuerySpec predicates and Acero filter/project nodes."""

    @staticmethod
    def field(name: str) -> ComputeExpression:
        """Return a field reference expression.

        Returns
        -------
        ComputeExpression
            Expression representing the field reference.
        """
        return pc.field(name)

    @staticmethod
    def scalar(value: ScalarValue) -> ComputeExpression:
        """Return a scalar expression.

        Returns
        -------
        ComputeExpression
            Expression representing the scalar value.
        """
        return pc.scalar(value)

    @staticmethod
    def eq(col: str, value: ScalarValue) -> PredicateExpr:
        """Return an equality predicate.

        Returns
        -------
        PredicateExpr
            Equality predicate expression.
        """
        expr = ensure_expression(pc.equal(E.field(col), E.scalar(value)))
        return PredicateExpr(
            expr=expr,
            materialize_fn=lambda table: pc.equal(table[col], pc.scalar(value)),
        )

    @staticmethod
    def ne(col: str, value: ScalarValue) -> PredicateExpr:
        """Return an inequality predicate.

        Returns
        -------
        PredicateExpr
            Inequality predicate expression.
        """
        expr = ensure_expression(pc.not_equal(E.field(col), E.scalar(value)))
        return PredicateExpr(
            expr=expr,
            materialize_fn=lambda table: pc.not_equal(table[col], pc.scalar(value)),
        )

    @staticmethod
    def lt(col: str, value: ScalarValue) -> PredicateExpr:
        """Return a less-than predicate.

        Returns
        -------
        PredicateExpr
            Less-than predicate expression.
        """
        expr = ensure_expression(pc.less(E.field(col), E.scalar(value)))
        return PredicateExpr(
            expr=expr,
            materialize_fn=lambda table: pc.less(table[col], pc.scalar(value)),
        )

    @staticmethod
    def le(col: str, value: ScalarValue) -> PredicateExpr:
        """Return a less-than-or-equal predicate.

        Returns
        -------
        PredicateExpr
            Less-than-or-equal predicate expression.
        """
        expr = ensure_expression(pc.less_equal(E.field(col), E.scalar(value)))
        return PredicateExpr(
            expr=expr,
            materialize_fn=lambda table: pc.less_equal(table[col], pc.scalar(value)),
        )

    @staticmethod
    def gt(col: str, value: ScalarValue) -> PredicateExpr:
        """Return a greater-than predicate.

        Returns
        -------
        PredicateExpr
            Greater-than predicate expression.
        """
        expr = ensure_expression(pc.greater(E.field(col), E.scalar(value)))
        return PredicateExpr(
            expr=expr,
            materialize_fn=lambda table: pc.greater(table[col], pc.scalar(value)),
        )

    @staticmethod
    def ge(col: str, value: ScalarValue) -> PredicateExpr:
        """Return a greater-than-or-equal predicate.

        Returns
        -------
        PredicateExpr
            Greater-than-or-equal predicate expression.
        """
        expr = ensure_expression(pc.greater_equal(E.field(col), E.scalar(value)))
        return PredicateExpr(
            expr=expr,
            materialize_fn=lambda table: pc.greater_equal(table[col], pc.scalar(value)),
        )

    @staticmethod
    def between(col: str, lo: ScalarValue, hi: ScalarValue) -> PredicateExpr:
        """Return a closed-interval predicate.

        Returns
        -------
        PredicateExpr
            Closed-interval predicate expression.
        """
        return E.and_(E.ge(col, lo), E.le(col, hi))

    @staticmethod
    def in_(col: str, values: Sequence[ScalarValue]) -> PredicateExpr:
        """Return an inclusion predicate.

        Returns
        -------
        PredicateExpr
            Inclusion predicate expression.
        """
        value_set = list(values)
        expr = ensure_expression(pc.is_in(E.field(col), value_set=value_set))
        return PredicateExpr(
            expr=expr,
            materialize_fn=lambda table: pc.is_in(table[col], value_set=value_set),
        )

    @staticmethod
    def is_null(col: str) -> PredicateExpr:
        """Return a null-check predicate.

        Returns
        -------
        PredicateExpr
            Null-check predicate expression.
        """
        expr = ensure_expression(pc.is_null(E.field(col)))
        return PredicateExpr(
            expr=expr,
            materialize_fn=lambda table: pc.is_null(table[col]),
        )

    @staticmethod
    def is_valid(col: str) -> PredicateExpr:
        """Return a validity-check predicate.

        Returns
        -------
        PredicateExpr
            Validity-check predicate expression.
        """
        expr = ensure_expression(pc.is_valid(E.field(col)))
        return PredicateExpr(
            expr=expr,
            materialize_fn=lambda table: pc.is_valid(table[col]),
        )

    @staticmethod
    def and_(*preds: ExprSpec) -> PredicateExpr:
        """Combine predicates with logical AND.

        Returns
        -------
        PredicateExpr
            Combined AND predicate expression.

        Raises
        ------
        ValueError
            Raised when no predicates are provided.
        """
        if not preds:
            msg = "and_ requires at least one predicate."
            raise ValueError(msg)
        expr_out = preds[0].to_expression()
        for pred in preds[1:]:
            expr_out = ensure_expression(pc.and_(expr_out, pred.to_expression()))

        def materialize_fn(table: TableLike) -> ArrayLike:
            out = preds[0].materialize(table)
            for pred in preds[1:]:
                out = pc.and_(out, pred.materialize(table))
            return out

        return PredicateExpr(expr=expr_out, materialize_fn=materialize_fn)

    @staticmethod
    def or_(*preds: ExprSpec) -> PredicateExpr:
        """Combine predicates with logical OR.

        Returns
        -------
        PredicateExpr
            Combined OR predicate expression.

        Raises
        ------
        ValueError
            Raised when no predicates are provided.
        """
        if not preds:
            msg = "or_ requires at least one predicate."
            raise ValueError(msg)
        expr_out = preds[0].to_expression()
        for pred in preds[1:]:
            expr_out = ensure_expression(pc.or_(expr_out, pred.to_expression()))

        def materialize_fn(table: TableLike) -> ArrayLike:
            out = preds[0].materialize(table)
            for pred in preds[1:]:
                out = pc.or_(out, pred.materialize(table))
            return out

        return PredicateExpr(expr=expr_out, materialize_fn=materialize_fn)

    @staticmethod
    def not_(pred: ExprSpec) -> PredicateExpr:
        """Negate a predicate.

        Returns
        -------
        PredicateExpr
            Negated predicate expression.
        """
        expr = ensure_expression(pc.invert(pred.to_expression()))
        return PredicateExpr(
            expr=expr, materialize_fn=lambda table: pc.invert(pred.materialize(table))
        )

    @staticmethod
    def startswith(col: str, prefix: str) -> PredicateExpr:
        """Return a startswith predicate.

        Returns
        -------
        PredicateExpr
            Startswith predicate expression.
        """
        expr = ensure_expression(pc.starts_with(E.field(col), E.scalar(prefix)))
        return PredicateExpr(
            expr=expr,
            materialize_fn=lambda table: pc.starts_with(table[col], pc.scalar(prefix)),
        )

    @staticmethod
    def endswith(col: str, suffix: str) -> PredicateExpr:
        """Return an endswith predicate.

        Returns
        -------
        PredicateExpr
            Endswith predicate expression.
        """
        expr = ensure_expression(pc.ends_with(E.field(col), E.scalar(suffix)))
        return PredicateExpr(
            expr=expr,
            materialize_fn=lambda table: pc.ends_with(table[col], pc.scalar(suffix)),
        )

    @staticmethod
    def contains(col: str, needle: str) -> PredicateExpr:
        """Return a substring-contains predicate.

        Returns
        -------
        PredicateExpr
            Substring-contains predicate expression.
        """
        expr = ensure_expression(pc.match_substring(E.field(col), E.scalar(needle)))
        return PredicateExpr(
            expr=expr,
            materialize_fn=lambda table: pc.match_substring(table[col], pc.scalar(needle)),
        )

    @staticmethod
    def regex_match(col: str, pattern: str) -> PredicateExpr:
        """Return a regex-match predicate.

        Returns
        -------
        PredicateExpr
            Regex-match predicate expression.
        """
        expr = ensure_expression(pc.match_substring_regex(E.field(col), E.scalar(pattern)))
        return PredicateExpr(
            expr=expr,
            materialize_fn=lambda table: pc.match_substring_regex(table[col], pc.scalar(pattern)),
        )

    @staticmethod
    def is_in_range(
        col: str,
        *,
        lo: ScalarValue | None = None,
        hi: ScalarValue | None = None,
    ) -> PredicateExpr:
        """Return a predicate that checks values within a range.

        Returns
        -------
        PredicateExpr
            Range predicate expression.

        Raises
        ------
        ValueError
            Raised when neither bound is provided.
        """
        if lo is None and hi is None:
            msg = "is_in_range requires at least one bound."
            raise ValueError(msg)
        if lo is None:
            return E.le(col, hi)
        if hi is None:
            return E.ge(col, lo)
        return E.between(col, lo, hi)


__all__ = [
    "E",
    "ExprSpec",
    "ExpressionLike",
    "PredicateExpr",
    "ScalarValue",
]
