"""Expression helpers for building Arrow compute predicates."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.compute import pc
from arrowdsl.pyarrow_protocols import ComputeExpression, ensure_expression

type ExpressionLike = str | ComputeExpression
type ScalarValue = bool | int | float | str | bytes | pa.Scalar | None


@dataclass(frozen=True)
class E:
    """Expression macros for QuerySpec predicates and Acero filter/project nodes."""

    @staticmethod
    def field(name: str) -> ComputeExpression:
        """Return a field reference expression.

        Parameters
        ----------
        name:
            Column name to reference.

        Returns
        -------
        pyarrow.compute.Expression
            Expression representing the field reference.
        """
        return pc.field(name)

    @staticmethod
    def scalar(value: ScalarValue) -> ComputeExpression:
        """Return a scalar expression.

        Parameters
        ----------
        value:
            Scalar literal value.

        Returns
        -------
        pyarrow.compute.Expression
            Expression representing the scalar value.
        """
        return pc.scalar(value)

    @staticmethod
    def eq(col: str, value: ScalarValue) -> ComputeExpression:
        """Return an equality predicate.

        Parameters
        ----------
        col:
            Column name to compare.
        value:
            Scalar value to compare against.

        Returns
        -------
        pyarrow.compute.Expression
            Equality predicate expression.
        """
        return ensure_expression(pc.equal(E.field(col), E.scalar(value)))

    @staticmethod
    def ne(col: str, value: ScalarValue) -> ComputeExpression:
        """Return an inequality predicate.

        Parameters
        ----------
        col:
            Column name to compare.
        value:
            Scalar value to compare against.

        Returns
        -------
        pyarrow.compute.Expression
            Inequality predicate expression.
        """
        return ensure_expression(pc.not_equal(E.field(col), E.scalar(value)))

    @staticmethod
    def lt(col: str, value: ScalarValue) -> ComputeExpression:
        """Return a less-than predicate.

        Parameters
        ----------
        col:
            Column name to compare.
        value:
            Scalar value to compare against.

        Returns
        -------
        pyarrow.compute.Expression
            Less-than predicate expression.
        """
        return ensure_expression(pc.less(E.field(col), E.scalar(value)))

    @staticmethod
    def le(col: str, value: ScalarValue) -> ComputeExpression:
        """Return a less-than-or-equal predicate.

        Parameters
        ----------
        col:
            Column name to compare.
        value:
            Scalar value to compare against.

        Returns
        -------
        pyarrow.compute.Expression
            Less-than-or-equal predicate expression.
        """
        return ensure_expression(pc.less_equal(E.field(col), E.scalar(value)))

    @staticmethod
    def gt(col: str, value: ScalarValue) -> ComputeExpression:
        """Return a greater-than predicate.

        Parameters
        ----------
        col:
            Column name to compare.
        value:
            Scalar value to compare against.

        Returns
        -------
        pyarrow.compute.Expression
            Greater-than predicate expression.
        """
        return ensure_expression(pc.greater(E.field(col), E.scalar(value)))

    @staticmethod
    def ge(col: str, value: ScalarValue) -> ComputeExpression:
        """Return a greater-than-or-equal predicate.

        Parameters
        ----------
        col:
            Column name to compare.
        value:
            Scalar value to compare against.

        Returns
        -------
        pyarrow.compute.Expression
            Greater-than-or-equal predicate expression.
        """
        return ensure_expression(pc.greater_equal(E.field(col), E.scalar(value)))

    @staticmethod
    def between(col: str, lo: ScalarValue, hi: ScalarValue) -> ComputeExpression:
        """Return a closed-interval predicate.

        Parameters
        ----------
        col:
            Column name to compare.
        lo:
            Lower bound (inclusive).
        hi:
            Upper bound (inclusive).

        Returns
        -------
        pyarrow.compute.Expression
            Range predicate expression.
        """
        lower = pc.greater_equal(E.field(col), E.scalar(lo))
        upper = pc.less_equal(E.field(col), E.scalar(hi))
        return ensure_expression(pc.and_(lower, upper))

    @staticmethod
    def in_(col: str, values: Sequence[ScalarValue]) -> ComputeExpression:
        """Return an inclusion predicate.

        Parameters
        ----------
        col:
            Column name to compare.
        values:
            Candidate values to match.

        Returns
        -------
        pyarrow.compute.Expression
            Inclusion predicate expression.
        """
        return E.field(col).isin(list(values))

    @staticmethod
    def is_null(col: str) -> ComputeExpression:
        """Return a null-check predicate.

        Parameters
        ----------
        col:
            Column name to check.

        Returns
        -------
        pyarrow.compute.Expression
            Null-check predicate expression.
        """
        return E.field(col).is_null()

    @staticmethod
    def is_valid(col: str) -> ComputeExpression:
        """Return a validity-check predicate.

        Parameters
        ----------
        col:
            Column name to check.

        Returns
        -------
        pyarrow.compute.Expression
            Validity predicate expression.
        """
        return E.field(col).is_valid()

    @staticmethod
    def and_(*exprs: ComputeExpression) -> ComputeExpression:
        """Combine predicates with logical AND.

        Parameters
        ----------
        *exprs:
            Expressions to combine.

        Returns
        -------
        pyarrow.compute.Expression
            Combined predicate.

        Raises
        ------
        ValueError
            Raised when no expressions are provided.
        """
        if not exprs:
            msg = "and_ requires at least one expression."
            raise ValueError(msg)
        out = exprs[0]
        for expr in exprs[1:]:
            out = ensure_expression(pc.and_(out, expr))
        return out

    @staticmethod
    def or_(*exprs: ComputeExpression) -> ComputeExpression:
        """Combine predicates with logical OR.

        Parameters
        ----------
        *exprs:
            Expressions to combine.

        Returns
        -------
        pyarrow.compute.Expression
            Combined predicate.

        Raises
        ------
        ValueError
            Raised when no expressions are provided.
        """
        if not exprs:
            msg = "or_ requires at least one expression."
            raise ValueError(msg)
        out = exprs[0]
        for expr in exprs[1:]:
            out = ensure_expression(pc.or_(out, expr))
        return out

    @staticmethod
    def not_(expr: ComputeExpression) -> ComputeExpression:
        """Negate a predicate expression.

        Parameters
        ----------
        expr:
            Expression to negate.

        Returns
        -------
        pyarrow.compute.Expression
            Negated expression.
        """
        return ensure_expression(pc.invert(expr))

    @staticmethod
    def cast(expr: ExpressionLike, target_type: object, *, safe: bool = True) -> ComputeExpression:
        """Cast an expression to a target type.

        Parameters
        ----------
        expr:
            Source expression or column name.
        target_type:
            Target Arrow data type.
        safe:
            When ``True``, only allow safe casts.

        Returns
        -------
        pyarrow.compute.Expression
            Cast expression.
        """
        expression = E.field(expr) if isinstance(expr, str) else expr
        return ensure_expression(pc.cast(expression, target_type, safe=safe))

    @staticmethod
    def coalesce(*exprs: ExpressionLike) -> ComputeExpression:
        """Return the first non-null expression in order.

        Parameters
        ----------
        *exprs:
            Expressions or column names to coalesce.

        Returns
        -------
        pyarrow.compute.Expression
            Coalesced expression.
        """
        expressions = [E.field(expr) if isinstance(expr, str) else expr for expr in exprs]
        return ensure_expression(pc.coalesce(*expressions))
