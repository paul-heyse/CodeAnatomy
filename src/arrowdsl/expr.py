"""Expression helpers for building Arrow compute predicates."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc

type ExpressionLike = str | pc.Expression
type ScalarValue = bool | int | float | str | bytes | pa.Scalar | None


@dataclass(frozen=True)
class E:
    """Expression macros for QuerySpec predicates and Acero filter/project nodes."""

    @staticmethod
    def field(name: str) -> pc.Expression:
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
    def scalar(value: ScalarValue) -> pc.Expression:
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
    def eq(col: str, value: ScalarValue) -> pc.Expression:
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
        return E.field(col) == E.scalar(value)

    @staticmethod
    def ne(col: str, value: ScalarValue) -> pc.Expression:
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
        return E.field(col) != E.scalar(value)

    @staticmethod
    def lt(col: str, value: ScalarValue) -> pc.Expression:
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
        return E.field(col) < E.scalar(value)

    @staticmethod
    def le(col: str, value: ScalarValue) -> pc.Expression:
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
        return E.field(col) <= E.scalar(value)

    @staticmethod
    def gt(col: str, value: ScalarValue) -> pc.Expression:
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
        return E.field(col) > E.scalar(value)

    @staticmethod
    def ge(col: str, value: ScalarValue) -> pc.Expression:
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
        return E.field(col) >= E.scalar(value)

    @staticmethod
    def between(col: str, lo: ScalarValue, hi: ScalarValue) -> pc.Expression:
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
        return (E.field(col) >= E.scalar(lo)) & (E.field(col) <= E.scalar(hi))

    @staticmethod
    def in_(col: str, values: Sequence[ScalarValue]) -> pc.Expression:
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
    def is_null(col: str) -> pc.Expression:
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
    def is_valid(col: str) -> pc.Expression:
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
    def and_(*exprs: pc.Expression) -> pc.Expression:
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
            out &= expr
        return out

    @staticmethod
    def or_(*exprs: pc.Expression) -> pc.Expression:
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
            out |= expr
        return out

    @staticmethod
    def not_(expr: pc.Expression) -> pc.Expression:
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
        return ~expr

    @staticmethod
    def cast(expr: ExpressionLike, target_type: object, *, safe: bool = True) -> pc.Expression:
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
        return pc.cast(expression, target_type, safe=safe)

    @staticmethod
    def coalesce(*exprs: ExpressionLike) -> pc.Expression:
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
        return pc.coalesce(*expressions)
