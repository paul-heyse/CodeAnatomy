"""Expression helpers for building Arrow compute predicates."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from arrowdsl.compute import pc
from arrowdsl.predicates import PredicateExpr, PredicateSpec
from arrowdsl.pyarrow_protocols import (
    ArrayLike,
    ComputeExpression,
    ScalarLike,
    TableLike,
    ensure_expression,
)

type ExpressionLike = str | ComputeExpression
type ScalarValue = bool | int | float | str | bytes | ScalarLike | None


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
    def eq(col: str, value: ScalarValue) -> PredicateExpr:
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
        expr = ensure_expression(pc.equal(E.field(col), E.scalar(value)))
        return PredicateExpr(
            expr=expr, mask_fn=lambda table: pc.equal(table[col], pc.scalar(value))
        )

    @staticmethod
    def ne(col: str, value: ScalarValue) -> PredicateExpr:
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
        expr = ensure_expression(pc.not_equal(E.field(col), E.scalar(value)))
        return PredicateExpr(
            expr=expr,
            mask_fn=lambda table: pc.not_equal(table[col], pc.scalar(value)),
        )

    @staticmethod
    def lt(col: str, value: ScalarValue) -> PredicateExpr:
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
        expr = ensure_expression(pc.less(E.field(col), E.scalar(value)))
        return PredicateExpr(expr=expr, mask_fn=lambda table: pc.less(table[col], pc.scalar(value)))

    @staticmethod
    def le(col: str, value: ScalarValue) -> PredicateExpr:
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
        expr = ensure_expression(pc.less_equal(E.field(col), E.scalar(value)))
        return PredicateExpr(
            expr=expr,
            mask_fn=lambda table: pc.less_equal(table[col], pc.scalar(value)),
        )

    @staticmethod
    def gt(col: str, value: ScalarValue) -> PredicateExpr:
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
        expr = ensure_expression(pc.greater(E.field(col), E.scalar(value)))
        return PredicateExpr(
            expr=expr,
            mask_fn=lambda table: pc.greater(table[col], pc.scalar(value)),
        )

    @staticmethod
    def ge(col: str, value: ScalarValue) -> PredicateExpr:
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
        expr = ensure_expression(pc.greater_equal(E.field(col), E.scalar(value)))
        return PredicateExpr(
            expr=expr,
            mask_fn=lambda table: pc.greater_equal(table[col], pc.scalar(value)),
        )

    @staticmethod
    def between(col: str, lo: ScalarValue, hi: ScalarValue) -> PredicateExpr:
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
        return E.and_(E.ge(col, lo), E.le(col, hi))

    @staticmethod
    def in_(col: str, values: Sequence[ScalarValue]) -> PredicateExpr:
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
        value_set = list(values)
        expr = ensure_expression(pc.is_in(E.field(col), value_set=value_set))
        return PredicateExpr(
            expr=expr, mask_fn=lambda table: pc.is_in(table[col], value_set=value_set)
        )

    @staticmethod
    def is_null(col: str) -> PredicateExpr:
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
        expr = ensure_expression(pc.is_null(E.field(col)))
        return PredicateExpr(expr=expr, mask_fn=lambda table: pc.is_null(table[col]))

    @staticmethod
    def is_valid(col: str) -> PredicateExpr:
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
        expr = ensure_expression(pc.is_valid(E.field(col)))
        return PredicateExpr(expr=expr, mask_fn=lambda table: pc.is_valid(table[col]))

    @staticmethod
    def and_(*preds: PredicateSpec) -> PredicateExpr:
        """Combine predicates with logical AND.

        Parameters
        ----------
        *preds:
            Predicate specs to combine.

        Returns
        -------
        PredicateExpr
            Combined predicate expression.

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

        def mask_fn(table: TableLike) -> ArrayLike:
            out = preds[0].mask(table)
            for pred in preds[1:]:
                out = pc.and_(out, pred.mask(table))
            return out

        return PredicateExpr(expr=expr_out, mask_fn=mask_fn)

    @staticmethod
    def or_(*preds: PredicateSpec) -> PredicateExpr:
        """Combine predicates with logical OR.

        Parameters
        ----------
        *preds:
            Predicate specs to combine.

        Returns
        -------
        PredicateExpr
            Combined predicate expression.

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

        def mask_fn(table: TableLike) -> ArrayLike:
            out = preds[0].mask(table)
            for pred in preds[1:]:
                out = pc.or_(out, pred.mask(table))
            return out

        return PredicateExpr(expr=expr_out, mask_fn=mask_fn)

    @staticmethod
    def not_(pred: PredicateSpec) -> PredicateExpr:
        """Negate a predicate expression.

        Parameters
        ----------
        pred:
            Predicate to negate.

        Returns
        -------
        PredicateExpr
            Negated predicate expression.
        """
        expr = ensure_expression(pc.invert(pred.to_expression()))
        return PredicateExpr(expr=expr, mask_fn=lambda table: pc.invert(pred.mask(table)))

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
