"""Arrow compute expression helpers for predicates and projections."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    import pyarrow.compute as pc


@dataclass(frozen=True)
class E:
    """Expression macros for QuerySpec predicates and Acero filter/project nodes."""

    @staticmethod
    def field(name: str) -> pc.Expression:
        """Return a field expression for a column name.

        Returns
        -------
        pc.Expression
            Expression referencing the column.
        """
        import pyarrow.compute as pc

        return pc.field(name)

    @staticmethod
    def scalar(value: Any) -> pc.Expression:
        """Return a scalar expression for a literal value.

        Returns
        -------
        pc.Expression
            Expression representing the literal.
        """
        import pyarrow.compute as pc

        return pc.scalar(value)

    # -------------
    # Predicates
    # -------------

    @staticmethod
    def eq(col: str, value: Any) -> pc.Expression:
        """Return an equality predicate for a column and literal.

        Returns
        -------
        pc.Expression
            Equality predicate expression.
        """
        return E.field(col) == E.scalar(value)

    @staticmethod
    def ne(col: str, value: Any) -> pc.Expression:
        """Return a not-equal predicate for a column and literal.

        Returns
        -------
        pc.Expression
            Not-equal predicate expression.
        """
        return E.field(col) != E.scalar(value)

    @staticmethod
    def lt(col: str, value: Any) -> pc.Expression:
        """Return a less-than predicate for a column and literal.

        Returns
        -------
        pc.Expression
            Less-than predicate expression.
        """
        return E.field(col) < E.scalar(value)

    @staticmethod
    def le(col: str, value: Any) -> pc.Expression:
        """Return a less-than-or-equal predicate for a column and literal.

        Returns
        -------
        pc.Expression
            Less-than-or-equal predicate expression.
        """
        return E.field(col) <= E.scalar(value)

    @staticmethod
    def gt(col: str, value: Any) -> pc.Expression:
        """Return a greater-than predicate for a column and literal.

        Returns
        -------
        pc.Expression
            Greater-than predicate expression.
        """
        return E.field(col) > E.scalar(value)

    @staticmethod
    def ge(col: str, value: Any) -> pc.Expression:
        """Return a greater-than-or-equal predicate for a column and literal.

        Returns
        -------
        pc.Expression
            Greater-than-or-equal predicate expression.
        """
        return E.field(col) >= E.scalar(value)

    @staticmethod
    def between(col: str, lo: Any, hi: Any) -> pc.Expression:
        """Return a range predicate with inclusive bounds.

        Returns
        -------
        pc.Expression
            Range predicate expression.
        """
        return (E.field(col) >= E.scalar(lo)) & (E.field(col) <= E.scalar(hi))

    @staticmethod
    def in_(col: str, values: Sequence[Any]) -> pc.Expression:
        """Return a membership predicate for a column and values.

        Returns
        -------
        pc.Expression
            Membership predicate expression.
        """
        return E.field(col).isin(list(values))

    @staticmethod
    def is_null(col: str) -> pc.Expression:
        """Return a null-check predicate for a column.

        Returns
        -------
        pc.Expression
            Null-check predicate expression.
        """
        return E.field(col).is_null()

    @staticmethod
    def is_valid(col: str) -> pc.Expression:
        """Return a validity-check predicate for a column.

        Returns
        -------
        pc.Expression
            Validity-check predicate expression.
        """
        return E.field(col).is_valid()

    # -------------
    # Boolean ops
    # -------------

    @staticmethod
    def and_(*exprs: pc.Expression) -> pc.Expression:
        """Combine expressions with boolean AND.

        Returns
        -------
        pc.Expression
            Conjunction of input expressions.
        """
        out = exprs[0]
        for e in exprs[1:]:
            out = out & e
        return out

    @staticmethod
    def or_(*exprs: pc.Expression) -> pc.Expression:
        """Combine expressions with boolean OR.

        Returns
        -------
        pc.Expression
            Disjunction of input expressions.
        """
        out = exprs[0]
        for e in exprs[1:]:
            out = out | e
        return out

    @staticmethod
    def not_(expr: pc.Expression) -> pc.Expression:
        """Negate an expression with boolean NOT.

        Returns
        -------
        pc.Expression
            Negated expression.
        """
        return ~expr

    # -------------
    # Projection helpers
    # -------------

    @staticmethod
    def cast(expr: str | pc.Expression, target_type: Any, *, safe: bool = True) -> pc.Expression:
        """Cast an expression to a target Arrow type.

        Returns
        -------
        pc.Expression
            Cast expression.
        """
        import pyarrow.compute as pc

        e = E.field(expr) if isinstance(expr, str) else expr
        return pc.cast(e, target_type, safe=safe)

    @staticmethod
    def coalesce(*exprs: str | pc.Expression) -> pc.Expression:
        """Coalesce expressions in order of non-null values.

        Returns
        -------
        pc.Expression
            Coalesced expression.
        """
        import pyarrow.compute as pc

        es = [E.field(e) if isinstance(e, str) else e for e in exprs]
        return pc.coalesce(*es)
