"""DataFusion-native expression helpers for quality-aware relationships.

This module provides expression-building utilities that enable declarative
specification of join predicates, features, and ranking without SQL strings.
All helpers produce DataFusion Expr objects directly.

Usage
-----
>>> from semantics.exprs import ExprContextImpl, clamp, c, v

>>> ctx = ExprContextImpl(left_alias="l", right_alias="r")
>>> expr = clamp(c("score")(ctx), min_value=v(0.0)(ctx), max_value=v(1.0)(ctx))
"""

from __future__ import annotations

from collections.abc import Callable, Collection
from typing import TYPE_CHECKING, Literal, Protocol

import pyarrow as pa
from datafusion import col, lit
from datafusion import functions as f

from datafusion_engine.udf.shims import stable_hash64 as _stable_hash64

if TYPE_CHECKING:
    from datafusion import Expr


class ExprContext(Protocol):
    """Expression context for building DataFusion Expr objects.

    Provides a consistent interface for resolving column references,
    handling left/right table aliases in joins, and creating literals.
    """

    def col(self, name: str) -> Expr:
        """Get column expression, resolving left/right aliases.

        Parameters
        ----------
        name
            Column name, optionally prefixed with alias indicator.

        Returns
        -------
        Expr
            DataFusion column expression.
        """
        ...

    def lit(self, value: object) -> Expr:
        """Create literal expression.

        Parameters
        ----------
        value
            Value to wrap as literal.

        Returns
        -------
        Expr
            DataFusion literal expression.
        """
        ...

    @property
    def left_alias(self) -> str:
        """Left table alias prefix."""
        ...

    @property
    def right_alias(self) -> str:
        """Right table alias prefix."""
        ...


type ExprSpec = Callable[[ExprContext], Expr]
"""Callable that builds an Expr given an ExprContext."""

type SortExprSpec = Callable[[ExprContext], object]
"""Callable that builds a SortExpr given an ExprContext.

Note: Returns object since SortExpr is a separate type from Expr
in DataFusion, used specifically for ORDER BY clauses.
"""


class ExprContextImpl:
    """Expression context with left/right table alias resolution.

    Resolves column references to their aliased forms when columns
    come from joined tables. The alias format uses double-underscore
    separation: ``l__column_name`` or ``r__column_name``.

    Parameters
    ----------
    left_alias
        Prefix for left table columns (default: "l").
    right_alias
        Prefix for right table columns (default: "r").

    Examples
    --------
    >>> ctx = ExprContextImpl(left_alias="l", right_alias="r")
    >>> ctx.col("l__file_id")  # Returns col("l__file_id")
    >>> ctx.col("score")  # Returns col("score") - no prefix
    """

    def __init__(self, left_alias: str = "l", right_alias: str = "r") -> None:
        self._left_alias = left_alias
        self._right_alias = right_alias

    @property
    def left_alias(self) -> str:
        """Left table alias prefix."""
        return self._left_alias

    @property
    def right_alias(self) -> str:
        """Right table alias prefix."""
        return self._right_alias

    @staticmethod
    def col(name: str) -> Expr:
        """Resolve column, checking for left/right prefixes.

        Parameters
        ----------
        name
            Column name. If prefixed with ``l__`` or ``r__``,
            returns the column as-is. Otherwise returns unprefixed.

        Returns
        -------
        Expr
            DataFusion column expression.
        """
        return col(name)

    @staticmethod
    def lit(value: object) -> Expr:
        """Create literal expression.

        Parameters
        ----------
        value
            Value to wrap as literal.

        Returns
        -------
        Expr
            DataFusion literal expression.
        """
        return lit(value)

    def left_col(self, name: str) -> Expr:
        """Get a column from the left table with alias prefix.

        Parameters
        ----------
        name
            Column name (without alias prefix).

        Returns
        -------
        Expr
            Column expression with left alias prefix.
        """
        return col(f"{self._left_alias}__{name}")

    def right_col(self, name: str) -> Expr:
        """Get a column from the right table with alias prefix.

        Parameters
        ----------
        name
            Column name (without alias prefix).

        Returns
        -------
        Expr
            Column expression with right alias prefix.
        """
        return col(f"{self._right_alias}__{name}")


class ExprValidationContext:
    """Expression context that records referenced columns for validation."""

    def __init__(self, left_alias: str = "l", right_alias: str = "r") -> None:
        self._left_alias = left_alias
        self._right_alias = right_alias
        self._columns: set[str] = set()

    @property
    def left_alias(self) -> str:
        """Left table alias prefix."""
        return self._left_alias

    @property
    def right_alias(self) -> str:
        """Right table alias prefix."""
        return self._right_alias

    def col(self, name: str) -> Expr:
        """Record and return a column expression.

        Returns
        -------
        Expr
            Column expression.
        """
        self._columns.add(name)
        return col(name)

    @staticmethod
    def lit(value: object) -> Expr:
        """Create literal expression.

        Returns
        -------
        Expr
            Literal expression.
        """
        return lit(value)

    def used_columns(self) -> set[str]:
        """Return referenced column names.

        Returns
        -------
        set[str]
            Referenced column names.
        """
        return set(self._columns)


# ---------------------------------------------------------------------------
# Clamp utility
# ---------------------------------------------------------------------------


def clamp(value: Expr, *, min_value: Expr, max_value: Expr) -> Expr:
    """Clamp value to [min, max] using DataFusion expressions.

    Uses ``greatest(least(value, max), min)`` pattern for bounds clamping.

    Parameters
    ----------
    value
        Expression to clamp.
    min_value
        Minimum bound expression.
    max_value
        Maximum bound expression.

    Returns
    -------
    Expr
        Clamped expression.

    Examples
    --------
    >>> clamped = clamp(col("score"), min_value=lit(0.0), max_value=lit(1.0))
    """
    # Implement clamp using CASE WHEN since greatest/least may not be available
    return f.when(value < min_value, min_value).when(value > max_value, max_value).otherwise(value)


# ---------------------------------------------------------------------------
# DSL Helpers - concise expression builders
# ---------------------------------------------------------------------------


def c(name: str) -> ExprSpec:
    """Column reference helper.

    Parameters
    ----------
    name
        Column name.

    Returns
    -------
    ExprSpec
        Callable that returns a column expression.

    Examples
    --------
    >>> spec = c("file_id")
    >>> expr = spec(ctx)  # Returns col("file_id")
    """

    def _build(ctx: ExprContext) -> Expr:
        return ctx.col(name)

    return _build


def v(value: object) -> ExprSpec:
    """Literal value helper.

    Parameters
    ----------
    value
        Value to wrap as literal.

    Returns
    -------
    ExprSpec
        Callable that returns a literal expression.

    Examples
    --------
    >>> spec = v(0.95)
    >>> expr = spec(ctx)  # Returns lit(0.95)
    """

    def _build(ctx: ExprContext) -> Expr:
        return ctx.lit(value)

    return _build


def eq_value(column: str, value: object) -> ExprSpec:
    """Equality predicate between a column and a literal value.

    Parameters
    ----------
    column
        Column name.
    value
        Literal value to compare against.

    Returns
    -------
    ExprSpec
        Callable that returns an equality expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        return ctx.col(column) == ctx.lit(value)

    return _build


def truthy_expr(column: str) -> ExprSpec:
    """Predicate for truthy flag columns, tolerant of string/boolean values.

    Parameters
    ----------
    column
        Column name.

    Returns
    -------
    ExprSpec
        Callable that returns a truthy predicate expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        return f.lower(ctx.col(column).cast(pa.string())) == ctx.lit("true")

    return _build


def eq(left: str, right: str) -> ExprSpec:
    """Equality predicate between two columns.

    Parameters
    ----------
    left
        Left column name.
    right
        Right column name.

    Returns
    -------
    ExprSpec
        Callable that returns an equality expression.

    Examples
    --------
    >>> spec = eq("owner_def_id", "def_id")
    >>> expr = spec(ctx)  # Returns col("owner_def_id") == col("def_id")
    """

    def _build(ctx: ExprContext) -> Expr:
        return ctx.col(left) == ctx.col(right)

    return _build


def gt(left: str, right: str) -> ExprSpec:
    """Greater-than predicate between two columns.

    Parameters
    ----------
    left
        Left column name.
    right
        Right column name.

    Returns
    -------
    ExprSpec
        Callable that returns a greater-than expression.

    Examples
    --------
    >>> spec = gt("score", "threshold")
    >>> expr = spec(ctx)  # Returns col("score") > col("threshold")
    """

    def _build(ctx: ExprContext) -> Expr:
        return ctx.col(left) > ctx.col(right)

    return _build


def lt(left: str, right: str) -> ExprSpec:
    """Less-than predicate between two columns.

    Parameters
    ----------
    left
        Left column name.
    right
        Right column name.

    Returns
    -------
    ExprSpec
        Callable that returns a less-than expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        return ctx.col(left) < ctx.col(right)

    return _build


def gte(left: str, right: str) -> ExprSpec:
    """Greater-than-or-equal predicate between two columns.

    Parameters
    ----------
    left
        Left column name.
    right
        Right column name.

    Returns
    -------
    ExprSpec
        Callable that returns a gte expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        return ctx.col(left) >= ctx.col(right)

    return _build


def lte(left: str, right: str) -> ExprSpec:
    """Less-than-or-equal predicate between two columns.

    Parameters
    ----------
    left
        Left column name.
    right
        Right column name.

    Returns
    -------
    ExprSpec
        Callable that returns a lte expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        return ctx.col(left) <= ctx.col(right)

    return _build


def is_not_null(column: str) -> ExprSpec:
    """Not-null predicate for a column.

    Parameters
    ----------
    column
        Column name.

    Returns
    -------
    ExprSpec
        Callable that returns an is_not_null expression.

    Examples
    --------
    >>> spec = is_not_null("owner_def_id")
    >>> expr = spec(ctx)  # Returns col("owner_def_id").is_not_null()
    """

    def _build(ctx: ExprContext) -> Expr:
        return ctx.col(column).is_not_null()

    return _build


def is_null(column: str) -> ExprSpec:
    """Null predicate for a column.

    Parameters
    ----------
    column
        Column name.

    Returns
    -------
    ExprSpec
        Callable that returns an is_null expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        return ctx.col(column).is_null()

    return _build


def and_(*predicates: ExprSpec) -> ExprSpec:
    """Logical AND of multiple predicates.

    Parameters
    ----------
    *predicates
        Predicate specs to combine.

    Returns
    -------
    ExprSpec
        Callable that returns an AND expression.

    Examples
    --------
    >>> spec = and_(is_not_null("a"), eq("a", "b"))
    """

    def _build(ctx: ExprContext) -> Expr:
        if not predicates:
            return ctx.lit(value=True)
        result = predicates[0](ctx)
        for pred in predicates[1:]:
            result &= pred(ctx)
        return result

    return _build


def or_(*predicates: ExprSpec) -> ExprSpec:
    """Logical OR of multiple predicates.

    Parameters
    ----------
    *predicates
        Predicate specs to combine.

    Returns
    -------
    ExprSpec
        Callable that returns an OR expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        if not predicates:
            return ctx.lit(value=False)
        result = predicates[0](ctx)
        for pred in predicates[1:]:
            result |= pred(ctx)
        return result

    return _build


def between_overlap(
    left_start: str,
    left_end: str,
    right_start: str,
    right_end: str,
) -> ExprSpec:
    """Span overlap predicate.

    Returns true if the two spans overlap (neither is completely before
    or after the other).

    Parameters
    ----------
    left_start
        Start column of left span.
    left_end
        End column of left span.
    right_start
        Start column of right span.
    right_end
        End column of right span.

    Returns
    -------
    ExprSpec
        Callable that returns an overlap expression.

    Examples
    --------
    >>> spec = between_overlap("l_bstart", "l_bend", "r_bstart", "r_bend")
    """

    def _build(ctx: ExprContext) -> Expr:
        # Overlap: NOT (left_end <= right_start OR right_end <= left_start)
        # Equivalent: left_start < right_end AND right_start < left_end
        l_start = ctx.col(left_start)
        l_end = ctx.col(left_end)
        r_start = ctx.col(right_start)
        r_end = ctx.col(right_end)
        return (l_start < r_end) & (r_start < l_end)

    return _build


def span_contains_span(
    outer_start: str,
    outer_end: str,
    inner_start: str,
    inner_end: str,
) -> ExprSpec:
    """Span containment predicate.

    Returns true if the outer span fully contains the inner span.

    Parameters
    ----------
    outer_start
        Start column of outer (containing) span.
    outer_end
        End column of outer span.
    inner_start
        Start column of inner (contained) span.
    inner_end
        End column of inner span.

    Returns
    -------
    ExprSpec
        Callable that returns a containment expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        o_start = ctx.col(outer_start)
        o_end = ctx.col(outer_end)
        i_start = ctx.col(inner_start)
        i_end = ctx.col(inner_end)
        return (o_start <= i_start) & (i_end <= o_end)

    return _build


def span_start_expr(span_column: str) -> ExprSpec:
    """Extract span start from a span struct column.

    Parameters
    ----------
    span_column
        Column name containing the span struct.

    Returns
    -------
    ExprSpec
        Callable that returns the span start expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        from datafusion_engine.udf.shims import span_start

        return span_start(ctx.col(span_column))

    return _build


def span_end_expr(span_column: str) -> ExprSpec:
    """Extract span end from a span struct column.

    Parameters
    ----------
    span_column
        Column name containing the span struct.

    Returns
    -------
    ExprSpec
        Callable that returns the span end expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        from datafusion_engine.udf.shims import span_end

        return span_end(ctx.col(span_column))

    return _build


def span_overlaps_expr(left_span: str, right_span: str) -> ExprSpec:
    """Span overlap predicate using span struct columns.

    Parameters
    ----------
    left_span
        Column name for the left span struct.
    right_span
        Column name for the right span struct.

    Returns
    -------
    ExprSpec
        Callable that returns an overlap expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        from datafusion_engine.udf.shims import span_overlaps

        return span_overlaps(ctx.col(left_span), ctx.col(right_span))

    return _build


def span_contains_expr(outer_span: str, inner_span: str) -> ExprSpec:
    """Span containment predicate using span struct columns.

    Parameters
    ----------
    outer_span
        Column name for the outer span struct.
    inner_span
        Column name for the inner span struct.

    Returns
    -------
    ExprSpec
        Callable that returns a containment expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        from datafusion_engine.udf.shims import span_contains

        return span_contains(ctx.col(outer_span), ctx.col(inner_span))

    return _build


def eq_expr(left: ExprSpec, right: ExprSpec) -> ExprSpec:
    """Equality predicate between two expressions.

    Parameters
    ----------
    left
        Left expression spec.
    right
        Right expression spec.

    Returns
    -------
    ExprSpec
        Callable that returns an equality expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        return left(ctx) == right(ctx)

    return _build


def case_eq_expr(left: ExprSpec, right: ExprSpec) -> ExprSpec:
    """Case expression returning 1 if expressions are equal, 0 otherwise.

    Parameters
    ----------
    left
        Left expression spec.
    right
        Right expression spec.

    Returns
    -------
    ExprSpec
        Callable that returns a case expression (1 or 0).
    """

    def _build(ctx: ExprContext) -> Expr:
        return f.when(left(ctx) == right(ctx), ctx.lit(1)).otherwise(ctx.lit(0))

    return _build


def is_not_null_expr(expr: ExprSpec) -> ExprSpec:
    """Predicate returning True if expression is not null.

    Parameters
    ----------
    expr
        Expression spec to evaluate.

    Returns
    -------
    ExprSpec
        Callable that returns a not-null expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        return expr(ctx).is_not_null()

    return _build


def is_null_expr(expr: ExprSpec) -> ExprSpec:
    """Predicate returning True if expression is null.

    Parameters
    ----------
    expr
        Expression spec to evaluate.

    Returns
    -------
    ExprSpec
        Callable that returns a null expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        return expr(ctx).is_null()

    return _build


def case_eq(column: str, match_column: str) -> ExprSpec:
    """Case expression returning 1 if columns match, 0 otherwise.

    Useful for computing match features.

    Parameters
    ----------
    column
        First column name.
    match_column
        Second column name.

    Returns
    -------
    ExprSpec
        Callable that returns a case expression (1 or 0).

    Examples
    --------
    >>> spec = case_eq("owner_kind", "def_kind")
    >>> # Returns 1 when owner_kind == def_kind, 0 otherwise
    """

    def _build(ctx: ExprContext) -> Expr:
        return f.when(ctx.col(column) == ctx.col(match_column), ctx.lit(1)).otherwise(ctx.lit(0))

    return _build


def case_eq_value(column: str, value: object) -> ExprSpec:
    """Case expression returning 1 if column equals value, 0 otherwise.

    Parameters
    ----------
    column
        Column name.
    value
        Value to compare against.

    Returns
    -------
    ExprSpec
        Callable that returns a case expression (1 or 0).

    Examples
    --------
    >>> spec = case_eq_value("kind", "function")
    """

    def _build(ctx: ExprContext) -> Expr:
        return f.when(ctx.col(column) == ctx.lit(value), ctx.lit(1)).otherwise(ctx.lit(0))

    return _build


def coalesce_default(column: str, default: object) -> ExprSpec:
    """Coalesce column with default value.

    Parameters
    ----------
    column
        Column name.
    default
        Default value if null.

    Returns
    -------
    ExprSpec
        Callable that returns a coalesce expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        return f.coalesce(ctx.col(column), ctx.lit(default))

    return _build


def add_columns(*columns: str) -> ExprSpec:
    """Sum of multiple columns.

    Parameters
    ----------
    *columns
        Column names to sum.

    Returns
    -------
    ExprSpec
        Callable that returns a sum expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        if not columns:
            return ctx.lit(0)
        result = ctx.col(columns[0])
        for col_name in columns[1:]:
            result += ctx.col(col_name)
        return result

    return _build


def mul(column: str, factor: float) -> ExprSpec:
    """Multiply column by a factor.

    Parameters
    ----------
    column
        Column name.
    factor
        Multiplication factor.

    Returns
    -------
    ExprSpec
        Callable that returns a multiplication expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        return ctx.col(column) * ctx.lit(factor)

    return _build


def stable_hash64(column: str) -> ExprSpec:
    """Stable 64-bit hash of a column.

    Parameters
    ----------
    column
        Column name to hash.

    Returns
    -------
    ExprSpec
        Callable that returns a stable hash expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        return _stable_hash64(ctx.col(column))

    return _build


def alias(expr_spec: ExprSpec, name: str) -> ExprSpec:
    """Wrap an expression spec with an alias.

    Parameters
    ----------
    expr_spec
        Expression spec to alias.
    name
        Alias name.

    Returns
    -------
    ExprSpec
        Callable that returns an aliased expression.
    """

    def _build(ctx: ExprContext) -> Expr:
        return expr_spec(ctx).alias(name)

    return _build


# Type for sort direction
SortDirection = Literal["asc", "desc"]


def sort_expr(column: str, direction: SortDirection = "asc") -> SortExprSpec:
    """Create a sort expression for the column.

    Parameters
    ----------
    column
        Column name.
    direction
        Sort direction ("asc" or "desc").

    Returns
    -------
    SortExprSpec
        Callable that returns a sorted expression for ORDER BY.
    """

    def _build(ctx: ExprContext) -> object:
        return ctx.col(column).sort(ascending=(direction == "asc"))

    return _build


_NONDETERMINISTIC_TOKENS: tuple[str, ...] = (
    "random",
    "rand(",
    "uuid",
    "now()",
    "current_timestamp",
    "current_date",
    "clock_timestamp",
)


def validate_expr_spec(
    expr_spec: ExprSpec,
    *,
    available_columns: Collection[str],
    expr_label: str,
) -> None:
    """Validate that an ExprSpec references only known columns and is deterministic.

    Raises
    ------
    ValueError
        Raised when referenced columns are missing or the expression is
        detected as non-deterministic.
    """
    ctx = ExprValidationContext()
    expr = expr_spec(ctx)
    used = ctx.used_columns()
    missing = sorted(name for name in used if name not in available_columns)
    if missing:
        msg = (
            f"ExprSpec {expr_label} references missing columns {missing!r}; "
            f"available columns: {sorted(available_columns)!r}."
        )
        raise ValueError(msg)
    expr_repr = str(expr).lower()
    nondeterministic = [token for token in _NONDETERMINISTIC_TOKENS if token in expr_repr]
    if nondeterministic:
        msg = (
            f"ExprSpec {expr_label} appears non-deterministic due to tokens "
            f"{sorted(set(nondeterministic))!r}."
        )
        raise ValueError(msg)


__all__ = [
    "ExprContext",
    "ExprContextImpl",
    "ExprSpec",
    "ExprValidationContext",
    "SortDirection",
    "SortExprSpec",
    "add_columns",
    "alias",
    "and_",
    "between_overlap",
    "c",
    "case_eq",
    "case_eq_expr",
    "case_eq_value",
    "clamp",
    "coalesce_default",
    "eq",
    "eq_expr",
    "eq_value",
    "gt",
    "gte",
    "is_not_null",
    "is_not_null_expr",
    "is_null",
    "is_null_expr",
    "lt",
    "lte",
    "mul",
    "or_",
    "sort_expr",
    "span_contains_expr",
    "span_contains_span",
    "span_end_expr",
    "span_overlaps_expr",
    "span_start_expr",
    "stable_hash64",
    "truthy_expr",
    "v",
    "validate_expr_spec",
]
