"""Compatibility shims for SQLGlot types and helpers."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, cast

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ErrorLevel
from sqlglot.expressions import Expression

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import Dialect as DialectType
else:
    type DialectType = object

try:
    from sqlglot.dialects.dialect import Dialect as _Dialect
except ImportError:  # pragma: no cover - optional sqlglot surface
    _Dialect = object

Dialect = cast("type[DialectType]", _Dialect)


def parse_one(*args: object, **kwargs: object) -> Expression:
    """Return SQLGlot parse_one when available.

    Returns
    -------
    Expression
        Parsed SQLGlot expression.

    Raises
    ------
    TypeError
        Raised when sqlglot.parse_one is unavailable.
    """
    parser = getattr(sqlglot, "parse_one", None)
    if not callable(parser):
        msg = "sqlglot.parse_one is not available."
        raise TypeError(msg)
    return cast("Callable[..., Expression]", parser)(*args, **kwargs)


def diff(left: Expression, right: Expression) -> Iterable[object]:
    """Return SQLGlot diff results when available.

    Returns
    -------
    Iterable[object]
        Diff entries between the expressions.

    Raises
    ------
    TypeError
        Raised when sqlglot.diff is unavailable.
    """
    diff_fn = getattr(sqlglot, "diff", None)
    if not callable(diff_fn):
        msg = "sqlglot.diff is not available."
        raise TypeError(msg)
    return cast("Iterable[object]", diff_fn(left, right))


def pushdown_predicates_transform(expr: Expression) -> Expression:
    """Return an expression with predicates pushed down when supported.

    Returns
    -------
    Expression
        SQLGlot expression with predicate pushdown applied when available.
    """
    try:
        from sqlglot.optimizer.pushdown_predicates import (
            pushdown_predicates as _pushdown_predicates,
        )
    except ImportError:
        return expr
    return _pushdown_predicates(expr)


def pushdown_projections_transform(expr: Expression) -> Expression:
    """Return an expression with projections pushed down when supported.

    Returns
    -------
    Expression
        SQLGlot expression with projection pushdown applied when available.
    """
    try:
        from sqlglot.optimizer.pushdown_projections import (
            pushdown_projections as _pushdown_projections,
        )
    except ImportError:
        return expr
    return _pushdown_projections(expr)


def add_recursive_cte_column_names_transform(expr: Expression) -> Expression:
    """Return an expression with recursive CTE column names when supported.

    Returns
    -------
    Expression
        SQLGlot expression with recursive CTE columns applied when available.
    """
    try:
        from sqlglot.transforms import (
            add_recursive_cte_column_names as _add_recursive_cte_column_names,
        )
    except ImportError:
        return expr
    return _add_recursive_cte_column_names(expr)


__all__ = [
    "Dialect",
    "DialectType",
    "ErrorLevel",
    "Expression",
    "add_recursive_cte_column_names_transform",
    "diff",
    "exp",
    "parse_one",
    "pushdown_predicates_transform",
    "pushdown_projections_transform",
]
