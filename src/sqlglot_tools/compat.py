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


__all__ = ["Dialect", "DialectType", "ErrorLevel", "Expression", "diff", "exp", "parse_one"]
