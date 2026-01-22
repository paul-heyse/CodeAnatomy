"""Compatibility shims for SQLGlot types and helpers."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import cast

import sqlglot
import sqlglot.expressions as exp
from sqlglot.dialects import Dialect
from sqlglot.errors import ErrorLevel
from sqlglot.expressions import Expression


def parse_one(*args: object, **kwargs: object) -> Expression:
    parser = getattr(sqlglot, "parse_one", None)
    if not callable(parser):
        msg = "sqlglot.parse_one is not available."
        raise RuntimeError(msg)
    return cast("Callable[..., Expression]", parser)(*args, **kwargs)


def diff(left: Expression, right: Expression) -> Iterable[object]:
    diff_fn = getattr(sqlglot, "diff", None)
    if not callable(diff_fn):
        msg = "sqlglot.diff is not available."
        raise RuntimeError(msg)
    return cast("Iterable[object]", diff_fn(left, right))


__all__ = ["Dialect", "ErrorLevel", "Expression", "diff", "exp", "parse_one"]
