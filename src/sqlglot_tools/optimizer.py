"""SQLGlot optimization helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from sqlglot import Expression
from sqlglot.optimizer import optimize
from sqlglot.optimizer.qualify import qualify

SchemaMapping = Mapping[str, Mapping[str, str]]


def qualify_expr(expr: Expression, *, schema: SchemaMapping | None = None) -> Expression:
    """Return a qualified SQLGlot expression.

    Returns
    -------
    sqlglot.Expression
        Qualified expression with fully qualified columns.
    """
    if schema is None:
        return qualify(expr)
    schema_map = cast("dict[object, object]", schema)
    return qualify(expr, schema=schema_map)


def optimize_expr(expr: Expression, *, schema: SchemaMapping | None = None) -> Expression:
    """Return an optimized SQLGlot expression.

    Returns
    -------
    sqlglot.Expression
        Optimized expression tree.
    """
    if schema is None:
        return optimize(expr)
    schema_map = cast("dict[object, object]", schema)
    return optimize(expr, schema=schema_map)


def normalize_expr(expr: Expression, *, schema: SchemaMapping | None = None) -> Expression:
    """Return a qualified + optimized SQLGlot expression.

    Returns
    -------
    sqlglot.Expression
        Normalized expression tree.
    """
    qualified = qualify_expr(expr, schema=schema)
    return optimize_expr(qualified, schema=schema)
