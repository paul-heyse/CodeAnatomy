"""SQLGlot optimization helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import ClassVar, cast

from sqlglot import Dialect, Expression, exp
from sqlglot.generator import Generator as SqlGlotGenerator
from sqlglot.optimizer import optimize
from sqlglot.optimizer.qualify import qualify

SchemaMapping = Mapping[str, Mapping[str, str]]


@dataclass(frozen=True)
class CanonicalizationRules:
    """Rules for canonicalizing SQLGlot expressions."""

    column_renames: Mapping[str, str] = field(default_factory=dict)
    function_renames: Mapping[str, str] = field(default_factory=dict)


def canonicalize_expr(expr: Expression, *, rules: CanonicalizationRules | None = None) -> Expression:
    """Return a canonicalized SQLGlot expression.

    Applies deterministic renames for columns and functions when rules are provided.

    Returns
    -------
    sqlglot.Expression
        Canonicalized expression tree.
    """
    if rules is None:
        return expr
    rules_local = rules

    def _rewrite(node: Expression) -> Expression:
        if isinstance(node, exp.Column):
            target = rules_local.column_renames.get(node.name)
            if target is None:
                return node
            return exp.column(target, table=node.table)
        if isinstance(node, exp.Func):
            name = node.sql_name().lower()
            target = rules_local.function_renames.get(name)
            if target is None:
                return node
            return exp.Anonymous(this=target, expressions=list(node.expressions))
        return node

    return expr.transform(_rewrite)


class DataFusionGenerator(SqlGlotGenerator):
    """SQL generator overrides for DataFusion."""

    TRANSFORMS: ClassVar[dict[type[Expression], object]] = {
        **SqlGlotGenerator.TRANSFORMS,
        exp.Array: lambda self, e: "ARRAY[" + self.expressions(e) + "]",
    }


class DataFusionDialect(Dialect):
    """Optional DataFusion-focused dialect overrides."""

    generator_class = DataFusionGenerator


def register_datafusion_dialect(name: str = "datafusion_ext") -> None:
    """Register the DataFusion dialect overrides under a custom name."""
    Dialect.classes[name] = DataFusionDialect


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


def normalize_expr(
    expr: Expression,
    *,
    schema: SchemaMapping | None = None,
    rules: CanonicalizationRules | None = None,
) -> Expression:
    """Return a qualified + optimized SQLGlot expression.

    Returns
    -------
    sqlglot.Expression
        Normalized expression tree.
    """
    canonical = canonicalize_expr(expr, rules=rules)
    qualified = qualify_expr(canonical, schema=schema)
    return optimize_expr(qualified, schema=schema)


__all__ = [
    "CanonicalizationRules",
    "DataFusionDialect",
    "SchemaMapping",
    "canonicalize_expr",
    "normalize_expr",
    "optimize_expr",
    "qualify_expr",
    "register_datafusion_dialect",
]
