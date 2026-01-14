"""SQLGlot optimization helpers."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TypedDict, Unpack, cast

from sqlglot import Dialect, ErrorLevel, Expression, exp
from sqlglot.generator import Generator as SqlGlotGenerator
from sqlglot.optimizer import optimize
from sqlglot.optimizer.qualify import qualify

SchemaMapping = Mapping[str, Mapping[str, str]]


class GeneratorInitKwargs(TypedDict, total=False):
    pretty: bool | None
    identify: bool | str
    normalize: bool
    pad: int
    indent: int
    normalize_functions: bool | str | None
    unsupported_level: ErrorLevel
    max_unsupported: int
    leading_comma: bool
    max_text_width: int
    comments: bool
    dialect: Dialect | str | type[Dialect] | None


@dataclass(frozen=True)
class CanonicalizationRules:
    """Rules for canonicalizing SQLGlot expressions."""

    column_renames: Mapping[str, str] = field(default_factory=dict)
    function_renames: Mapping[str, str] = field(default_factory=dict)


def canonicalize_expr(
    expr: Expression, *, rules: CanonicalizationRules | None = None
) -> Expression:
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


def _array_transform(generator: SqlGlotGenerator, expression: Expression) -> str:
    return "ARRAY[" + generator.expressions(expression) + "]"


class DataFusionGenerator(SqlGlotGenerator):
    """SQL generator overrides for DataFusion."""

    def __init__(self, **kwargs: Unpack[GeneratorInitKwargs]) -> None:
        super().__init__(**kwargs)
        self.TRANSFORMS = {**self.TRANSFORMS, exp.Array: _array_transform}


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
    rewrite_hook: Callable[[Expression], Expression] | None = None,
    enable_rewrites: bool = True,
) -> Expression:
    """Return a qualified + optimized SQLGlot expression.

    Returns
    -------
    sqlglot.Expression
        Normalized expression tree.
    """
    canonical = canonicalize_expr(expr, rules=rules)
    rewritten = rewrite_expr(canonical, rewrite_hook=rewrite_hook, enabled=enable_rewrites)
    qualified = qualify_expr(rewritten, schema=schema)
    return optimize_expr(qualified, schema=schema)


def rewrite_expr(
    expr: Expression,
    *,
    rewrite_hook: Callable[[Expression], Expression] | None = None,
    enabled: bool = True,
) -> Expression:
    """Return a SQLGlot expression after applying a rewrite hook.

    Returns
    -------
    sqlglot.Expression
        Rewritten expression tree.
    """
    if not enabled or rewrite_hook is None:
        return expr
    return rewrite_hook(expr)


def bind_params(expr: Expression, *, params: Mapping[str, object]) -> Expression:
    """Return a SQLGlot expression with parameter nodes bound to literals.

    Returns
    -------
    sqlglot.Expression
        Expression tree with parameters replaced by literals.
    """
    if not params:
        return expr

    def _rewrite(node: Expression) -> Expression:
        if isinstance(node, exp.Parameter):
            name = _param_name(node)
            if name not in params:
                msg = f"Missing parameter binding for {name!r}."
                raise KeyError(msg)
            return _literal_from_value(params[name])
        return node

    return expr.transform(_rewrite)


def _param_name(node: exp.Parameter) -> str:
    name = getattr(node, "name", None)
    if isinstance(name, str):
        return name
    value = node.this
    return value if isinstance(value, str) else str(value)


def _literal_from_value(value: object) -> Expression:
    if value is None:
        return exp.Null()
    if isinstance(value, bool):
        return exp.Boolean(this=value)
    if isinstance(value, int):
        return exp.Literal.number(value)
    if isinstance(value, float):
        return exp.Literal.number(value)
    return exp.Literal.string(str(value))


__all__ = [
    "CanonicalizationRules",
    "DataFusionDialect",
    "SchemaMapping",
    "bind_params",
    "canonicalize_expr",
    "normalize_expr",
    "optimize_expr",
    "qualify_expr",
    "register_datafusion_dialect",
    "rewrite_expr",
]
