"""SQLGlot to DataFusion DataFrame translator."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal, cast

from datafusion import SessionContext, col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr, SortExpr
from sqlglot import Expression, exp

from datafusion_engine.registry_bridge import register_dataset_df
from ibis_engine.registry import DatasetLocation


@dataclass(frozen=True)
class TranslationError(ValueError):
    """Raised when SQLGlot translation is not supported."""

    message: str

    def __str__(self) -> str:
        """Return the error message.

        Returns
        -------
        str
            Error message string.
        """
        return self.message


JoinType = Literal["inner", "left", "right", "full", "semi", "anti"]


_JOIN_TYPE_MAP: dict[str, JoinType] = {
    "inner": "inner",
    "left": "left",
    "right": "right",
    "full": "full",
    "semi": "semi",
    "anti": "anti",
}


def df_from_sqlglot(ctx: SessionContext, expr: Expression) -> DataFrame:
    """Translate a SQLGlot expression into a DataFusion DataFrame.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame for the provided SQLGlot expression.

    Raises
    ------
    TranslationError
        Raised when the expression cannot be translated.
    """
    if isinstance(expr, exp.Subquery):
        return df_from_sqlglot(ctx, expr.this)
    if isinstance(expr, exp.Select):
        return _select_to_df(ctx, expr)
    if isinstance(expr, exp.Union):
        left = df_from_sqlglot(ctx, expr.this)
        right = df_from_sqlglot(ctx, expr.expression)
        if expr.args.get("distinct") is False:
            return left.union(right)
        return left.union_distinct(right)
    msg = f"Unsupported SQLGlot root expression: {expr.__class__.__name__}."
    raise TranslationError(msg)


def register_dataset(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
) -> DataFrame:
    """Register a dataset location with DataFusion and return a DataFrame.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame for the registered dataset.

    Raises
    ------
    TranslationError
        Raised when the dataset format is not supported.
    """
    try:
        return register_dataset_df(ctx, name=name, location=location)
    except ValueError as exc:
        raise TranslationError(str(exc)) from exc


def _select_to_df(ctx: SessionContext, select: exp.Select) -> DataFrame:
    source = _from_expr(ctx, select.args.get("from"))
    df = _apply_joins(ctx, source, select.args.get("joins"))
    if select.args.get("where") is not None:
        df = df.filter(_expr_to_df(select.args["where"].this))
    if select.args.get("group") is not None or _has_aggregate(select.expressions):
        df = _apply_aggregate(df, select)
    else:
        df = df.select(*_select_exprs(select.expressions))
    if select.args.get("having") is not None:
        df = df.filter(_expr_to_df(select.args["having"].this))
    if select.args.get("order") is not None:
        df = df.sort(*_order_exprs(select.args["order"]))
    if select.args.get("limit") is not None:
        limit = cast("exp.Limit", select.args["limit"])
        count = _int_value(limit.expression)
        offset = _int_value(limit.args.get("offset"))
        df = df.limit(count, offset=offset)
    return df


def _from_expr(ctx: SessionContext, from_expr: exp.From | None) -> DataFrame:
    if from_expr is None or not from_expr.expressions:
        msg = "SELECT without FROM is not supported."
        raise TranslationError(msg)
    source = from_expr.expressions[0]
    if isinstance(source, exp.Table):
        return ctx.table(source.name)
    if isinstance(source, exp.Subquery):
        return df_from_sqlglot(ctx, source.this)
    msg = f"Unsupported FROM source: {source.__class__.__name__}."
    raise TranslationError(msg)


def _apply_joins(
    ctx: SessionContext,
    df: DataFrame,
    joins: Sequence[exp.Join] | None,
) -> DataFrame:
    if not joins:
        return df
    for join in joins:
        right = _join_source(ctx, join.this)
        how = _join_type(join)
        join_keys = _join_keys(join.args.get("on"))
        if join_keys is None:
            msg = "JOIN predicate does not contain equi-join columns."
            raise TranslationError(msg)
        df = df.join(
            right,
            join_keys=join_keys,
            how=how,
            coalesce_duplicate_keys=True,
        )
    return df


def _join_source(ctx: SessionContext, node: Expression) -> DataFrame:
    if isinstance(node, exp.Table):
        return ctx.table(node.name)
    if isinstance(node, exp.Subquery):
        return df_from_sqlglot(ctx, node.this)
    msg = f"Unsupported JOIN source: {node.__class__.__name__}."
    raise TranslationError(msg)


def _join_type(join: exp.Join) -> JoinType:
    join_kind = (join.args.get("kind") or "inner").lower()
    if join_kind in _JOIN_TYPE_MAP:
        return _JOIN_TYPE_MAP[join_kind]
    msg = f"Unsupported JOIN type: {join_kind!r}."
    raise TranslationError(msg)


def _join_keys(on_expr: Expression | None) -> tuple[list[str], list[str]] | None:
    if on_expr is None:
        return None
    pairs = _extract_join_pairs(on_expr)
    if not pairs:
        msg = "JOIN predicate does not contain equi-join columns."
        raise TranslationError(msg)
    left_keys = [left for left, _ in pairs]
    right_keys = [right for _, right in pairs]
    return left_keys, right_keys


def _extract_join_pairs(expr: Expression) -> list[tuple[str, str]]:
    if isinstance(expr, exp.And):
        return [*(_extract_join_pairs(expr.left)), *(_extract_join_pairs(expr.right))]
    if isinstance(expr, exp.EQ):
        left = _column_name(expr.left)
        right = _column_name(expr.right)
        if left is None or right is None:
            return []
        return [(left, right)]
    return []


def _column_name(expr: Expression) -> str | None:
    if isinstance(expr, exp.Column):
        if expr.table:
            return f"{expr.table}.{expr.name}"
        return expr.name
    return None


def _has_aggregate(exprs: Sequence[Expression]) -> bool:
    return any(bool(expr.find(exp.AggFunc)) for expr in exprs)


def _apply_aggregate(df: DataFrame, select: exp.Select) -> DataFrame:
    group = select.args.get("group")
    group_exprs = []
    if group is not None:
        group_exprs = [_expr_to_df(expr) for expr in group.expressions]
    agg_exprs = []
    for expr in select.expressions:
        if _is_group_expr(expr):
            continue
        agg_exprs.append(_aggregate_expr(expr))
    aggregated = df.aggregate(group_exprs, agg_exprs)
    if select.expressions:
        projected = [_select_output_expr(expr) for expr in select.expressions]
        return aggregated.select(*projected)
    return aggregated


def _is_group_expr(expr: Expression) -> bool:
    if isinstance(expr, exp.Alias):
        return _is_group_expr(expr.this)
    return isinstance(expr, exp.Column)


def _aggregate_expr(expr: Expression) -> Expr:
    if isinstance(expr, exp.Alias):
        base = _aggregate_expr(expr.this)
        return base.alias(expr.alias)
    if isinstance(expr, exp.AggFunc):
        agg = _func_expr(expr)
        alias = expr.alias_or_name or _default_agg_alias(expr)
        return agg.alias(alias)
    msg = f"Unsupported aggregate expression: {expr.__class__.__name__}."
    raise TranslationError(msg)


def _select_exprs(exprs: Sequence[Expression]) -> list[Expr | str]:
    out: list[Expr | str] = []
    for expr in exprs:
        if isinstance(expr, exp.Star):
            out.append("*")
            continue
        out.append(_expr_to_df(expr))
    return out


def _order_exprs(order: exp.Order) -> list[SortExpr]:
    keys = []
    for expr in order.expressions:
        ordered = cast("exp.Ordered", expr)
        sort_expr = _expr_to_df(ordered.this).sort(
            ascending=ordered.args.get("desc") is not True,
            nulls_first=ordered.args.get("nulls_first") is not False,
        )
        keys.append(sort_expr)
    return keys


def _expr_to_df(expr: Expression) -> Expr:
    if isinstance(expr, exp.Alias):
        return _expr_to_df(expr.this).alias(expr.alias)
    handler = _EXPR_DISPATCH.get(type(expr))
    if handler is not None:
        return handler(expr)
    if isinstance(expr, exp.Func):
        return _func_expr(expr)
    msg = f"Unsupported expression: {expr.__class__.__name__}."
    raise TranslationError(msg)


def _column_expr(expr: exp.Column) -> Expr:
    return col(_column_name(expr) or expr.name)


def _literal_expr(expr: exp.Literal) -> Expr:
    if expr.is_string:
        return lit(expr.this)
    if expr.this is None:
        return lit(None)
    return lit(_numeric_literal(expr.this))


def _null_expr(expr: exp.Null) -> Expr:
    _ = expr
    return lit(None)


def _cast_expr(expr: exp.Cast) -> Expr:
    return _expr_to_df(expr.this).cast(expr.to.sql())


def _and_expr(expr: exp.And) -> Expr:
    return _expr_to_df(expr.left) & _expr_to_df(expr.right)


def _or_expr(expr: exp.Or) -> Expr:
    return _expr_to_df(expr.left) | _expr_to_df(expr.right)


def _not_expr(expr: exp.Not) -> Expr:
    return ~_expr_to_df(expr.this)


def _eq_expr(expr: exp.EQ) -> Expr:
    return _expr_to_df(expr.left) == _expr_to_df(expr.right)


def _neq_expr(expr: exp.NEQ) -> Expr:
    return _expr_to_df(expr.left) != _expr_to_df(expr.right)


def _gt_expr(expr: exp.GT) -> Expr:
    return _expr_to_df(expr.left) > _expr_to_df(expr.right)


def _gte_expr(expr: exp.GTE) -> Expr:
    return _expr_to_df(expr.left) >= _expr_to_df(expr.right)


def _lt_expr(expr: exp.LT) -> Expr:
    return _expr_to_df(expr.left) < _expr_to_df(expr.right)


def _lte_expr(expr: exp.LTE) -> Expr:
    return _expr_to_df(expr.left) <= _expr_to_df(expr.right)


def _is_expr(expr: exp.Is) -> Expr:
    if isinstance(expr.expression, exp.Null):
        return _expr_to_df(expr.this).is_null()
    return _expr_to_df(expr.this) == _expr_to_df(expr.expression)


_EXPR_DISPATCH: dict[type[Expression], Callable[[Expression], Expr]] = {
    exp.Column: cast("Callable[[Expression], Expr]", _column_expr),
    exp.Literal: cast("Callable[[Expression], Expr]", _literal_expr),
    exp.Null: cast("Callable[[Expression], Expr]", _null_expr),
    exp.Cast: cast("Callable[[Expression], Expr]", _cast_expr),
    exp.And: cast("Callable[[Expression], Expr]", _and_expr),
    exp.Or: cast("Callable[[Expression], Expr]", _or_expr),
    exp.Not: cast("Callable[[Expression], Expr]", _not_expr),
    exp.EQ: cast("Callable[[Expression], Expr]", _eq_expr),
    exp.NEQ: cast("Callable[[Expression], Expr]", _neq_expr),
    exp.GT: cast("Callable[[Expression], Expr]", _gt_expr),
    exp.GTE: cast("Callable[[Expression], Expr]", _gte_expr),
    exp.LT: cast("Callable[[Expression], Expr]", _lt_expr),
    exp.LTE: cast("Callable[[Expression], Expr]", _lte_expr),
    exp.Is: cast("Callable[[Expression], Expr]", _is_expr),
}


def _func_expr(expr: exp.Func) -> Expr:
    name = expr.sql_name().lower()
    func = getattr(f, name, None)
    if func is None:
        msg = f"Unsupported function: {name!r}."
        raise TranslationError(msg)
    args = [_expr_to_df(arg) for arg in expr.expressions]
    result = func(*args)
    alias = expr.alias_or_name
    if alias:
        return result.alias(alias)
    return result


def _numeric_literal(value: str) -> int | float:
    try:
        return int(value)
    except ValueError:
        return float(value)


def _int_value(expr: Expression | None) -> int:
    if expr is None:
        return 0
    if isinstance(expr, exp.Literal):
        return int(_numeric_literal(expr.this))
    if isinstance(expr, exp.Identifier):
        return int(expr.this)
    msg = f"Unsupported LIMIT/OFFSET value: {expr.__class__.__name__}."
    raise TranslationError(msg)


def _default_agg_alias(expr: exp.AggFunc) -> str:
    name = expr.sql_name().lower()
    args = list(expr.expressions)
    if args:
        col_name = _column_name(args[0])
        if col_name:
            return f"{name}_{col_name.replace('.', '_')}"
    return f"{name}_expr"


def _select_output_expr(expr: Expression) -> Expr | str:
    if isinstance(expr, exp.Star):
        return "*"
    if isinstance(expr, exp.Alias):
        return col(expr.alias)
    if isinstance(expr, exp.Column):
        return col(_column_name(expr) or expr.name)
    if isinstance(expr, exp.AggFunc):
        return col(expr.alias_or_name or _default_agg_alias(expr))
    return _expr_to_df(expr)
