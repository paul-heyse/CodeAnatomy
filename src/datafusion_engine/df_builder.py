"""SQLGlot to DataFusion DataFrame translator."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa
from datafusion import SessionContext, col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr, SortExpr
from sqlglot import Expression, exp

from datafusion_engine.registry_bridge import DataFusionCachePolicy, register_dataset_df
from datafusion_engine.udf_registry import datafusion_scalar_udf_map
from ibis_engine.registry import DatasetLocation

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile


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

_SQLGLOT_CAST_TYPES: dict[exp.DataType.Type, pa.DataType] = {
    exp.DataType.Type.BOOLEAN: pa.bool_(),
    exp.DataType.Type.TINYINT: pa.int8(),
    exp.DataType.Type.SMALLINT: pa.int16(),
    exp.DataType.Type.INT: pa.int32(),
    exp.DataType.Type.BIGINT: pa.int64(),
    exp.DataType.Type.UTINYINT: pa.uint8(),
    exp.DataType.Type.USMALLINT: pa.uint16(),
    exp.DataType.Type.UINT: pa.uint32(),
    exp.DataType.Type.UBIGINT: pa.uint64(),
    exp.DataType.Type.FLOAT: pa.float32(),
    exp.DataType.Type.DOUBLE: pa.float64(),
    exp.DataType.Type.DATE: pa.date32(),
    exp.DataType.Type.DATE32: pa.date32(),
    exp.DataType.Type.TIME: pa.time64("us"),
    exp.DataType.Type.TIMESTAMP: pa.timestamp("us"),
    exp.DataType.Type.TIMESTAMP_S: pa.timestamp("s"),
    exp.DataType.Type.TIMESTAMP_MS: pa.timestamp("ms"),
    exp.DataType.Type.TIMESTAMP_NS: pa.timestamp("ns"),
}

_UDF_FUNC_MAP = datafusion_scalar_udf_map()


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
    expr = _resolve_table_aliases(expr)
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


def _resolve_table_aliases(expr: Expression) -> Expression:
    alias_map: dict[str, str | None] = {}
    for table in expr.find_all(exp.Table):
        alias = table.args.get("alias")
        alias_name = _table_alias_name(alias)
        if alias_name:
            alias_map[alias_name] = table.name
    for subquery in expr.find_all(exp.Subquery):
        alias = subquery.args.get("alias")
        alias_name = _table_alias_name(alias)
        if alias_name:
            alias_map.setdefault(alias_name, None)
    if not alias_map:
        return expr

    def _rewrite(node: Expression) -> Expression:
        if isinstance(node, exp.Column) and node.table in alias_map:
            mapped = alias_map[node.table]
            if mapped:
                return exp.column(node.name, table=mapped)
            return exp.column(node.name)
        return node

    return expr.transform(_rewrite)


def _table_alias_name(alias: Expression | str | None) -> str | None:
    if isinstance(alias, exp.TableAlias):
        alias = alias.this
    if isinstance(alias, exp.Identifier):
        return alias.this
    if isinstance(alias, str):
        return alias
    return None


def register_dataset(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    cache_policy: DataFusionCachePolicy | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
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
    if runtime_profile is not None:
        runtime_profile.ensure_delta_plan_codecs(ctx)
    try:
        return register_dataset_df(
            ctx,
            name=name,
            location=location,
            cache_policy=cache_policy,
            runtime_profile=runtime_profile,
        )
    except ValueError as exc:
        raise TranslationError(str(exc)) from exc


def _select_to_df(ctx: SessionContext, select: exp.Select) -> DataFrame:
    from_expr = select.args.get("from") or select.args.get("from_")
    source = _from_expr(ctx, from_expr)
    df = _apply_joins(ctx, source, select.args.get("joins"))
    if select.args.get("where") is not None:
        df = df.filter(_expr_to_df(select.args["where"].this))
    if select.args.get("group") is not None or _has_aggregate(select.expressions):
        df = _apply_aggregate(df, select)
    else:
        df = df.select(*_select_exprs(select.expressions))
    if select.args.get("having") is not None:
        df = df.filter(_expr_to_df(select.args["having"].this))
    if select.args.get("distinct") is not None:
        df = df.distinct()
    if select.args.get("order") is not None:
        df = df.sort(*_order_exprs(select.args["order"]))
    if select.args.get("limit") is not None:
        limit = cast("exp.Limit", select.args["limit"])
        count = _int_value(limit.expression)
        offset = _int_value(limit.args.get("offset"))
        df = df.limit(count, offset=offset)
    return df


def _from_expr(ctx: SessionContext, from_expr: exp.From | None) -> DataFrame:
    if from_expr is None:
        msg = "SELECT without FROM is not supported."
        raise TranslationError(msg)
    sources = list(from_expr.expressions)
    if not sources and from_expr.this is not None:
        sources.append(from_expr.this)
    if not sources:
        msg = "SELECT without FROM is not supported."
        raise TranslationError(msg)
    source = sources[0]
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
        left_schema = tuple(df.schema().names)
        right_schema = tuple(right.schema().names)
        join_keys = _join_keys(join, left_schema=left_schema, right_schema=right_schema)
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


def _join_keys(
    join: exp.Join,
    *,
    left_schema: Sequence[str],
    right_schema: Sequence[str],
) -> tuple[list[str], list[str]] | None:
    using = join.args.get("using")
    if using:
        using_exprs = _using_exprs(using)
        if not using_exprs:
            msg = "JOIN USING clause must include column names."
            raise TranslationError(msg)
        names = [_using_column_name(expr) for expr in using_exprs]
        left_keys = [
            _resolve_join_key(
                name,
                schema_names=left_schema,
                other_schema=right_schema,
                side="left",
                allow_ambiguous=True,
            )
            for name in names
        ]
        right_keys = [
            _resolve_join_key(
                name,
                schema_names=right_schema,
                other_schema=left_schema,
                side="right",
                allow_ambiguous=True,
            )
            for name in names
        ]
        return left_keys, right_keys
    on_expr = join.args.get("on")
    if on_expr is None:
        return None
    pairs = _extract_join_pairs(on_expr)
    if not pairs:
        msg = "JOIN predicate does not contain equi-join columns."
        raise TranslationError(msg)
    left_keys = [
        _resolve_join_key(
            left,
            schema_names=left_schema,
            other_schema=right_schema,
            side="left",
            allow_ambiguous=False,
        )
        for left, _ in pairs
    ]
    right_keys = [
        _resolve_join_key(
            right,
            schema_names=right_schema,
            other_schema=left_schema,
            side="right",
            allow_ambiguous=False,
        )
        for _, right in pairs
    ]
    return left_keys, right_keys


def _using_exprs(using: object) -> list[Expression]:
    if isinstance(using, list):
        return [expr for expr in using if isinstance(expr, Expression)]
    if isinstance(using, Expression):
        return list(using.expressions)
    return []


def _using_column_name(expr: Expression) -> str:
    if isinstance(expr, exp.Identifier):
        return expr.this
    if isinstance(expr, exp.Column):
        if expr.table:
            msg = f"JOIN USING columns must be unqualified: {expr.sql()}."
            raise TranslationError(msg)
        return expr.name
    msg = f"Unsupported JOIN USING expression: {expr.__class__.__name__}."
    raise TranslationError(msg)


def _resolve_join_key(
    name: str,
    *,
    schema_names: Sequence[str],
    other_schema: Sequence[str],
    side: str,
    allow_ambiguous: bool,
) -> str:
    if name in schema_names:
        if not allow_ambiguous and name in other_schema:
            msg = f"Ambiguous JOIN key {name!r} on {side} side."
            raise TranslationError(msg)
        return name
    unqualified = _strip_qualifier(name)
    if unqualified in schema_names:
        return unqualified
    msg = f"JOIN key {name!r} not found on {side} side."
    raise TranslationError(msg)


def _strip_qualifier(name: str) -> str:
    if "." not in name:
        return name
    return name.split(".", maxsplit=1)[-1]


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


def _column_name(expr: Expression, *, strip_qualifier: bool = False) -> str | None:
    if isinstance(expr, exp.Column):
        if expr.table and not strip_qualifier:
            return f"{expr.table}.{expr.name}"
        return expr.name
    return None


def _has_aggregate(exprs: Sequence[Expression]) -> bool:
    return any(bool(expr.find(exp.AggFunc)) for expr in exprs)


def _apply_aggregate(df: DataFrame, select: exp.Select) -> DataFrame:
    group = select.args.get("group")
    group_exprs: list[Expr] = []
    if group is not None:
        group_exprs = [_expr_to_df(expr) for expr in group.expressions]
    agg_exprs: list[Expr] = []
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


def _aggregate_expr(expr: Expression, *, apply_default_alias: bool = True) -> Expr:
    if isinstance(expr, exp.Filter):
        agg_expr = _aggregate_func(expr.this)
        predicate = _expr_to_df(expr.expression.this)
        filtered = agg_expr.filter(predicate).build()
        if not apply_default_alias:
            return filtered
        alias = _aggregate_alias(expr.this)
        return filtered.alias(alias)
    if isinstance(expr, exp.Alias):
        base = _aggregate_expr(expr.this, apply_default_alias=False)
        return base.alias(expr.alias)
    if isinstance(expr, exp.AggFunc):
        agg_expr = _aggregate_func(expr)
        if not apply_default_alias:
            return agg_expr
        alias = _aggregate_alias(expr)
        return agg_expr.alias(alias)
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


def _boolean_expr(expr: exp.Boolean) -> Expr:
    return lit(expr.this)


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
    return _expr_to_df(expr.this).cast(_cast_target(expr.to))


def _cast_target(data_type: exp.DataType) -> pa.DataType | type:
    dtype = data_type.this
    if isinstance(dtype, exp.DataType.Type):
        if dtype in _SQLGLOT_CAST_TYPES:
            return _SQLGLOT_CAST_TYPES[dtype]
        if dtype in {
            exp.DataType.Type.CHAR,
            exp.DataType.Type.NCHAR,
            exp.DataType.Type.TEXT,
            exp.DataType.Type.VARCHAR,
            exp.DataType.Type.NVARCHAR,
            exp.DataType.Type.BPCHAR,
            exp.DataType.Type.FIXEDSTRING,
            exp.DataType.Type.UUID,
            exp.DataType.Type.JSON,
            exp.DataType.Type.JSONB,
        }:
            return pa.string()
        if dtype in {
            exp.DataType.Type.BINARY,
            exp.DataType.Type.VARBINARY,
            exp.DataType.Type.BLOB,
            exp.DataType.Type.LONGBLOB,
            exp.DataType.Type.MEDIUMBLOB,
            exp.DataType.Type.TINYBLOB,
        }:
            return pa.binary()
        if dtype in {exp.DataType.Type.DECIMAL, exp.DataType.Type.DECIMAL128}:
            precision, scale = _decimal_params(data_type)
            return pa.decimal128(precision, scale)
        if dtype == exp.DataType.Type.DECIMAL256:
            precision, scale = _decimal_params(data_type)
            return pa.decimal256(precision, scale)
        if dtype == exp.DataType.Type.TIMESTAMPTZ:
            return pa.timestamp("us", tz="UTC")
    msg = f"Unsupported cast target: {data_type.sql()}."
    raise TranslationError(msg)


def _decimal_params(data_type: exp.DataType) -> tuple[int, int]:
    params = data_type.args.get("expressions") or []
    if not params:
        return (38, 10)
    precision = _data_type_param_int(params[0])
    scale = _data_type_param_int(params[1]) if len(params) > 1 else 0
    if precision is None:
        msg = f"Decimal precision must be numeric: {data_type.sql()}."
        raise TranslationError(msg)
    if scale is None:
        msg = f"Decimal scale must be numeric: {data_type.sql()}."
        raise TranslationError(msg)
    return precision, scale


def _data_type_param_int(param: exp.DataTypeParam) -> int | None:
    value = param.this
    if isinstance(value, exp.Literal) and not value.is_string:
        return int(_numeric_literal(value.this))
    return None


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


def _add_expr(expr: exp.Add) -> Expr:
    return _expr_to_df(expr.left) + _expr_to_df(expr.right)


def _sub_expr(expr: exp.Sub) -> Expr:
    return _expr_to_df(expr.left) - _expr_to_df(expr.right)


def _mul_expr(expr: exp.Mul) -> Expr:
    return _expr_to_df(expr.left) * _expr_to_df(expr.right)


def _div_expr(expr: exp.Div) -> Expr:
    return _expr_to_df(expr.left) / _expr_to_df(expr.right)


def _neg_expr(expr: exp.Neg) -> Expr:
    return lit(-1) * _expr_to_df(expr.this)


def _coalesce_expr(expr: exp.Coalesce) -> Expr:
    args = [_expr_to_df(arg) for arg in expr.expressions]
    if not args:
        msg = "COALESCE requires at least one argument."
        raise TranslationError(msg)
    return f.coalesce(*args)


def _if_expr(expr: exp.If) -> Expr:
    condition = _expr_to_df(expr.this)
    true_expr = _expr_to_df(expr.args["true"])
    false_expr = expr.args.get("false")
    if false_expr is None:
        return f.when(condition, true_expr).otherwise(lit(None))
    return f.when(condition, true_expr).otherwise(_expr_to_df(false_expr))


def _case_expr(expr: exp.Case) -> Expr:
    clauses = list(expr.args.get("ifs") or [])
    if not clauses:
        default = expr.args.get("default")
        return _expr_to_df(default) if default is not None else lit(None)
    first = clauses[0]
    case_builder = f.when(_expr_to_df(first.this), _expr_to_df(first.args["true"]))
    for clause in clauses[1:]:
        case_builder = case_builder.when(
            _expr_to_df(clause.this),
            _expr_to_df(clause.args["true"]),
        )
    default = expr.args.get("default")
    if default is not None:
        return case_builder.otherwise(_expr_to_df(default))
    return case_builder.end()


def _concat_expr(expr: exp.Concat) -> Expr:
    args = [_expr_to_df(arg) for arg in expr.expressions]
    if not args:
        msg = "CONCAT requires at least one argument."
        raise TranslationError(msg)
    return f.concat(*args)


def _pipe_concat_expr(expr: exp.DPipe) -> Expr:
    return f.concat(_expr_to_df(expr.left), _expr_to_df(expr.right))


def _like_expr(expr: exp.Like) -> Expr:
    return _like_match(expr, case_sensitive=True, escape=None)


def _ilike_expr(expr: exp.ILike) -> Expr:
    return _like_match(expr, case_sensitive=False, escape=None)


def _escape_expr(expr: exp.Escape) -> Expr:
    base = expr.this
    if not isinstance(base, (exp.Like, exp.ILike)):
        msg = f"Unsupported ESCAPE expression: {base.__class__.__name__}."
        raise TranslationError(msg)
    escape = expr.expression
    if escape is None:
        msg = "ESCAPE clause requires a literal escape character."
        raise TranslationError(msg)
    return _like_match(base, case_sensitive=isinstance(base, exp.Like), escape=escape)


def _like_match(
    expr: exp.Like | exp.ILike,
    *,
    case_sensitive: bool,
    escape: Expression | None,
) -> Expr:
    pattern_expr = expr.expression
    if not isinstance(pattern_expr, exp.Literal) or not pattern_expr.is_string:
        msg = "LIKE patterns must be string literals."
        raise TranslationError(msg)
    escape_char = _escape_literal(escape)
    regex = _like_pattern_to_regex(pattern_expr.this, escape=escape_char)
    flags = None if case_sensitive else lit("i")
    return f.regexp_like(_expr_to_df(expr.this), lit(regex), flags)


def _escape_literal(expr: Expression | None) -> str | None:
    if expr is None:
        return None
    if isinstance(expr, exp.Literal) and expr.is_string and expr.this:
        return expr.this
    msg = "ESCAPE clause requires a string literal."
    raise TranslationError(msg)


def _like_pattern_to_regex(pattern: str, *, escape: str | None) -> str:
    regex_parts = ["^"]
    idx = 0
    while idx < len(pattern):
        char = pattern[idx]
        if escape is not None and char == escape:
            idx += 1
            if idx >= len(pattern):
                regex_parts.append(re.escape(escape))
                break
            regex_parts.append(re.escape(pattern[idx]))
        elif char == "%":
            regex_parts.append(".*")
        elif char == "_":
            regex_parts.append(".")
        else:
            regex_parts.append(re.escape(char))
        idx += 1
    regex_parts.append("$")
    return "".join(regex_parts)


def _in_expr(expr: exp.In) -> Expr:
    values = expr.expressions
    if not values:
        msg = "IN requires a list of literals or expressions."
        raise TranslationError(msg)
    args = [_expr_to_df(value) for value in values]
    return f.in_list(_expr_to_df(expr.this), args)


def _between_expr(expr: exp.Between) -> Expr:
    if expr.args.get("symmetric"):
        msg = "BETWEEN SYMMETRIC is not supported."
        raise TranslationError(msg)
    low = _expr_to_df(expr.args["low"])
    high = _expr_to_df(expr.args["high"])
    return _expr_to_df(expr.this).between(low, high)


def _bracket_expr(expr: exp.Bracket) -> Expr:
    base = _expr_to_df(expr.this)
    index = expr.expression
    if isinstance(index, exp.Literal):
        if index.is_string:
            return base[index.this]
        position = int(_numeric_literal(index.this))
        if position < 1:
            msg = "Array indices must be 1-based for SQL bracket access."
            raise TranslationError(msg)
        return base[position - 1]
    msg = "Bracket access requires a string or integer literal."
    raise TranslationError(msg)


_EXPR_DISPATCH: dict[type[Expression], Callable[[Expression], Expr]] = {
    exp.Column: cast("Callable[[Expression], Expr]", _column_expr),
    exp.Boolean: cast("Callable[[Expression], Expr]", _boolean_expr),
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
    exp.Add: cast("Callable[[Expression], Expr]", _add_expr),
    exp.Sub: cast("Callable[[Expression], Expr]", _sub_expr),
    exp.Mul: cast("Callable[[Expression], Expr]", _mul_expr),
    exp.Div: cast("Callable[[Expression], Expr]", _div_expr),
    exp.Neg: cast("Callable[[Expression], Expr]", _neg_expr),
    exp.Coalesce: cast("Callable[[Expression], Expr]", _coalesce_expr),
    exp.If: cast("Callable[[Expression], Expr]", _if_expr),
    exp.Case: cast("Callable[[Expression], Expr]", _case_expr),
    exp.Concat: cast("Callable[[Expression], Expr]", _concat_expr),
    exp.DPipe: cast("Callable[[Expression], Expr]", _pipe_concat_expr),
    exp.Like: cast("Callable[[Expression], Expr]", _like_expr),
    exp.ILike: cast("Callable[[Expression], Expr]", _ilike_expr),
    exp.Escape: cast("Callable[[Expression], Expr]", _escape_expr),
    exp.In: cast("Callable[[Expression], Expr]", _in_expr),
    exp.Between: cast("Callable[[Expression], Expr]", _between_expr),
    exp.Bracket: cast("Callable[[Expression], Expr]", _bracket_expr),
}


def _func_expr(expr: exp.Func) -> Expr:
    name = _func_name(expr)
    args = _function_args(expr)
    func = getattr(f, name, None)
    if func is not None:
        result = func(*args)
    else:
        udf_impl = _UDF_FUNC_MAP.get(name)
        if udf_impl is None:
            msg = f"Unsupported function: {name!r}."
            raise TranslationError(msg)
        result = udf_impl(*args)
    alias = expr.alias_or_name
    if alias:
        return result.alias(alias)
    return result


def _function_args(expr: exp.Func) -> list[Expr]:
    args = [_expr_to_df(arg) for arg in expr.expressions]
    if args:
        return args
    if expr.this is None or isinstance(expr, exp.Anonymous):
        return []
    resolved = [_expr_to_df(expr.this)]
    extra = expr.args.get("expression")
    if isinstance(extra, Expression):
        resolved.append(_expr_to_df(extra))
    return resolved


def _func_name(expr: exp.Func) -> str:
    if isinstance(expr, exp.Anonymous):
        name = expr.this
        if isinstance(name, str):
            return name.lower()
        return str(name).lower()
    return expr.sql_name().lower()


def _aggregate_func(expr: exp.AggFunc) -> Expr:
    distinct = False
    args: list[Expression] = []
    if isinstance(expr, exp.Count) and isinstance(expr.this, exp.Star):
        args = []
    elif isinstance(expr.this, exp.Distinct):
        distinct = True
        args = list(expr.this.expressions)
    else:
        args = _function_arg_exprs(expr)
    agg_expr = _call_aggregate(expr, args)
    if distinct:
        agg_expr = agg_expr.distinct().build()
    return agg_expr


def _function_arg_exprs(expr: exp.Func) -> list[Expression]:
    if expr.expressions:
        return list(expr.expressions)
    if expr.this is None or isinstance(expr, exp.Anonymous):
        return []
    args = [expr.this]
    extra = expr.args.get("expression")
    if isinstance(extra, Expression):
        args.append(extra)
    return args


def _call_aggregate(expr: exp.AggFunc, args: Sequence[Expression]) -> Expr:
    name = expr.sql_name().lower()
    func = getattr(f, name, None)
    if func is None:
        msg = f"Unsupported aggregate function: {name!r}."
        raise TranslationError(msg)
    resolved = [_expr_to_df(arg) for arg in args]
    return func(*resolved)


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
    args: list[Expression] = list(expr.expressions)
    if not args:
        if isinstance(expr.this, exp.Distinct):
            args = list(expr.this.expressions)
        elif isinstance(expr.this, exp.Star):
            args = []
        elif isinstance(expr.this, Expression):
            args = [expr.this]
    if args:
        col_name = _column_name(args[0])
        if col_name:
            return f"{name}_{col_name.replace('.', '_')}"
    return f"{name}_expr"


def _aggregate_alias(expr: exp.AggFunc) -> str:
    alias = expr.alias_or_name
    if alias and alias != "*":
        return alias
    return _default_agg_alias(expr)


def _select_output_expr(expr: Expression) -> Expr | str:
    if isinstance(expr, exp.Star):
        return "*"
    if isinstance(expr, exp.Alias):
        return col(expr.alias)
    if isinstance(expr, exp.Column):
        return col(_column_name(expr) or expr.name)
    if isinstance(expr, exp.Filter) and isinstance(expr.this, exp.AggFunc):
        return col(_aggregate_alias(expr.this))
    if isinstance(expr, exp.AggFunc):
        return col(_aggregate_alias(expr))
    return _expr_to_df(expr)
