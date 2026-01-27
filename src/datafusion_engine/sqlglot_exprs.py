"""Translate DataFusion expressions into SQLGlot AST nodes."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, cast

from datafusion.dataframe import DataFrame
from datafusion.expr import Expr

from sqlglot_tools.compat import Expression, exp
from sqlglot_tools.optimizer import (
    StrictParseOptions,
    parse_sql_strict,
    register_datafusion_dialect,
)

_MIN_CASE_OPERANDS = 2
_MIN_BINARY_OPERANDS = 2


class _RawExpr(Protocol):
    def rex_type(self) -> object: ...

    def rex_call_operator(self) -> object: ...

    def rex_call_operands(self) -> Sequence[Expr]: ...

    def python_value(self) -> object: ...

    def schema_name(self) -> str: ...

    def canonical_name(self) -> str: ...


class _RexType(Protocol):
    Reference: object
    Literal: object
    Alias: object
    Call: object


def sqlglot_expr_from_datafusion(expr: Expr) -> Expression:
    """Return a SQLGlot expression from a DataFusion Expr.

    Parameters
    ----------
    expr:
        DataFusion expression to translate.

    Returns
    -------
    sqlglot.expressions.Expression
        SQLGlot expression matching the input Expr.
    """
    return _translate_raw(_raw_expr(expr))


def sqlglot_select_from_exprs(
    *,
    base_table: str,
    exprs: Sequence[Expr],
) -> Expression:
    """Return a SQLGlot SELECT for a base table and DataFusion Expr list.

    Parameters
    ----------
    base_table:
        Base table name for the SELECT.
    exprs:
        DataFusion select expressions.

    Returns
    -------
    sqlglot.expressions.Expression
        SQLGlot SELECT statement.
    """
    selections = [_translate_raw(_raw_expr(expr)) for expr in exprs]
    return exp.select(*selections).from_(base_table)


def _raw_expr(expr: Expr) -> _RawExpr:
    return cast("_RawExpr", expr.expr)


def _translate_raw(raw: _RawExpr) -> Expression:
    rex_type = cast("_RexType", raw.rex_type())
    if rex_type == rex_type.Reference:
        return _column_from_name(raw.schema_name())
    if rex_type == rex_type.Literal:
        return _literal(raw.python_value())
    if rex_type == rex_type.Alias:
        operands = raw.rex_call_operands()
        if not operands:
            msg = "Alias expression missing operand."
            raise ValueError(msg)
        alias_name = _alias_name(raw.canonical_name())
        return exp.Alias(this=sqlglot_expr_from_datafusion(operands[0]), alias=alias_name)
    if rex_type == rex_type.Call:
        return _translate_call(raw)
    msg = f"Unsupported DataFusion expression type: {rex_type}."
    raise TypeError(msg)


def _translate_call(raw: _RawExpr) -> Expression:
    operator = str(raw.rex_call_operator())
    operands = raw.rex_call_operands()
    op_lower = operator.lower()
    if op_lower == "case":
        return _case_expr(operands)
    if op_lower in {"is null", "is not null"}:
        return _is_null_expr(op_lower, operands)
    if op_lower == "not":
        return exp.Not(this=_single_operand(operands, operator))
    if op_lower in _BINARY_OPERATOR_MAP:
        return _fold_binary(_BINARY_OPERATOR_MAP[op_lower], operands)
    if op_lower == "-" and len(operands) == 1:
        return exp.Neg(this=sqlglot_expr_from_datafusion(operands[0]))
    translated = [sqlglot_expr_from_datafusion(operand) for operand in operands]
    return exp.func(operator, *translated)


def _case_expr(operands: Sequence[Expr]) -> Expression:
    if len(operands) < _MIN_CASE_OPERANDS:
        msg = "CASE expression missing operands."
        raise ValueError(msg)
    has_default = len(operands) % 2 == 1
    limit = len(operands) - 1 if has_default else len(operands)
    pairs = operands[:limit]
    if len(pairs) % 2 != 0:
        msg = "CASE expression has incomplete condition/value pairs."
        raise ValueError(msg)
    whens = []
    for idx in range(0, len(pairs), 2):
        condition = sqlglot_expr_from_datafusion(pairs[idx])
        value = sqlglot_expr_from_datafusion(pairs[idx + 1])
        whens.append(exp.When(this=condition, true=value))
    default_expr = sqlglot_expr_from_datafusion(operands[-1]) if has_default else None
    return exp.Case(ifs=whens, default=default_expr)


def _is_null_expr(op_lower: str, operands: Sequence[Expr]) -> Expression:
    base = exp.Is(this=_single_operand(operands, op_lower), expression=exp.Null())
    if op_lower == "is not null":
        return exp.Not(this=base)
    return base


def _single_operand(operands: Sequence[Expr], operator: str) -> Expression:
    if len(operands) != 1:
        msg = f"Operator {operator!r} expects 1 operand, got {len(operands)}."
        raise ValueError(msg)
    return sqlglot_expr_from_datafusion(operands[0])


def _fold_binary(op_cls: type[Expression], operands: Sequence[Expr]) -> Expression:
    if len(operands) < _MIN_BINARY_OPERANDS:
        msg = "Binary operator missing operands."
        raise ValueError(msg)
    expr = sqlglot_expr_from_datafusion(operands[0])
    for operand in operands[1:]:
        expr = op_cls(this=expr, expression=sqlglot_expr_from_datafusion(operand))
    return expr


def _column_from_name(name: str) -> Expression:
    if "." in name:
        parts = name.split(".")
        return exp.column(parts[-1], table=".".join(parts[:-1]))
    return exp.column(name)


def _literal(value: object) -> Expression:
    if value is None:
        return exp.Null()
    if isinstance(value, bool):
        return exp.Boolean(this=value)
    if isinstance(value, (int, float)):
        return exp.Literal.number(value)
    if isinstance(value, bytes):
        return exp.Literal.string(value.decode("utf-8", errors="replace"))
    if isinstance(value, str):
        return exp.Literal.string(value)
    msg = f"Unsupported literal value: {value!r}."
    raise TypeError(msg)


def _alias_name(canonical: str) -> str:
    if " AS " not in canonical:
        return canonical
    return canonical.rsplit(" AS ", 1)[-1].strip()


_BINARY_OPERATOR_MAP: dict[str, type[Expression]] = {
    "+": exp.Add,
    "-": exp.Sub,
    "*": exp.Mul,
    "/": exp.Div,
    "%": exp.Mod,
    "=": exp.EQ,
    "!=": exp.NEQ,
    "<>": exp.NEQ,
    "<": exp.LT,
    "<=": exp.LTE,
    ">": exp.GT,
    ">=": exp.GTE,
    "and": exp.And,
    "or": exp.Or,
}


__all__ = ["sqlglot_expr_from_datafusion", "sqlglot_select_from_exprs"]


def sqlglot_ast_from_dataframe(df: DataFrame) -> Expression:
    """Return a SQLGlot AST from a DataFusion DataFrame plan.

    Parameters
    ----------
    df:
        DataFusion DataFrame with a logical plan.

    Returns
    -------
    sqlglot.expressions.Expression
        Parsed SQLGlot AST for the DataFusion plan.

    Raises
    ------
    TypeError
        Raised when the DataFrame lacks a logical plan.
    ValueError
        Raised when the DataFrame plan cannot be translated.
    """
    from datafusion.plan import LogicalPlan as DataFusionLogicalPlan
    from datafusion.unparser import Dialect as DataFusionDialect
    from datafusion.unparser import Unparser as DataFusionUnparser

    logical_plan = getattr(df, "logical_plan", None)
    if not callable(logical_plan):
        msg = "DataFrame missing logical_plan; cannot derive SQLGlot AST."
        raise TypeError(msg)
    sql = ""
    try:
        plan_obj = logical_plan()
        if isinstance(plan_obj, DataFusionLogicalPlan):
            unparser = DataFusionUnparser(DataFusionDialect.default())
            sql = str(unparser.plan_to_sql(plan_obj))
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = "Failed to unparse DataFusion plan."
        raise ValueError(msg) from exc
    if not sql:
        msg = "DataFusion plan did not produce SQL."
        raise ValueError(msg)
    register_datafusion_dialect()
    # NOTE: This parse_sql_strict usage is for converting DataFusion's
    # unparsed SQL back to SQLGlot AST, not for external SQL validation.
    # This is an internal transformation path and does not need DataFusion
    # SQL parser since the SQL was generated by DataFusion itself.
    return parse_sql_strict(
        sql,
        dialect="datafusion_ext",
        options=StrictParseOptions(error_level=None),
    )


__all__ += ["sqlglot_ast_from_dataframe"]
