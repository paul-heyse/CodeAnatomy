"""SQLGlot-backed expression specs for derived fields and predicates."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from arrowdsl.core.expr_types import ScalarValue
from arrowdsl.core.interop import ScalarLike
from sqlglot_tools.compat import Expression, exp, parse_one
from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    normalize_expr,
    resolve_sqlglot_policy,
    sqlglot_sql,
)


def _literal_expr(value: ScalarValue) -> Expression:
    if value is None:
        return exp.Null()
    if isinstance(value, bool):
        return exp.Boolean(this=value)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return exp.Literal.number(value)
    if isinstance(value, (str, bytes)):
        text = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
        return exp.Literal.string(text)
    if isinstance(value, ScalarLike):
        resolved = value.as_py()
        if resolved is None or isinstance(resolved, (bool, int, float, str, bytes)):
            return _literal_expr(resolved)
        msg = f"Unsupported scalar literal type: {type(resolved).__name__}."
        raise TypeError(msg)
    msg = f"Unsupported literal value: {type(value).__name__}."
    raise TypeError(msg)


def _field_expr(name: str) -> Expression:
    return exp.Column(this=exp.Identifier(this=name))


def _call_expr(name: str, args: Sequence[Expression]) -> Expression:
    return exp.Anonymous(this=name, expressions=list(args))


@dataclass(frozen=True)
class ExprIR:
    """Minimal expression spec with SQLGlot serialization."""

    op: str
    name: str | None = None
    value: ScalarValue | None = None
    args: tuple[ExprIR, ...] = ()
    options: object | None = None

    def to_sqlglot(self) -> Expression:
        """Return a SQLGlot expression for this spec.

        Returns
        -------
        sqlglot.Expression
            SQLGlot expression for the expression spec.

        Raises
        ------
        ValueError
            Raised when the spec is missing required fields or the op is unsupported.
        """
        if self.op == "field":
            if self.name is None:
                msg = "ExprIR field op requires name."
                raise ValueError(msg)
            return _field_expr(self.name)
        if self.op == "literal":
            return _literal_expr(self.value)
        if self.op == "call":
            if self.name is None:
                msg = "ExprIR call op requires name."
                raise ValueError(msg)
            return _call_expr(self.name, [arg.to_sqlglot() for arg in self.args])
        msg = f"Unsupported ExprIR op: {self.op!r}."
        raise ValueError(msg)

    def to_sql(
        self,
        *,
        policy_name: str = "datafusion_compile",
        pretty: bool | None = None,
    ) -> str:
        """Return SQL text for the expression under a SQLGlot policy.

        Returns
        -------
        str
            SQL expression text emitted using policy settings.
        """
        policy = resolve_sqlglot_policy(name=policy_name)
        expr = self.to_sqlglot()
        return sqlglot_sql(expr, policy=policy, pretty=pretty)

    def to_sql_spec(
        self,
        *,
        policy_name: str = "datafusion_compile",
        dialect: str | None = None,
    ) -> SqlExprSpec:
        """Return a SQL expression spec derived from this ExprIR.

        Returns
        -------
        SqlExprSpec
            SQL expression spec for the expression IR.
        """
        return SqlExprSpec(
            sql=self.to_sql(policy_name=policy_name),
            policy_name=policy_name,
            dialect=dialect,
        )


@dataclass(frozen=True)
class SqlExprSpec:
    """SQL-backed expression specification."""

    sql: str
    policy_name: str = "datafusion_compile"
    dialect: str | None = None

    def normalized_sql(self) -> str:
        """Return normalized SQL text for this expression spec.

        Returns
        -------
        str
            Normalized SQL expression text.
        """
        policy = resolve_sqlglot_policy(name=self.policy_name)
        read_dialect = self.dialect or policy.read_dialect
        expr = parse_one(self.sql, read=read_dialect)
        normalized = normalize_expr(
            expr,
            options=NormalizeExprOptions(policy=policy, sql=self.sql),
        )
        return sqlglot_sql(normalized, policy=policy)

    def resolved_dialect(self) -> str:
        """Return the SQL dialect to use for execution.

        Returns
        -------
        str
            Dialect for SQL emission/execution.
        """
        policy = resolve_sqlglot_policy(name=self.policy_name)
        return self.dialect or policy.write_dialect


__all__ = ["ExprIR", "SqlExprSpec"]
