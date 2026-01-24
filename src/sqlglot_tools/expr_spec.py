"""SQLGlot-backed expression specs for derived fields and predicates."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from arrowdsl.core.expr_types import ScalarValue
from arrowdsl.core.interop import ScalarLike
from sqlglot_tools.compat import Expression, exp
from sqlglot_tools.optimizer import resolve_sqlglot_policy, sqlglot_sql


def _literal_expr(value: ScalarValue) -> Expression:
    if value is None:
        return exp.Null()
    if isinstance(value, bool):
        return exp.Boolean(this=value)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return exp.Literal.number(value)
    if isinstance(value, (str, bytes)):
        if isinstance(value, bytes):
            text = value.decode("utf-8", errors="replace")
        else:
            text = value
        return exp.Literal.string(text)
    if isinstance(value, ScalarLike):
        return _literal_expr(value.as_py())
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


__all__ = ["ExprIR"]
