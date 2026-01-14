"""SQLGlot list-filter validation gate helpers."""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot import Expression, exp

from ibis_engine.param_tables import ParamTablePolicy


@dataclass(frozen=True)
class ListFilterGatePolicy:
    """Policy settings for list-filter validation."""

    reject_literal_inlist: bool = True
    allow_small_literal_inlist_max: int = 0


class ListFilterGateError(ValueError):
    """Raised when list-filter gate validation fails."""


def validate_no_inline_inlists(
    *,
    rule_name: str,
    sg_ast: Expression,
    param_policy: ParamTablePolicy,
    policy: ListFilterGatePolicy | None = None,
) -> None:
    """Reject inline literal IN-lists unless sourced from param tables."""
    policy = policy or ListFilterGatePolicy()
    for node in sg_ast.find_all(exp.In):
        rhs = _in_rhs(node)
        if rhs is None:
            continue
        _validate_in_rhs(
            rhs,
            rule_name=rule_name,
            param_policy=param_policy,
            policy=policy,
        )


def _validate_in_rhs(
    rhs: object,
    *,
    rule_name: str,
    param_policy: ParamTablePolicy,
    policy: ListFilterGatePolicy,
) -> None:
    """Validate the right-hand side of an IN expression.

    Parameters
    ----------
    rhs
        Right-hand side payload from a SQLGlot IN expression.
    rule_name
        Rule name used in diagnostics.
    param_policy
        Param table policy for allowed sources.
    policy
        List-filter validation policy.

    Raises
    ------
    ListFilterGateError
        Raised when list-filter validation fails.
    """
    if isinstance(rhs, (exp.Subquery, exp.Select)):
        if _subquery_is_from_param_table(
            rhs,
            param_schema=param_policy.schema,
            param_prefix=param_policy.prefix,
        ):
            return
        msg = (
            f"[list-filter-gate] Rule '{rule_name}' uses IN-subquery not sourced from "
            f"declared param tables; use params.{param_policy.prefix}<name> join instead."
        )
        raise ListFilterGateError(msg)
    if isinstance(rhs, (exp.Tuple, exp.Array)):
        _validate_in_literals(
            rhs.expressions,
            rule_name=rule_name,
            policy=policy,
        )
        return
    if isinstance(rhs, list):
        _validate_in_literals(
            rhs,
            rule_name=rule_name,
            policy=policy,
        )
        return


def _validate_in_literals(
    items: list[Expression],
    *,
    rule_name: str,
    policy: ListFilterGatePolicy,
) -> None:
    """Validate literal IN-list usage against policy settings.

    Parameters
    ----------
    items
        Literal expressions used in the IN list.
    rule_name
        Rule name used in diagnostics.
    policy
        List-filter validation policy.

    Raises
    ------
    ListFilterGateError
        Raised when literal IN-list usage violates policy.
    """
    lit_count = _literal_count(items)
    if _allow_small_literal_inlist(policy, lit_count):
        return
    if policy.reject_literal_inlist:
        msg = (
            f"[list-filter-gate] Rule '{rule_name}' contains literal IN-list "
            f"({lit_count} literals). Use a declared param table join."
        )
        raise ListFilterGateError(msg)


def _allow_small_literal_inlist(policy: ListFilterGatePolicy, lit_count: int) -> bool:
    """Return True when a small literal IN-list is allowed.

    Parameters
    ----------
    policy
        List-filter validation policy.
    lit_count
        Number of literal items in the IN list.

    Returns
    -------
    bool
        ``True`` when the literal list is within the allowed size.
    """
    if not policy.allow_small_literal_inlist_max:
        return False
    return lit_count <= policy.allow_small_literal_inlist_max


def _in_rhs(node: exp.In) -> object | None:
    """Extract the IN expression right-hand side from SQLGlot args.

    Parameters
    ----------
    node
        SQLGlot IN expression.

    Returns
    -------
    object | None
        Extracted RHS payload when present.
    """
    if "query" in node.args:
        return node.args.get("query")
    if "expression" in node.args:
        return node.args.get("expression")
    if "expressions" in node.args:
        return node.args.get("expressions")
    return None


def _literal_count(items: list[Expression]) -> int:
    """Count literal expressions in an IN-list.

    Parameters
    ----------
    items
        Expressions to inspect.

    Returns
    -------
    int
        Number of literal expressions.
    """
    return sum(1 for expr in items if isinstance(expr, exp.Literal))


def _subquery_is_from_param_table(
    query: Expression,
    *,
    param_schema: str,
    param_prefix: str,
) -> bool:
    """Check whether a subquery sources from param tables.

    Parameters
    ----------
    query
        SQLGlot query expression to inspect.
    param_schema
        Allowed parameter schema name.
    param_prefix
        Allowed parameter table prefix.

    Returns
    -------
    bool
        ``True`` when the query uses only param tables.
    """
    for table in query.find_all(exp.Table):
        schema = table.args.get("db")
        name = table.name
        if (
            isinstance(schema, str)
            and (schema == param_schema or schema.startswith(f"{param_schema}_"))
            and name.startswith(param_prefix)
        ):
            return True
    return False


__all__ = [
    "ListFilterGateError",
    "ListFilterGatePolicy",
    "validate_no_inline_inlists",
]
