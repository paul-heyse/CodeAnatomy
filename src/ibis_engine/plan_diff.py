"""Semantic SQLGlot diff helpers for plan invalidation."""

from __future__ import annotations

from dataclasses import dataclass, replace

from ibis.expr.types import Table as IbisTable
from sqlglot.diff import Insert, Keep, diff

from sqlglot_tools.bridge import IbisCompilerBackend, ibis_to_sqlglot
from sqlglot_tools.compat import ErrorLevel, exp
from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    SqlGlotPolicy,
    default_sqlglot_policy,
    normalize_expr,
    parse_sql_strict,
)


@dataclass(frozen=True)
class PlanDiffResult:
    """Semantic diff result for a pair of SQL strings."""

    changed: bool
    breaking: bool
    changes: tuple[str, ...] = ()
    edit_script: tuple[DiffOpEntry, ...] = ()


@dataclass(frozen=True)
class DiffOpEntry:
    """Summary entry for a SQLGlot diff operation."""

    op: str
    expression: str | None = None
    source: str | None = None
    target: str | None = None

    def payload(self) -> dict[str, str]:
        """Return a JSON-ready payload for the diff entry.

        Returns
        -------
        dict[str, str]
            JSON-ready payload for the diff entry.
        """
        payload: dict[str, str] = {"op": self.op}
        if self.expression is not None:
            payload["expression"] = self.expression
        if self.source is not None:
            payload["source"] = self.source
        if self.target is not None:
            payload["target"] = self.target
        return payload


@dataclass(frozen=True)
class SemanticDiffOptions:
    """Configuration options for semantic diffing."""

    dialect: str = "datafusion"
    schema_map: dict[str, dict[str, str]] | None = None
    policy: SqlGlotPolicy | None = None


def semantic_diff_sql(
    left_sql: str,
    right_sql: str,
    *,
    dialect: str = "datafusion",
    schema_map: dict[str, dict[str, str]] | None = None,
    policy: SqlGlotPolicy | None = None,
) -> PlanDiffResult:
    """Return semantic diff between two SQL strings.

    Returns
    -------
    PlanDiffResult
        Diff result indicating whether semantic changes exist.
    """
    policy = _policy_with_dialect(policy, dialect=dialect)
    try:
        left = parse_sql_strict(left_sql, dialect=dialect, error_level=policy.error_level)
        right = parse_sql_strict(right_sql, dialect=dialect, error_level=policy.error_level)
    except (TypeError, ValueError):
        return PlanDiffResult(
            changed=True,
            breaking=True,
            changes=("parse_error",),
            edit_script=(),
        )
    left_norm = normalize_expr(
        left,
        options=NormalizeExprOptions(
            schema=schema_map,
            policy=policy,
            sql=left_sql,
        ),
    )
    right_norm = normalize_expr(
        right,
        options=NormalizeExprOptions(
            schema=schema_map,
            policy=policy,
            sql=right_sql,
        ),
    )
    return _semantic_diff_from_exprs(left_norm, right_norm, policy=policy)


def semantic_diff_expr(
    left: IbisTable,
    right: IbisTable,
    *,
    backend: IbisCompilerBackend,
    options: SemanticDiffOptions | None = None,
) -> PlanDiffResult:
    """Return semantic diff between two Ibis expressions.

    Returns
    -------
    PlanDiffResult
        Diff result indicating whether semantic changes exist.
    """
    resolved = options or SemanticDiffOptions()
    policy = _policy_with_dialect(resolved.policy, dialect=resolved.dialect)
    left_expr = ibis_to_sqlglot(left, backend=backend, params=None)
    right_expr = ibis_to_sqlglot(right, backend=backend, params=None)
    left_norm = normalize_expr(
        left_expr,
        options=NormalizeExprOptions(schema=resolved.schema_map, policy=policy),
    )
    right_norm = normalize_expr(
        right_expr,
        options=NormalizeExprOptions(schema=resolved.schema_map, policy=policy),
    )
    return _semantic_diff_from_exprs(left_norm, right_norm, policy=policy)


def _policy_with_dialect(policy: SqlGlotPolicy | None, *, dialect: str) -> SqlGlotPolicy:
    resolved = policy or default_sqlglot_policy()
    if not dialect:
        return resolved
    return replace(resolved, read_dialect=dialect, write_dialect=dialect)


def _semantic_diff_from_exprs(
    left: exp.Expression,
    right: exp.Expression,
    *,
    policy: SqlGlotPolicy,
) -> PlanDiffResult:
    changes = [change for change in diff(left, right) if not isinstance(change, Keep)]
    edit_script = tuple(_diff_entry(change, dialect=policy.write_dialect) for change in changes)
    labels = [change.__class__.__name__.lower() for change in changes]
    window_changed = _window_signature(left, policy=policy) != _window_signature(
        right, policy=policy
    )
    join_changed = _join_signature(left, policy=policy) != _join_signature(right, policy=policy)
    if window_changed:
        labels.append("window_change")
    if join_changed:
        labels.append("join_change")
    if not labels:
        return PlanDiffResult(changed=False, breaking=False, changes=(), edit_script=())
    breaking = any(not isinstance(change, Insert) for change in changes)
    if window_changed or join_changed:
        breaking = True
    return PlanDiffResult(
        changed=True,
        breaking=breaking,
        changes=tuple(labels),
        edit_script=edit_script,
    )


def _window_signature(expr: exp.Expression, *, policy: SqlGlotPolicy) -> tuple[str, ...]:
    windows = [
        _expr_sql(window, dialect=policy.write_dialect) for window in expr.find_all(exp.Window)
    ]
    return tuple(sorted(windows))


def _join_signature(expr: exp.Expression, *, policy: SqlGlotPolicy) -> tuple[str, ...]:
    joins = [_expr_sql(join, dialect=policy.write_dialect) for join in expr.find_all(exp.Join)]
    return tuple(sorted(joins))


def _diff_entry(change: object, *, dialect: str) -> DiffOpEntry:
    op = change.__class__.__name__.lower()
    expression = _expr_sql(getattr(change, "expression", None), dialect=dialect)
    source = _expr_sql(getattr(change, "source", None), dialect=dialect)
    target = _expr_sql(getattr(change, "target", None), dialect=dialect)
    return DiffOpEntry(op=op, expression=expression, source=source, target=target)


def _expr_sql(expr: object, *, dialect: str | None = None) -> str:
    if expr is None:
        return ""
    if isinstance(expr, exp.Expression):
        try:
            return expr.sql(
                dialect=dialect,
                unsupported_level=ErrorLevel.RAISE,
            )
        except (AttributeError, TypeError, ValueError):
            return str(expr)
    return str(expr)


__all__ = ["DiffOpEntry", "PlanDiffResult", "semantic_diff_expr", "semantic_diff_sql"]
