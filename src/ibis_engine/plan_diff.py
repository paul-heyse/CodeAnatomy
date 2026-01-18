"""Semantic SQLGlot diff helpers for plan invalidation."""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot import ErrorLevel, parse_one
from sqlglot.diff import Insert, Keep, diff

from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    SqlGlotPolicy,
    default_sqlglot_policy,
    normalize_expr,
)


@dataclass(frozen=True)
class PlanDiffResult:
    """Semantic diff result for a pair of SQL strings."""

    changed: bool
    breaking: bool
    changes: tuple[str, ...] = ()


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
    policy = policy or default_sqlglot_policy()
    error_level = policy.error_level if policy is not None else ErrorLevel.RAISE
    try:
        left = parse_one(left_sql, dialect=dialect, error_level=error_level)
        right = parse_one(right_sql, dialect=dialect, error_level=error_level)
    except (TypeError, ValueError):
        return PlanDiffResult(changed=True, breaking=True, changes=("parse_error",))
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
    changes = [change for change in diff(left_norm, right_norm) if not isinstance(change, Keep)]
    if not changes:
        return PlanDiffResult(changed=False, breaking=False, changes=())
    labels = tuple(change.__class__.__name__.lower() for change in changes)
    breaking = any(not isinstance(change, Insert) for change in changes)
    return PlanDiffResult(changed=True, breaking=breaking, changes=labels)


__all__ = ["PlanDiffResult", "semantic_diff_sql"]
