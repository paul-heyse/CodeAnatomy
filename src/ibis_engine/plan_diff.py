"""Semantic SQLGlot diff helpers for plan invalidation."""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot import ErrorLevel, parse_one
from sqlglot.diff import Keep, diff


@dataclass(frozen=True)
class PlanDiffResult:
    """Semantic diff result for a pair of SQL strings."""

    changed: bool
    changes: tuple[str, ...] = ()


def semantic_diff_sql(
    left_sql: str,
    right_sql: str,
    *,
    dialect: str = "datafusion",
) -> PlanDiffResult:
    """Return semantic diff between two SQL strings.

    Returns
    -------
    PlanDiffResult
        Diff result indicating whether semantic changes exist.
    """
    try:
        left = parse_one(left_sql, dialect=dialect, error_level=ErrorLevel.RAISE)
        right = parse_one(right_sql, dialect=dialect, error_level=ErrorLevel.RAISE)
    except (TypeError, ValueError):
        return PlanDiffResult(changed=True, changes=("parse_error",))
    changes = [change for change in diff(left, right) if not isinstance(change, Keep)]
    if not changes:
        return PlanDiffResult(changed=False, changes=())
    labels = tuple(change.__class__.__name__.lower() for change in changes)
    return PlanDiffResult(changed=True, changes=labels)


__all__ = ["PlanDiffResult", "semantic_diff_sql"]
