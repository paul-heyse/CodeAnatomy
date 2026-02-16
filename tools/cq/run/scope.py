"""Run-plan scope merge helpers."""

from __future__ import annotations

from collections.abc import Iterable

from tools.cq.query.ir import Query, Scope
from tools.cq.run.helpers import merge_in_dir


def merge_excludes(
    run_exclude: Iterable[str],
    step_exclude: Iterable[str],
) -> list[str]:
    """Merge run and step exclude patterns with stable de-duplication.

    Returns:
        list[str]: Ordered unique exclusion patterns.
    """
    seen: set[str] = set()
    merged: list[str] = []
    for item in list(run_exclude) + list(step_exclude):
        if item in seen:
            continue
        seen.add(item)
        merged.append(item)
    return merged


def apply_run_scope(query: Query, in_dir: str | None, exclude: tuple[str, ...]) -> Query:
    """Apply plan-level scope overlays onto a step query.

    Returns:
        Query: Query with merged run/step scope overlays.
    """
    if not in_dir and not exclude:
        return query
    scope = query.scope
    merged = Scope(
        in_dir=merge_in_dir(in_dir, scope.in_dir),
        exclude=tuple(merge_excludes(exclude, scope.exclude)),
        globs=scope.globs,
    )
    return query.with_scope(merged)


__all__ = ["apply_run_scope", "merge_excludes"]
