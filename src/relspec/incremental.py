"""Incremental helpers for plan-catalog diffs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from relspec.plan_catalog import PlanCatalog


@dataclass(frozen=True)
class IncrementalDiff:
    """Summary of plan fingerprint changes between catalogs."""

    changed_tasks: tuple[str, ...]
    added_tasks: tuple[str, ...] = ()
    removed_tasks: tuple[str, ...] = ()
    unchanged_tasks: tuple[str, ...] = ()


def diff_plan_catalog(prev: PlanCatalog, curr: PlanCatalog) -> IncrementalDiff:
    """Return a diff summary between two plan catalogs.

    Parameters
    ----------
    prev : PlanCatalog
        Previous plan catalog snapshot.
    curr : PlanCatalog
        Current plan catalog snapshot.

    Returns
    -------
    IncrementalDiff
        Diff summary based on plan fingerprints.
    """
    prev_map = plan_fingerprint_map(prev)
    curr_map = plan_fingerprint_map(curr)
    return diff_plan_fingerprints(prev_map, curr_map)


def diff_plan_fingerprints(
    prev: Mapping[str, str],
    curr: Mapping[str, str],
) -> IncrementalDiff:
    """Return a diff summary between two plan fingerprint mappings.

    Parameters
    ----------
    prev : Mapping[str, str]
        Previous plan fingerprint mapping keyed by task name.
    curr : Mapping[str, str]
        Current plan fingerprint mapping keyed by task name.

    Returns
    -------
    IncrementalDiff
        Diff summary based on plan fingerprints.
    """
    prev_names = set(prev)
    curr_names = set(curr)

    added = sorted(curr_names - prev_names)
    removed = sorted(prev_names - curr_names)
    common = prev_names & curr_names

    changed = sorted(name for name in common if prev[name] != curr[name])
    unchanged = sorted(name for name in common if prev[name] == curr[name])

    return IncrementalDiff(
        changed_tasks=tuple(changed),
        added_tasks=tuple(added),
        removed_tasks=tuple(removed),
        unchanged_tasks=tuple(unchanged),
    )


def plan_fingerprint_map(catalog: PlanCatalog) -> dict[str, str]:
    """Return a mapping of task names to plan fingerprints.

    Returns
    -------
    dict[str, str]
        Mapping of task name to plan fingerprint.
    """
    return {artifact.task.name: artifact.plan_fingerprint for artifact in catalog.artifacts}


__all__ = [
    "IncrementalDiff",
    "diff_plan_catalog",
    "diff_plan_fingerprints",
    "plan_fingerprint_map",
]
