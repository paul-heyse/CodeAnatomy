"""Incremental helpers for plan-catalog diffs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from ibis_engine.plan_diff import PlanDiffResult, semantic_diff_sql
from incremental.plan_fingerprints import PlanFingerprintSnapshot
from relspec.plan_catalog import PlanCatalog
from sqlglot_tools.optimizer import sqlglot_sql


@dataclass(frozen=True)
class IncrementalDiff:
    """Summary of plan fingerprint changes between catalogs."""

    changed_tasks: tuple[str, ...]
    added_tasks: tuple[str, ...] = ()
    removed_tasks: tuple[str, ...] = ()
    unchanged_tasks: tuple[str, ...] = ()
    semantic_changes: Mapping[str, PlanDiffResult] = field(default_factory=dict)


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
    prev_snapshots = plan_snapshot_map(prev)
    curr_snapshots = plan_snapshot_map(curr)
    return diff_plan_snapshots(prev_snapshots, curr_snapshots)


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


def diff_plan_snapshots(
    prev: Mapping[str, PlanFingerprintSnapshot],
    curr: Mapping[str, PlanFingerprintSnapshot],
) -> IncrementalDiff:
    """Return a diff summary between two plan snapshot mappings.

    Parameters
    ----------
    prev : Mapping[str, PlanFingerprintSnapshot]
        Previous plan snapshot mapping keyed by task name.
    curr : Mapping[str, PlanFingerprintSnapshot]
        Current plan snapshot mapping keyed by task name.

    Returns
    -------
    IncrementalDiff
        Diff summary with semantic diff metadata when available.
    """
    prev_fingerprints = {name: snap.plan_fingerprint for name, snap in prev.items()}
    curr_fingerprints = {name: snap.plan_fingerprint for name, snap in curr.items()}
    diff = diff_plan_fingerprints(prev_fingerprints, curr_fingerprints)
    semantic = _semantic_diff_map(prev, curr, diff.changed_tasks)
    return IncrementalDiff(
        changed_tasks=diff.changed_tasks,
        added_tasks=diff.added_tasks,
        removed_tasks=diff.removed_tasks,
        unchanged_tasks=diff.unchanged_tasks,
        semantic_changes=semantic,
    )


def plan_fingerprint_map(catalog: PlanCatalog) -> dict[str, str]:
    """Return a mapping of task names to plan fingerprints.

    Returns
    -------
    dict[str, str]
        Mapping of task name to plan fingerprint.
    """
    return {artifact.task.name: artifact.plan_fingerprint for artifact in catalog.artifacts}


def plan_snapshot_map(catalog: PlanCatalog) -> dict[str, PlanFingerprintSnapshot]:
    """Return a mapping of task names to plan snapshots.

    Returns
    -------
    dict[str, PlanFingerprintSnapshot]
        Mapping of task name to snapshot metadata.
    """
    snapshots: dict[str, PlanFingerprintSnapshot] = {}
    for artifact in catalog.artifacts:
        snapshots[artifact.task.name] = PlanFingerprintSnapshot(
            plan_fingerprint=artifact.plan_fingerprint,
            plan_sql=sqlglot_sql(artifact.sqlglot_ast),
        )
    return snapshots


def _semantic_diff_map(
    prev: Mapping[str, PlanFingerprintSnapshot],
    curr: Mapping[str, PlanFingerprintSnapshot],
    changed_tasks: tuple[str, ...],
) -> dict[str, PlanDiffResult]:
    results: dict[str, PlanDiffResult] = {}
    for name in changed_tasks:
        prev_snapshot = prev.get(name)
        curr_snapshot = curr.get(name)
        if prev_snapshot is None or curr_snapshot is None:
            continue
        if prev_snapshot.plan_sql is None or curr_snapshot.plan_sql is None:
            continue
        results[name] = semantic_diff_sql(
            prev_snapshot.plan_sql,
            curr_snapshot.plan_sql,
            dialect="datafusion",
        )
    return results


__all__ = [
    "IncrementalDiff",
    "diff_plan_catalog",
    "diff_plan_fingerprints",
    "diff_plan_snapshots",
    "plan_fingerprint_map",
    "plan_snapshot_map",
]
