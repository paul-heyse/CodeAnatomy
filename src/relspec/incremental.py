"""Incremental helpers for plan-catalog diffs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from datafusion_engine.semantic_diff import (
    ChangeCategory,
    RebuildPolicy,
    SemanticChange,
    SemanticDiff,
)
from ibis_engine.registry import DatasetCatalog
from incremental.cdf_cursors import CdfCursorStore
from incremental.cdf_filters import CdfFilterPolicy
from incremental.cdf_runtime import read_cdf_changes
from incremental.delta_context import DeltaAccessContext
from incremental.plan_fingerprints import PlanFingerprintSnapshot
from relspec.evidence import EvidenceCatalog
from relspec.rustworkx_graph import TaskGraph
from relspec.rustworkx_schedule import impacted_tasks

if TYPE_CHECKING:
    from datafusion_engine.view_graph_registry import ViewNode


@dataclass(frozen=True)
class IncrementalDiff:
    """Summary of plan fingerprint changes between catalogs."""

    changed_tasks: tuple[str, ...]
    added_tasks: tuple[str, ...] = ()
    removed_tasks: tuple[str, ...] = ()
    unchanged_tasks: tuple[str, ...] = ()
    semantic_changes: Mapping[str, SemanticDiff] = field(default_factory=dict)

    def tasks_requiring_rebuild(
        self,
        *,
        policy: RebuildPolicy = RebuildPolicy.CONSERVATIVE,
    ) -> tuple[str, ...]:
        """Return task names that require rebuild under the policy.

        Returns
        -------
        tuple[str, ...]
            Task names that should be rebuilt.
        """
        rebuild: set[str] = set(self.added_tasks)
        for name in self.changed_tasks:
            change = self.semantic_changes.get(name)
            if change is None or change.requires_rebuild(policy):
                rebuild.add(name)
        return tuple(sorted(rebuild))


@dataclass(frozen=True)
class CdfImpactRequest:
    """Inputs for determining CDF-driven task impacts."""

    graph: TaskGraph
    catalog: DatasetCatalog
    context: DeltaAccessContext
    cursor_store: CdfCursorStore
    evidence: EvidenceCatalog | None = None
    filter_policy: CdfFilterPolicy | None = None


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


def impacted_tasks_for_cdf(request: CdfImpactRequest) -> tuple[str, ...]:
    """Return tasks impacted by CDF changes in registered datasets.

    Returns
    -------
    tuple[str, ...]
        Sorted task names impacted by datasets with CDF changes.
    """
    evidence_names = (
        request.evidence.sources if request.evidence is not None else set(request.catalog.names())
    )
    impacted: set[str] = set()
    for name in sorted(evidence_names):
        if not request.catalog.has(name):
            continue
        if request.evidence is not None:
            supports = request.evidence.supports_cdf(name)
            if supports is False:
                continue
        location = request.catalog.get(name)
        if location.format != "delta":
            continue
        result = read_cdf_changes(
            request.context,
            dataset_path=str(location.path),
            dataset_name=name,
            cursor_store=request.cursor_store,
            filter_policy=request.filter_policy,
        )
        if result is None:
            continue
        impacted.update(impacted_tasks(request.graph, evidence_name=name))
    return tuple(sorted(impacted))


def view_fingerprint_map(nodes: Sequence[ViewNode]) -> dict[str, str]:
    """Return a mapping of view names to plan fingerprints.

    Returns
    -------
    dict[str, str]
        Mapping of view name to plan fingerprint.
    """
    from relspec.inferred_deps import infer_deps_from_view_nodes

    inferred = infer_deps_from_view_nodes(nodes)
    return {dep.task_name: dep.plan_fingerprint for dep in inferred}


def view_snapshot_map(nodes: Sequence[ViewNode]) -> dict[str, PlanFingerprintSnapshot]:
    """Return a mapping of view names to plan snapshots.

    Returns
    -------
    dict[str, PlanFingerprintSnapshot]
        Mapping of view name to snapshot metadata.

    Raises
    ------
    ValueError
        Raised when a view node is missing a SQLGlot AST.
    """
    from relspec.inferred_deps import infer_deps_from_view_nodes

    inferred = {dep.task_name: dep for dep in infer_deps_from_view_nodes(nodes)}
    snapshots: dict[str, PlanFingerprintSnapshot] = {}
    for node in nodes:
        if node.sqlglot_ast is None:
            msg = f"View node {node.name!r} is missing a SQLGlot AST."
            raise ValueError(msg)
        dep = inferred.get(node.name)
        fingerprint = dep.plan_fingerprint if dep is not None else ""
        snapshots[node.name] = PlanFingerprintSnapshot(
            plan_fingerprint=fingerprint,
            sqlglot_ast=node.sqlglot_ast,
        )
    return snapshots


def _semantic_diff_map(
    prev: Mapping[str, PlanFingerprintSnapshot],
    curr: Mapping[str, PlanFingerprintSnapshot],
    changed_tasks: tuple[str, ...],
) -> dict[str, SemanticDiff]:
    results: dict[str, SemanticDiff] = {}
    for name in changed_tasks:
        prev_snapshot = prev.get(name)
        curr_snapshot = curr.get(name)
        if prev_snapshot is None or curr_snapshot is None:
            continue
        if prev_snapshot.sqlglot_ast is None or curr_snapshot.sqlglot_ast is None:
            continue
        try:
            diff = SemanticDiff.compute(
                prev_snapshot.sqlglot_ast,
                curr_snapshot.sqlglot_ast,
            )
        except (TypeError, ValueError) as exc:
            diff = SemanticDiff(
                changes=[
                    SemanticChange(
                        edit_type="diff_error",
                        node_type=type(exc).__name__,
                        description=str(exc) or "AST diff error",
                        category=ChangeCategory.BREAKING,
                    )
                ]
            )
        results[name] = diff
    return results


__all__ = [
    "CdfImpactRequest",
    "IncrementalDiff",
    "diff_plan_fingerprints",
    "diff_plan_snapshots",
    "impacted_tasks_for_cdf",
    "view_fingerprint_map",
    "view_snapshot_map",
]
