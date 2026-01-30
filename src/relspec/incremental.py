"""Incremental helpers for CDF-driven planning."""

from __future__ import annotations

from dataclasses import dataclass

from datafusion_engine.dataset.registry import DatasetCatalog
from incremental.cdf_cursors import CdfCursorStore
from incremental.cdf_filters import CdfFilterPolicy
from incremental.cdf_runtime import read_cdf_changes
from incremental.delta_context import DeltaAccessContext
from relspec.evidence import EvidenceCatalog
from relspec.rustworkx_graph import TaskGraph
from relspec.rustworkx_schedule import impacted_tasks


@dataclass(frozen=True)
class CdfImpactRequest:
    """Inputs for determining CDF-driven task impacts."""

    graph: TaskGraph
    catalog: DatasetCatalog
    context: DeltaAccessContext
    cursor_store: CdfCursorStore
    evidence: EvidenceCatalog | None = None
    filter_policy: CdfFilterPolicy | None = None


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


__all__ = [
    "CdfImpactRequest",
    "impacted_tasks_for_cdf",
]
