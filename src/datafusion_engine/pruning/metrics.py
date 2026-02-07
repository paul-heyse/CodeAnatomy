"""File-level pruning metrics for DataFusion query executions.

Provide a strict, frozen contract for pruning effectiveness data
extracted from explain-analyze output or runtime instrumentation.
"""

from __future__ import annotations

from serde_msgspec import StructBaseStrict


class PruningMetrics(StructBaseStrict, frozen=True):
    """File-level pruning metrics from a single query execution.

    Parameters
    ----------
    row_groups_total
        Total row groups considered by the scan operator.
    row_groups_pruned
        Row groups eliminated by statistics-based pruning.
    pages_total
        Total column pages considered by the scan operator.
    pages_pruned
        Column pages eliminated by page-index pruning.
    filters_pushed
        Number of filter predicates pushed down to the scan.
    statistics_available
        Whether file-level statistics were available for pruning.
    pruning_effectiveness
        Ratio of pruned row groups to total row groups (0.0--1.0).
    """

    row_groups_total: int = 0
    row_groups_pruned: int = 0
    pages_total: int = 0
    pages_pruned: int = 0
    filters_pushed: int = 0
    statistics_available: bool = False
    pruning_effectiveness: float = 0.0


__all__ = [
    "PruningMetrics",
]
