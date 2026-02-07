"""Accumulate pruning metrics across a pipeline run.

Provide a mutable ``PruningTracker`` for collecting per-view pruning
data and a frozen ``PruningSummary`` for aggregate reporting.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from datafusion_engine.pruning.metrics import PruningMetrics


@dataclass(frozen=True)
class PruningSummary:
    """Aggregate pruning summary for a pipeline run.

    Parameters
    ----------
    total_queries
        Number of queries tracked.
    queries_with_pruning
        Number of queries where any row groups were pruned.
    avg_effectiveness
        Mean pruning effectiveness across queries with pruning data.
    total_row_groups_pruned
        Sum of pruned row groups across all tracked queries.
    total_row_groups_total
        Sum of total row groups across all tracked queries.
    """

    total_queries: int = 0
    queries_with_pruning: int = 0
    avg_effectiveness: float = 0.0
    total_row_groups_pruned: int = 0
    total_row_groups_total: int = 0


@dataclass
class PruningTracker:
    """Accumulate pruning metrics across multiple queries in a pipeline run.

    Parameters
    ----------
    _entries
        Internal list of (view_name, metrics) tuples.
    """

    _entries: list[tuple[str, PruningMetrics]] = field(default_factory=list)

    def record(self, view_name: str, metrics: PruningMetrics) -> None:
        """Record pruning metrics for a view execution.

        Parameters
        ----------
        view_name
            Logical name of the view that was executed.
        metrics
            Pruning metrics from the view execution.
        """
        self._entries.append((view_name, metrics))

    def summary(self) -> PruningSummary:
        """Compute an aggregate pruning summary.

        Returns:
        -------
        PruningSummary
            Aggregate statistics across all recorded entries.
        """
        if not self._entries:
            return PruningSummary()

        total_queries = len(self._entries)
        queries_with_pruning = 0
        effectiveness_sum = 0.0
        total_rg_pruned = 0
        total_rg_total = 0

        for _name, metrics in self._entries:
            total_rg_pruned += metrics.row_groups_pruned
            total_rg_total += metrics.row_groups_total
            if metrics.row_groups_pruned > 0:
                queries_with_pruning += 1
                effectiveness_sum += metrics.pruning_effectiveness

        avg_effectiveness = 0.0
        if queries_with_pruning > 0:
            avg_effectiveness = effectiveness_sum / queries_with_pruning

        return PruningSummary(
            total_queries=total_queries,
            queries_with_pruning=queries_with_pruning,
            avg_effectiveness=avg_effectiveness,
            total_row_groups_pruned=total_rg_pruned,
            total_row_groups_total=total_rg_total,
        )


__all__ = [
    "PruningSummary",
    "PruningTracker",
]
