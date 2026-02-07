"""Workload classification from plan signals.

Classify a ``PlanSignals`` bundle into a ``WorkloadClass`` to enable
workload-aware session configuration and resource tuning.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion_engine.plan.signals import PlanSignals

# ---------------------------------------------------------------------------
# Classification thresholds
# ---------------------------------------------------------------------------

_BATCH_ROW_THRESHOLD = 1_000_000
_BATCH_BYTES_THRESHOLD = 100_000_000
_INTERACTIVE_ROW_CEILING = 10_000
_INCREMENTAL_PARTITION_CEILING = 4
_INTERACTIVE_MAX_SCANS = 2
_BATCH_SCAN_THRESHOLD = 3


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


class WorkloadClass(StrEnum):
    """Workload classification derived from plan signal heuristics."""

    BATCH_INGEST = "batch_ingest"
    INTERACTIVE_QUERY = "interactive_query"
    COMPILE_REPLAY = "compile_replay"
    INCREMENTAL_UPDATE = "incremental_update"


def classify_workload(plan_signals: PlanSignals) -> WorkloadClass:
    """Classify a plan signal bundle into a workload category.

    The classification uses heuristics based on row counts, byte sizes,
    scan patterns, and plan fingerprint presence.  When signals are
    insufficient, the function defaults to ``BATCH_INGEST``.

    Parameters
    ----------
    plan_signals
        Typed signal bundle extracted from a DataFusion plan.

    Returns:
    -------
    WorkloadClass
        The inferred workload classification.
    """
    if _is_compile_replay(plan_signals):
        return WorkloadClass.COMPILE_REPLAY

    if _is_incremental_update(plan_signals):
        return WorkloadClass.INCREMENTAL_UPDATE

    if _is_interactive_query(plan_signals):
        return WorkloadClass.INTERACTIVE_QUERY

    return WorkloadClass.BATCH_INGEST


# ---------------------------------------------------------------------------
# Internal heuristics
# ---------------------------------------------------------------------------


def _is_compile_replay(signals: PlanSignals) -> bool:
    if signals.plan_fingerprint is None:
        return False
    stats = signals.stats
    if stats is None:
        return True
    has_rows = stats.num_rows is not None and stats.num_rows > 0
    has_bytes = stats.total_bytes is not None and stats.total_bytes > 0
    return not has_rows and not has_bytes


def _is_incremental_update(signals: PlanSignals) -> bool:
    if not signals.scan_compat:
        return False
    stats = signals.stats
    if stats is None:
        return True
    partition_count = stats.partition_count
    return partition_count is not None and partition_count <= _INCREMENTAL_PARTITION_CEILING


def _is_interactive_query(signals: PlanSignals) -> bool:
    stats = signals.stats
    if stats is None:
        return False
    if stats.num_rows is not None and stats.num_rows >= _INTERACTIVE_ROW_CEILING:
        return False
    lineage = signals.lineage
    if lineage is not None:
        if len(lineage.scans) > _INTERACTIVE_MAX_SCANS:
            return False
        if lineage.aggregations:
            return False
    return stats.num_rows is not None


def _is_batch_ingest(signals: PlanSignals) -> bool:
    stats = signals.stats
    if stats is not None:
        if stats.num_rows is not None and stats.num_rows > _BATCH_ROW_THRESHOLD:
            return True
        if stats.total_bytes is not None and stats.total_bytes > _BATCH_BYTES_THRESHOLD:
            return True
    lineage = signals.lineage
    return lineage is not None and len(lineage.scans) > _BATCH_SCAN_THRESHOLD


__all__ = [
    "WorkloadClass",
    "classify_workload",
]
