"""Workload-specific DataFusion session configuration profiles.

Map a ``WorkloadClass`` to a ``WorkloadSessionProfile`` containing
session configuration adjustments suitable for the workload type.
"""

from __future__ import annotations

from dataclasses import dataclass

from datafusion_engine.workload.classifier import WorkloadClass


@dataclass(frozen=True)
class WorkloadSessionProfile:
    """Session configuration adjustments for a workload class.

    Each field is optional; ``None`` means "use the session default".

    Parameters
    ----------
    target_partitions
        Number of target partitions for the session.
    batch_size
        Batch size for record-batch processing.
    repartition_aggregations
        Whether to repartition before aggregation operators.
    repartition_file_scans
        Whether to repartition file scans across partitions.
    sort_spill_reservation_bytes
        Reserved bytes for sort spill to disk.
    memory_fraction
        Fraction of available memory to target for the execution pool.
    """

    target_partitions: int | None = None
    batch_size: int | None = None
    repartition_aggregations: bool | None = None
    repartition_file_scans: bool | None = None
    sort_spill_reservation_bytes: int | None = None
    memory_fraction: float | None = None


# ---------------------------------------------------------------------------
# Profile definitions
# ---------------------------------------------------------------------------

_BATCH_INGEST_PROFILE = WorkloadSessionProfile(
    target_partitions=8,
    batch_size=16384,
    repartition_aggregations=True,
    repartition_file_scans=True,
    sort_spill_reservation_bytes=64 * 1024 * 1024,
    memory_fraction=0.8,
)

_INTERACTIVE_QUERY_PROFILE = WorkloadSessionProfile(
    target_partitions=2,
    batch_size=4096,
    repartition_aggregations=False,
    repartition_file_scans=False,
    sort_spill_reservation_bytes=8 * 1024 * 1024,
    memory_fraction=0.4,
)

_COMPILE_REPLAY_PROFILE = WorkloadSessionProfile(
    target_partitions=1,
    batch_size=4096,
    repartition_aggregations=False,
    repartition_file_scans=False,
    sort_spill_reservation_bytes=4 * 1024 * 1024,
    memory_fraction=0.2,
)

_INCREMENTAL_UPDATE_PROFILE = WorkloadSessionProfile(
    target_partitions=4,
    batch_size=8192,
    repartition_aggregations=True,
    repartition_file_scans=True,
    sort_spill_reservation_bytes=16 * 1024 * 1024,
    memory_fraction=0.6,
)

_PROFILES: dict[WorkloadClass, WorkloadSessionProfile] = {
    WorkloadClass.BATCH_INGEST: _BATCH_INGEST_PROFILE,
    WorkloadClass.INTERACTIVE_QUERY: _INTERACTIVE_QUERY_PROFILE,
    WorkloadClass.COMPILE_REPLAY: _COMPILE_REPLAY_PROFILE,
    WorkloadClass.INCREMENTAL_UPDATE: _INCREMENTAL_UPDATE_PROFILE,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def workload_session_profile(workload_class: WorkloadClass) -> WorkloadSessionProfile:
    """Return the session profile for a given workload class.

    Parameters
    ----------
    workload_class
        The workload classification to look up.

    Returns:
    -------
    WorkloadSessionProfile
        Session configuration adjustments for the workload.
    """
    return _PROFILES.get(workload_class, _BATCH_INGEST_PROFILE)


__all__ = [
    "WorkloadSessionProfile",
    "workload_session_profile",
]
