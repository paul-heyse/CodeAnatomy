"""Test workload classification from plan signals."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock

from datafusion_engine.plan.signals import (
    NormalizedPlanStats,
    PlanSignals,
    ScanUnitCompatSummary,
)
from datafusion_engine.workload.classifier import WorkloadClass, classify_workload

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _LineageParams:
    """Lineage mock parameters for test signal construction."""

    scans: int = 0
    aggregations: int = 0


def _signals(
    *,
    num_rows: int | None = None,
    total_bytes: int | None = None,
    partition_count: int | None = None,
    scan_compat: tuple[ScanUnitCompatSummary, ...] = (),
    plan_fingerprint: str | None = None,
    lineage_params: _LineageParams | None = None,
) -> PlanSignals:
    """Build a PlanSignals with controlled stats and lineage.

    Parameters
    ----------
    num_rows
        Row count for plan statistics.
    total_bytes
        Byte size for plan statistics.
    partition_count
        Partition count for plan statistics.
    scan_compat
        Scan compatibility entries.
    plan_fingerprint
        Plan fingerprint string.
    lineage_params
        Optional lineage mock configuration.

    Returns:
    -------
    PlanSignals
        Constructed test signal bundle.
    """
    stats = NormalizedPlanStats(
        num_rows=num_rows,
        total_bytes=total_bytes,
        partition_count=partition_count,
        stats_source="test",
    )
    lineage = None
    if lineage_params is not None:
        lineage = MagicMock()
        lineage.scans = tuple(MagicMock() for _ in range(lineage_params.scans))
        lineage.aggregations = tuple(f"agg_{i}" for i in range(lineage_params.aggregations))
    return PlanSignals(
        stats=stats,
        scan_compat=scan_compat,
        plan_fingerprint=plan_fingerprint,
        lineage=lineage,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestClassifyWorkloadBatchIngest:
    """Test batch ingest classification."""

    def test_large_row_count_defaults_to_batch(self) -> None:
        """Large row counts classify as BATCH_INGEST."""
        signals = _signals(num_rows=2_000_000)
        assert classify_workload(signals) == WorkloadClass.BATCH_INGEST

    def test_large_byte_size_defaults_to_batch(self) -> None:
        """Large byte sizes classify as BATCH_INGEST."""
        signals = _signals(total_bytes=500_000_000)
        assert classify_workload(signals) == WorkloadClass.BATCH_INGEST

    def test_empty_signals_default_to_batch(self) -> None:
        """Empty signals default to BATCH_INGEST."""
        signals = PlanSignals()
        assert classify_workload(signals) == WorkloadClass.BATCH_INGEST

    def test_no_stats_defaults_to_batch(self) -> None:
        """Signals with no stats and no special markers default to batch."""
        signals = PlanSignals(stats=None)
        assert classify_workload(signals) == WorkloadClass.BATCH_INGEST


class TestClassifyWorkloadInteractiveQuery:
    """Test interactive query classification."""

    def test_small_row_count_is_interactive(self) -> None:
        """Small row counts with few scans classify as INTERACTIVE_QUERY."""
        signals = _signals(num_rows=500, lineage_params=_LineageParams(scans=1))
        assert classify_workload(signals) == WorkloadClass.INTERACTIVE_QUERY

    def test_few_scans_no_aggregations_is_interactive(self) -> None:
        """Few scans without aggregations classify as INTERACTIVE_QUERY."""
        signals = _signals(num_rows=100, lineage_params=_LineageParams(scans=2))
        assert classify_workload(signals) == WorkloadClass.INTERACTIVE_QUERY

    def test_many_scans_not_interactive(self) -> None:
        """Many scans prevent interactive classification."""
        signals = _signals(num_rows=100, lineage_params=_LineageParams(scans=5))
        assert classify_workload(signals) != WorkloadClass.INTERACTIVE_QUERY

    def test_aggregations_prevent_interactive(self) -> None:
        """Aggregations prevent interactive classification."""
        signals = _signals(
            num_rows=100,
            lineage_params=_LineageParams(scans=1, aggregations=2),
        )
        assert classify_workload(signals) != WorkloadClass.INTERACTIVE_QUERY

    def test_high_row_count_not_interactive(self) -> None:
        """Row count at threshold prevents interactive classification."""
        signals = _signals(num_rows=10_000, lineage_params=_LineageParams(scans=1))
        assert classify_workload(signals) != WorkloadClass.INTERACTIVE_QUERY


class TestClassifyWorkloadCompileReplay:
    """Test compile replay classification."""

    def test_fingerprint_no_stats_is_replay(self) -> None:
        """Plan fingerprint with no stats is COMPILE_REPLAY."""
        signals = PlanSignals(plan_fingerprint="abc123", stats=None)
        assert classify_workload(signals) == WorkloadClass.COMPILE_REPLAY

    def test_fingerprint_zero_stats_is_replay(self) -> None:
        """Plan fingerprint with zero row/byte stats is COMPILE_REPLAY."""
        signals = _signals(plan_fingerprint="abc123", num_rows=0, total_bytes=0)
        assert classify_workload(signals) == WorkloadClass.COMPILE_REPLAY

    def test_fingerprint_with_real_stats_not_replay(self) -> None:
        """Plan fingerprint with actual statistics is not COMPILE_REPLAY."""
        signals = _signals(plan_fingerprint="abc123", num_rows=50_000)
        assert classify_workload(signals) != WorkloadClass.COMPILE_REPLAY

    def test_no_fingerprint_never_replay(self) -> None:
        """Without plan fingerprint, never classify as COMPILE_REPLAY."""
        signals = _signals(num_rows=0, total_bytes=0)
        assert classify_workload(signals) != WorkloadClass.COMPILE_REPLAY


class TestClassifyWorkloadIncrementalUpdate:
    """Test incremental update classification."""

    def test_scan_compat_small_partitions_is_incremental(self) -> None:
        """Scan compat with small partition count is INCREMENTAL_UPDATE."""
        compat = (ScanUnitCompatSummary(dataset_name="table_a", compatible=True),)
        signals = _signals(scan_compat=compat, partition_count=2)
        assert classify_workload(signals) == WorkloadClass.INCREMENTAL_UPDATE

    def test_scan_compat_no_stats_is_incremental(self) -> None:
        """Scan compat with no stats is INCREMENTAL_UPDATE."""
        compat = (ScanUnitCompatSummary(dataset_name="table_a"),)
        signals = PlanSignals(scan_compat=compat, stats=None)
        assert classify_workload(signals) == WorkloadClass.INCREMENTAL_UPDATE

    def test_no_scan_compat_not_incremental(self) -> None:
        """Without scan compat entries, never classify as INCREMENTAL_UPDATE."""
        signals = _signals(partition_count=2)
        assert classify_workload(signals) != WorkloadClass.INCREMENTAL_UPDATE


class TestClassifyWorkloadEdgeCases:
    """Test boundary conditions and edge cases."""

    def test_all_workload_classes_reachable(self) -> None:
        """Verify every WorkloadClass value is producible."""
        reachable = set()

        reachable.add(classify_workload(PlanSignals()))

        reachable.add(
            classify_workload(_signals(num_rows=100, lineage_params=_LineageParams(scans=1)))
        )

        reachable.add(classify_workload(PlanSignals(plan_fingerprint="fp1", stats=None)))

        compat = (ScanUnitCompatSummary(dataset_name="t", compatible=True),)
        reachable.add(classify_workload(PlanSignals(scan_compat=compat, stats=None)))

        assert reachable == set(WorkloadClass)

    def test_workload_class_string_values(self) -> None:
        """WorkloadClass members have expected string values."""
        assert WorkloadClass.BATCH_INGEST == "batch_ingest"
        assert WorkloadClass.INTERACTIVE_QUERY == "interactive_query"
        assert WorkloadClass.COMPILE_REPLAY == "compile_replay"
        assert WorkloadClass.INCREMENTAL_UPDATE == "incremental_update"

    def test_priority_compile_replay_over_incremental(self) -> None:
        """Compile replay takes priority over incremental when both match."""
        compat = (ScanUnitCompatSummary(dataset_name="t", compatible=True),)
        signals = PlanSignals(
            plan_fingerprint="fp1",
            scan_compat=compat,
            stats=None,
        )
        assert classify_workload(signals) == WorkloadClass.COMPILE_REPLAY
