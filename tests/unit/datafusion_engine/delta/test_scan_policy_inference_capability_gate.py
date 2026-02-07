"""Tests for capability-gated scan policy inference behavior."""

from __future__ import annotations

from datafusion_engine.delta.scan_policy_inference import derive_scan_policy_overrides
from datafusion_engine.lineage.datafusion import LineageReport, ScanLineage
from datafusion_engine.plan.signals import NormalizedPlanStats, PlanSignals
from schema_spec.system import DeltaScanPolicyDefaults, ScanPolicyConfig, ScanPolicyDefaults


def _policy(
    *,
    collect_statistics: bool | None,
    enable_parquet_pushdown: bool | None,
) -> ScanPolicyConfig:
    return ScanPolicyConfig(
        listing=ScanPolicyDefaults(collect_statistics=collect_statistics),
        delta_listing=ScanPolicyDefaults(collect_statistics=collect_statistics),
        delta_scan=DeltaScanPolicyDefaults(enable_parquet_pushdown=enable_parquet_pushdown),
    )


def test_missing_stats_do_not_trigger_small_table_override_when_capabilities_unavailable() -> None:
    """Small-table inference should not trigger when row stats are unavailable."""
    signals = PlanSignals(
        lineage=LineageReport(
            scans=(ScanLineage(dataset_name="dataset_a"),),
        ),
        stats=NormalizedPlanStats(num_rows=None, stats_source="unavailable"),
    )
    overrides = derive_scan_policy_overrides(
        signals,
        base_policy=_policy(
            collect_statistics=True,
            enable_parquet_pushdown=False,
        ),
        capability_snapshot={
            "execution_metrics": None,
            "plan_capabilities": {"has_execution_plan_statistics": False},
        },
    )
    assert overrides == ()


def test_missing_stats_still_allow_lineage_based_pushdown_override() -> None:
    """Lineage-driven heuristics should remain active even when stats are missing."""
    signals = PlanSignals(
        lineage=LineageReport(
            scans=(
                ScanLineage(
                    dataset_name="dataset_a",
                    pushed_filters=("col_a > 1",),
                ),
            ),
        ),
        stats=NormalizedPlanStats(num_rows=None, stats_source="unavailable"),
    )
    overrides = derive_scan_policy_overrides(
        signals,
        base_policy=_policy(
            collect_statistics=True,
            enable_parquet_pushdown=False,
        ),
        capability_snapshot={
            "execution_metrics": None,
            "plan_capabilities": {"has_execution_plan_statistics": False},
        },
    )
    assert len(overrides) == 1
    override = overrides[0]
    assert override.dataset_name == "dataset_a"
    assert override.reasons == ("has_pushed_filters",)
    assert override.policy.delta_scan.enable_parquet_pushdown is True
    assert override.confidence == 0.8


def test_available_row_stats_still_drive_small_table_inference() -> None:
    """Small-table overrides should still trigger when concrete row stats exist."""
    signals = PlanSignals(
        lineage=LineageReport(
            scans=(ScanLineage(dataset_name="dataset_a"),),
        ),
        stats=NormalizedPlanStats(num_rows=100, stats_source="plan_details"),
    )
    overrides = derive_scan_policy_overrides(
        signals,
        base_policy=_policy(
            collect_statistics=True,
            enable_parquet_pushdown=False,
        ),
    )
    assert len(overrides) == 1
    override = overrides[0]
    assert override.dataset_name == "dataset_a"
    assert override.reasons == ("small_table",)
    assert override.policy.listing.collect_statistics is False
    # No capability_snapshot passed, but stats.num_rows is set -> confidence = 0.7
    assert override.confidence == 0.7


def test_small_table_with_capable_snapshot_has_high_confidence() -> None:
    """Confidence should be 0.9 when both capabilities and stats are available."""
    signals = PlanSignals(
        lineage=LineageReport(
            scans=(ScanLineage(dataset_name="dataset_a"),),
        ),
        stats=NormalizedPlanStats(num_rows=100, stats_source="plan_details"),
    )
    overrides = derive_scan_policy_overrides(
        signals,
        base_policy=_policy(
            collect_statistics=True,
            enable_parquet_pushdown=False,
        ),
        capability_snapshot={
            "execution_metrics": {"status": "available"},
            "plan_capabilities": {"has_execution_plan_statistics": True},
        },
    )
    assert len(overrides) == 1
    override = overrides[0]
    assert override.reasons == ("small_table",)
    assert override.confidence == 0.9


def test_pushed_filters_and_small_table_takes_minimum_confidence() -> None:
    """When both reasons fire, confidence should be the minimum of the two."""
    signals = PlanSignals(
        lineage=LineageReport(
            scans=(
                ScanLineage(
                    dataset_name="dataset_a",
                    pushed_filters=("col_a > 1",),
                ),
            ),
        ),
        stats=NormalizedPlanStats(num_rows=100, stats_source="plan_details"),
    )
    overrides = derive_scan_policy_overrides(
        signals,
        base_policy=_policy(
            collect_statistics=True,
            enable_parquet_pushdown=False,
        ),
    )
    assert len(overrides) == 1
    override = overrides[0]
    assert "small_table" in override.reasons
    assert "has_pushed_filters" in override.reasons
    # small_table without capability => 0.7, pushed_filters => 0.8, min = 0.7
    assert override.confidence == 0.7
