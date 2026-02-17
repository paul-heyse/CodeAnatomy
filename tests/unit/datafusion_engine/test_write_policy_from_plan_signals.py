"""Tests for adaptive write policy sizing driven by plan signals."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.io import write_core as write_module
from datafusion_engine.io import write_planning as write_planning_module
from datafusion_engine.io.write_core import (
    _AdaptiveFileSizeDecision,
)
from datafusion_engine.io.write_planning import (
    adaptive_file_size_from_bundle,
    compute_adaptive_file_size,
)
from datafusion_engine.plan.signals import NormalizedPlanStats, PlanSignals

SMALL_TABLE_ROWS = 100
LARGE_TABLE_ROWS = 2_000_000
SMALL_TABLE_CONTEXT_ROWS = 50

if TYPE_CHECKING:
    from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
else:
    DataFusionPlanArtifact = object


# ---------------------------------------------------------------------------
# compute_adaptive_file_size unit tests
# ---------------------------------------------------------------------------


class TestComputeAdaptiveFileSize:
    """Test adaptive file size computation for small, mid, and large tables."""

    @staticmethod
    def test_small_table_caps_file_size() -> None:
        """Row count below threshold caps file size at 32 MB."""
        result = compute_adaptive_file_size(100, 64 * 1024 * 1024)
        assert result == 32 * 1024 * 1024

    @staticmethod
    def test_small_table_keeps_smaller_base() -> None:
        """Small base target below 32 MB cap is preserved."""
        result = compute_adaptive_file_size(100, 16 * 1024 * 1024)
        assert result == 16 * 1024 * 1024

    @staticmethod
    def test_large_table_floors_file_size() -> None:
        """Row count above threshold raises file size to 128 MB minimum."""
        result = compute_adaptive_file_size(2_000_000, 64 * 1024 * 1024)
        assert result == 128 * 1024 * 1024

    @staticmethod
    def test_large_table_keeps_larger_base() -> None:
        """Large base target above 128 MB floor is preserved."""
        result = compute_adaptive_file_size(2_000_000, 256 * 1024 * 1024)
        assert result == 256 * 1024 * 1024

    @staticmethod
    def test_mid_range_unchanged() -> None:
        """Row count in the mid range returns the base target unchanged."""
        base = 64 * 1024 * 1024
        result = compute_adaptive_file_size(500_000, base)
        assert result == base


# ---------------------------------------------------------------------------
# _adaptive_file_size_from_bundle helper
# ---------------------------------------------------------------------------


class TestAdaptiveFileSizeFromBundle:
    """Test the plan bundle adaptive file size extraction helper."""

    @staticmethod
    def test_returns_decision_for_small_table(
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Return a decision when row count triggers small-table cap."""

        def _fake(_bundle: object, **_kw: object) -> PlanSignals:
            return PlanSignals(stats=NormalizedPlanStats(num_rows=100))

        monkeypatch.setattr(write_planning_module, "extract_plan_signals", _fake)
        bundle = cast("DataFusionPlanArtifact", object())
        target, decision = adaptive_file_size_from_bundle(bundle, 64 * 1024 * 1024)

        assert target == 32 * 1024 * 1024
        assert decision is not None
        assert decision.reason == "small_table"
        assert decision.estimated_rows == SMALL_TABLE_ROWS

    @staticmethod
    def test_returns_decision_for_large_table(
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Return a decision when row count triggers large-table floor."""

        def _fake(_bundle: object, **_kw: object) -> PlanSignals:
            return PlanSignals(stats=NormalizedPlanStats(num_rows=2_000_000))

        monkeypatch.setattr(write_planning_module, "extract_plan_signals", _fake)
        bundle = cast("DataFusionPlanArtifact", object())
        target, decision = adaptive_file_size_from_bundle(bundle, 64 * 1024 * 1024)

        assert target == 128 * 1024 * 1024
        assert decision is not None
        assert decision.reason == "large_table"
        assert decision.estimated_rows == LARGE_TABLE_ROWS

    @staticmethod
    def test_returns_none_decision_when_no_change(
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Return None decision when row count does not alter file size."""

        def _fake(_bundle: object, **_kw: object) -> PlanSignals:
            return PlanSignals(stats=NormalizedPlanStats(num_rows=500_000))

        monkeypatch.setattr(write_planning_module, "extract_plan_signals", _fake)
        bundle = cast("DataFusionPlanArtifact", object())
        base = 64 * 1024 * 1024
        target, decision = adaptive_file_size_from_bundle(bundle, base)

        assert target == base
        assert decision is None

    @staticmethod
    def test_returns_none_decision_when_stats_unavailable(
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Return None decision when plan statistics are unavailable."""

        def _fake(_bundle: object, **_kw: object) -> PlanSignals:
            return PlanSignals(stats=None)

        monkeypatch.setattr(write_planning_module, "extract_plan_signals", _fake)
        bundle = cast("DataFusionPlanArtifact", object())
        base = 64 * 1024 * 1024
        target, decision = adaptive_file_size_from_bundle(bundle, base)

        assert target == base
        assert decision is None


# ---------------------------------------------------------------------------
# _delta_policy_context integration with plan_bundle
# ---------------------------------------------------------------------------


def test_delta_policy_context_uses_plan_signals_for_adaptive_target_file_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Small row-count plan signals should downsize target file size."""

    def _fake_extract_plan_signals(*_args: object, **_kwargs: object) -> PlanSignals:
        return PlanSignals(stats=NormalizedPlanStats(num_rows=1))

    monkeypatch.setattr(write_planning_module, "extract_plan_signals", _fake_extract_plan_signals)
    delta_policy_context = write_module.__dict__["_delta_policy_context"]

    policy_context = delta_policy_context(
        options={"target_file_size": 64 * 1024 * 1024},
        dataset_location=None,
        request_partition_by=None,
        plan_bundle=cast("DataFusionPlanArtifact", object()),
    )

    assert policy_context.target_file_size == 32 * 1024 * 1024


def test_delta_policy_context_keeps_target_file_size_when_stats_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing plan-signal stats should not mutate target file size."""

    def _fake_extract_plan_signals(*_args: object, **_kwargs: object) -> PlanSignals:
        return PlanSignals(stats=None)

    monkeypatch.setattr(write_planning_module, "extract_plan_signals", _fake_extract_plan_signals)
    delta_policy_context = write_module.__dict__["_delta_policy_context"]

    policy_context = delta_policy_context(
        options={"target_file_size": 64 * 1024 * 1024},
        dataset_location=None,
        request_partition_by=None,
        plan_bundle=cast("DataFusionPlanArtifact", object()),
    )

    assert policy_context.target_file_size == 64 * 1024 * 1024


def test_delta_policy_context_carries_adaptive_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Policy context should carry the adaptive decision when file size changes."""

    def _fake_extract_plan_signals(*_args: object, **_kwargs: object) -> PlanSignals:
        return PlanSignals(stats=NormalizedPlanStats(num_rows=50))

    monkeypatch.setattr(write_planning_module, "extract_plan_signals", _fake_extract_plan_signals)
    delta_policy_context = write_module.__dict__["_delta_policy_context"]

    policy_context = delta_policy_context(
        options={"target_file_size": 64 * 1024 * 1024},
        dataset_location=None,
        request_partition_by=None,
        plan_bundle=cast("DataFusionPlanArtifact", object()),
    )

    decision = policy_context.adaptive_file_size_decision
    assert decision is not None
    assert isinstance(decision, _AdaptiveFileSizeDecision)
    assert decision.base_target_file_size == 64 * 1024 * 1024
    assert decision.adaptive_target_file_size == 32 * 1024 * 1024
    assert decision.estimated_rows == SMALL_TABLE_CONTEXT_ROWS
    assert decision.reason == "small_table"


def test_delta_policy_context_no_adaptive_decision_without_bundle() -> None:
    """Policy context should have no adaptive decision when plan_bundle is None."""
    delta_policy_context = write_module.__dict__["_delta_policy_context"]
    policy_context = delta_policy_context(
        options={"target_file_size": 64 * 1024 * 1024},
        dataset_location=None,
        request_partition_by=None,
    )

    assert policy_context.adaptive_file_size_decision is None


def test_delta_policy_context_no_adaptive_decision_mid_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Policy context has no adaptive decision when row count is in mid range."""

    def _fake_extract_plan_signals(*_args: object, **_kwargs: object) -> PlanSignals:
        return PlanSignals(stats=NormalizedPlanStats(num_rows=500_000))

    monkeypatch.setattr(write_planning_module, "extract_plan_signals", _fake_extract_plan_signals)
    delta_policy_context = write_module.__dict__["_delta_policy_context"]

    policy_context = delta_policy_context(
        options={"target_file_size": 64 * 1024 * 1024},
        dataset_location=None,
        request_partition_by=None,
        plan_bundle=cast("DataFusionPlanArtifact", object()),
    )

    assert policy_context.adaptive_file_size_decision is None
    assert policy_context.target_file_size == 64 * 1024 * 1024


# ---------------------------------------------------------------------------
# ADAPTIVE_WRITE_POLICY_SPEC artifact
# ---------------------------------------------------------------------------


def test_adaptive_write_policy_spec_registered() -> None:
    """Verify that the ADAPTIVE_WRITE_POLICY_SPEC is in the artifact registry."""
    from serde_artifact_specs import ADAPTIVE_WRITE_POLICY_SPEC
    from serde_schema_registry import artifact_spec_registry

    registry = artifact_spec_registry()
    assert ADAPTIVE_WRITE_POLICY_SPEC.canonical_name in registry
    assert registry.get(ADAPTIVE_WRITE_POLICY_SPEC.canonical_name) is ADAPTIVE_WRITE_POLICY_SPEC
    assert ADAPTIVE_WRITE_POLICY_SPEC.canonical_name == "adaptive_write_policy_v1"
