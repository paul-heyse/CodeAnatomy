"""Tests for adaptive write policy sizing driven by plan signals."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.io import write as write_module
from datafusion_engine.io.write import (
    _adaptive_file_size_from_bundle,
    _AdaptiveFileSizeDecision,
    compute_adaptive_file_size,
)
from datafusion_engine.plan.signals import NormalizedPlanStats, PlanSignals

if TYPE_CHECKING:
    from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
else:
    DataFusionPlanArtifact = object


# ---------------------------------------------------------------------------
# compute_adaptive_file_size unit tests
# ---------------------------------------------------------------------------


class TestComputeAdaptiveFileSize:
    """Test adaptive file size computation for small, mid, and large tables."""

    def test_small_table_caps_file_size(self) -> None:
        """Row count below threshold caps file size at 32 MB."""
        result = compute_adaptive_file_size(100, 64 * 1024 * 1024)
        assert result == 32 * 1024 * 1024

    def test_small_table_keeps_smaller_base(self) -> None:
        """Small base target below 32 MB cap is preserved."""
        result = compute_adaptive_file_size(100, 16 * 1024 * 1024)
        assert result == 16 * 1024 * 1024

    def test_large_table_floors_file_size(self) -> None:
        """Row count above threshold raises file size to 128 MB minimum."""
        result = compute_adaptive_file_size(2_000_000, 64 * 1024 * 1024)
        assert result == 128 * 1024 * 1024

    def test_large_table_keeps_larger_base(self) -> None:
        """Large base target above 128 MB floor is preserved."""
        result = compute_adaptive_file_size(2_000_000, 256 * 1024 * 1024)
        assert result == 256 * 1024 * 1024

    def test_mid_range_unchanged(self) -> None:
        """Row count in the mid range returns the base target unchanged."""
        base = 64 * 1024 * 1024
        result = compute_adaptive_file_size(500_000, base)
        assert result == base


# ---------------------------------------------------------------------------
# _adaptive_file_size_from_bundle helper
# ---------------------------------------------------------------------------


class TestAdaptiveFileSizeFromBundle:
    """Test the plan bundle adaptive file size extraction helper."""

    def test_returns_decision_for_small_table(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Return a decision when row count triggers small-table cap."""

        def _fake(_bundle: object, **_kw: object) -> PlanSignals:
            return PlanSignals(stats=NormalizedPlanStats(num_rows=100))

        monkeypatch.setattr(write_module, "extract_plan_signals", _fake)
        bundle = cast("DataFusionPlanArtifact", object())
        target, decision = _adaptive_file_size_from_bundle(bundle, 64 * 1024 * 1024)

        assert target == 32 * 1024 * 1024
        assert decision is not None
        assert decision.reason == "small_table"
        assert decision.estimated_rows == 100

    def test_returns_decision_for_large_table(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Return a decision when row count triggers large-table floor."""

        def _fake(_bundle: object, **_kw: object) -> PlanSignals:
            return PlanSignals(stats=NormalizedPlanStats(num_rows=2_000_000))

        monkeypatch.setattr(write_module, "extract_plan_signals", _fake)
        bundle = cast("DataFusionPlanArtifact", object())
        target, decision = _adaptive_file_size_from_bundle(bundle, 64 * 1024 * 1024)

        assert target == 128 * 1024 * 1024
        assert decision is not None
        assert decision.reason == "large_table"
        assert decision.estimated_rows == 2_000_000

    def test_returns_none_decision_when_no_change(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Return None decision when row count does not alter file size."""

        def _fake(_bundle: object, **_kw: object) -> PlanSignals:
            return PlanSignals(stats=NormalizedPlanStats(num_rows=500_000))

        monkeypatch.setattr(write_module, "extract_plan_signals", _fake)
        bundle = cast("DataFusionPlanArtifact", object())
        base = 64 * 1024 * 1024
        target, decision = _adaptive_file_size_from_bundle(bundle, base)

        assert target == base
        assert decision is None

    def test_returns_none_decision_when_stats_unavailable(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Return None decision when plan statistics are unavailable."""

        def _fake(_bundle: object, **_kw: object) -> PlanSignals:
            return PlanSignals(stats=None)

        monkeypatch.setattr(write_module, "extract_plan_signals", _fake)
        bundle = cast("DataFusionPlanArtifact", object())
        base = 64 * 1024 * 1024
        target, decision = _adaptive_file_size_from_bundle(bundle, base)

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

    monkeypatch.setattr(write_module, "extract_plan_signals", _fake_extract_plan_signals)

    policy_context = write_module._delta_policy_context(  # noqa: SLF001
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

    monkeypatch.setattr(write_module, "extract_plan_signals", _fake_extract_plan_signals)

    policy_context = write_module._delta_policy_context(  # noqa: SLF001
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

    monkeypatch.setattr(write_module, "extract_plan_signals", _fake_extract_plan_signals)

    policy_context = write_module._delta_policy_context(  # noqa: SLF001
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
    assert decision.estimated_rows == 50
    assert decision.reason == "small_table"


def test_delta_policy_context_no_adaptive_decision_without_bundle() -> None:
    """Policy context should have no adaptive decision when plan_bundle is None."""
    policy_context = write_module._delta_policy_context(  # noqa: SLF001
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

    monkeypatch.setattr(write_module, "extract_plan_signals", _fake_extract_plan_signals)

    policy_context = write_module._delta_policy_context(  # noqa: SLF001
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
