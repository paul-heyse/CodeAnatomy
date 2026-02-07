"""Tests for adaptive write policy sizing driven by plan signals."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.io import write as write_module
from datafusion_engine.plan.signals import NormalizedPlanStats, PlanSignals

if TYPE_CHECKING:
    from datafusion_engine.plan.bundle import DataFusionPlanBundle
else:
    DataFusionPlanBundle = object


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
        plan_bundle=cast("DataFusionPlanBundle", object()),
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
        plan_bundle=cast("DataFusionPlanBundle", object()),
    )

    assert policy_context.target_file_size == 64 * 1024 * 1024
