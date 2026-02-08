"""Tests for compiled-policy counterfactual replay."""

from __future__ import annotations

from relspec.compiled_policy import CompiledExecutionPolicy
from relspec.counterfactual_replay import (
    CounterfactualScenario,
    replay_compiled_policy_counterfactuals,
)


def test_replay_applies_cache_overrides_and_emits_fingerprint() -> None:
    baseline = CompiledExecutionPolicy(
        cache_policy_by_view={
            "a": "delta_staging",
            "b": "delta_output",
        },
        scan_policy_overrides={"ds": {"policy": "full"}},
        workload_class="batch_ingest",
    )
    results = replay_compiled_policy_counterfactuals(
        baseline,
        scenarios=(
            CounterfactualScenario(
                name="no_staging",
                cache_policy_overrides={"a": "none"},
            ),
        ),
    )
    assert len(results) == 1
    result = results[0]
    assert result.scenario_name == "no_staging"
    assert result.changed_cache_views == 1
    assert result.changed_scan_overrides == 0
    assert result.policy_fingerprint is not None


def test_replay_ignores_invalid_cache_override() -> None:
    baseline = CompiledExecutionPolicy(cache_policy_by_view={"a": "delta_staging"})
    results = replay_compiled_policy_counterfactuals(
        baseline,
        scenarios=(
            CounterfactualScenario(
                name="invalid",
                cache_policy_overrides={"a": "memory"},
            ),
        ),
    )
    result = results[0]
    assert result.changed_cache_views == 0
    assert "ignored_invalid_cache_policy:a" in result.notes


def test_replay_estimates_cost_delta_by_workload_class() -> None:
    baseline = CompiledExecutionPolicy(
        cache_policy_by_view={"a": "delta_staging"},
        workload_class="batch_ingest",
    )
    results = replay_compiled_policy_counterfactuals(
        baseline,
        scenarios=(
            CounterfactualScenario(
                name="interactive",
                workload_class="interactive_query",
                cache_policy_overrides={"a": "none"},
            ),
        ),
        task_costs={"a": 200.0},
    )
    result = results[0]
    assert result.estimated_cost_delta is not None
    assert result.estimated_cost_delta < 0.0
