"""Counterfactual replay for compiled execution policies.

Evaluate alternative policy scenarios against a baseline compiled policy
without mutating runtime state.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import msgspec

from relspec.compiled_policy import CompiledExecutionPolicy
from relspec.policy_compiler import _compute_policy_fingerprint
from serde_msgspec import StructBaseStrict


class CounterfactualScenario(StructBaseStrict, frozen=True):
    """Single counterfactual policy scenario."""

    name: str
    workload_class: str | None = None
    cache_policy_overrides: Mapping[str, str] = msgspec.field(default_factory=dict)
    scan_policy_overrides: Mapping[str, object] = msgspec.field(default_factory=dict)


class CounterfactualReplayResult(StructBaseStrict, frozen=True):
    """Replay outcome for one scenario."""

    scenario_name: str
    policy_fingerprint: str | None = None
    changed_cache_views: int = 0
    changed_scan_overrides: int = 0
    estimated_cost_delta: float | None = None
    notes: str = ""


def replay_compiled_policy_counterfactuals(
    baseline_policy: CompiledExecutionPolicy,
    *,
    scenarios: Sequence[CounterfactualScenario],
    task_costs: Mapping[str, float] | None = None,
) -> tuple[CounterfactualReplayResult, ...]:
    """Replay counterfactual policy scenarios against a baseline policy."""
    results: list[CounterfactualReplayResult] = []
    baseline_cache = dict(baseline_policy.cache_policy_by_view)
    baseline_scan = dict(baseline_policy.scan_policy_overrides)
    for scenario in scenarios:
        cache_values = dict(baseline_cache)
        scan_values = dict(baseline_scan)
        changed_cache_names: set[str] = set()
        notes: list[str] = []

        for view_name, override in scenario.cache_policy_overrides.items():
            normalized = _normalize_cache_policy_value(override)
            if normalized is None:
                notes.append(f"ignored_invalid_cache_policy:{view_name}")
                continue
            if cache_values.get(view_name) == normalized:
                continue
            cache_values[view_name] = normalized
            changed_cache_names.add(view_name)

        changed_scan_overrides = 0
        for dataset_name, override in scenario.scan_policy_overrides.items():
            if scan_values.get(dataset_name) == override:
                continue
            scan_values[dataset_name] = override
            changed_scan_overrides += 1

        workload_class = scenario.workload_class or baseline_policy.workload_class
        variant = CompiledExecutionPolicy(
            cache_policy_by_view=cache_values,
            scan_policy_overrides=scan_values,
            maintenance_policy_by_dataset=baseline_policy.maintenance_policy_by_dataset,
            udf_requirements_by_view=baseline_policy.udf_requirements_by_view,
            materialization_strategy=baseline_policy.materialization_strategy,
            diagnostics_flags=baseline_policy.diagnostics_flags,
            workload_class=workload_class,
            validation_mode=baseline_policy.validation_mode,
        )
        results.append(
            CounterfactualReplayResult(
                scenario_name=scenario.name,
                policy_fingerprint=_compute_policy_fingerprint(variant),
                changed_cache_views=len(changed_cache_names),
                changed_scan_overrides=changed_scan_overrides,
                estimated_cost_delta=_estimated_cost_delta(
                    changed_cache_names=changed_cache_names,
                    workload_class=workload_class,
                    task_costs=task_costs,
                ),
                notes=";".join(notes),
            )
        )
    return tuple(results)


def _normalize_cache_policy_value(value: object) -> str | None:
    if value in {"none", "delta_staging", "delta_output"}:
        return str(value)
    return None


def _estimated_cost_delta(
    *,
    changed_cache_names: set[str],
    workload_class: str | None,
    task_costs: Mapping[str, float] | None,
) -> float | None:
    if task_costs is None or not changed_cache_names:
        return None
    changed_cost = sum(float(task_costs.get(name, 0.0)) for name in changed_cache_names)
    if changed_cost == 0.0:
        return 0.0
    if workload_class == "compile_replay":
        return -0.10 * changed_cost
    if workload_class == "interactive_query":
        return -0.05 * changed_cost
    if workload_class == "batch_ingest":
        return 0.02 * changed_cost
    return 0.0


__all__ = [
    "CounterfactualReplayResult",
    "CounterfactualScenario",
    "replay_compiled_policy_counterfactuals",
]
