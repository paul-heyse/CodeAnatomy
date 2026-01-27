"""Hamilton lifecycle hooks for pipeline diagnostics."""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from hamilton.lifecycle import api as lifecycle_api

from obs.diagnostics import DiagnosticsCollector

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from relspec.execution_plan import ExecutionPlan

_DIAGNOSTICS_STATE: dict[str, DiagnosticsCollector | None] = {"collector": None}


def set_hamilton_diagnostics_collector(collector: DiagnosticsCollector | None) -> None:
    """Set the global diagnostics collector for Hamilton hooks."""
    _DIAGNOSTICS_STATE["collector"] = collector


def get_hamilton_diagnostics_collector() -> DiagnosticsCollector | None:
    """Return the configured diagnostics collector for Hamilton hooks.

    Returns
    -------
    DiagnosticsCollector | None
        Configured diagnostics collector when available.
    """
    return _DIAGNOSTICS_STATE["collector"]


@dataclass
class DiagnosticsNodeHook(lifecycle_api.NodeExecutionHook):
    """Record Hamilton node execution events in a DiagnosticsCollector."""

    collector: DiagnosticsCollector
    _starts: dict[tuple[str, str, str | None], float] = field(default_factory=dict)

    def run_before_node_execution(
        self,
        *,
        node_name: str,
        node_tags: Mapping[str, Any],
        run_id: str,
        task_id: str | None,
        **kwargs: Any,
    ) -> None:
        """Record diagnostics for a node starting execution."""
        key = (run_id, node_name, task_id)
        self._starts[key] = time.monotonic()
        node_input_types = kwargs.get("node_input_types", {})
        node_return_type = kwargs.get("node_return_type", object)
        input_types_payload = {key: str(value) for key, value in node_input_types.items()}
        self.collector.record_events(
            "hamilton_node_start_v1",
            [
                {
                    "run_id": run_id,
                    "task_id": task_id,
                    "node_name": node_name,
                    "node_tags": dict(node_tags),
                    "node_input_types": input_types_payload,
                    "node_return_type": str(node_return_type),
                }
            ],
        )

    def run_after_node_execution(
        self,
        *,
        node_name: str,
        node_tags: Mapping[str, Any],
        run_id: str,
        task_id: str | None,
        success: bool,
        **kwargs: Any,
    ) -> None:
        """Record diagnostics for a node after execution."""
        key = (run_id, node_name, task_id)
        start = self._starts.pop(key, None)
        duration_ms = None if start is None else int((time.monotonic() - start) * 1000)
        error = kwargs.get("error")
        payload = {
            "run_id": run_id,
            "task_id": task_id,
            "node_name": node_name,
            "node_tags": dict(node_tags),
            "success": bool(success),
            "duration_ms": duration_ms,
            "error": str(error) if error is not None else None,
        }
        self.collector.record_events("hamilton_node_finish_v1", [payload])


def _plan_diagnostics_payload(
    plan: ExecutionPlan,
    *,
    run_id: str,
) -> dict[str, object]:
    diagnostics = plan.diagnostics
    from relspec.rustworkx_graph import task_graph_node_label

    def _labels(node_ids: tuple[int, ...]) -> tuple[str, ...]:
        labels = [task_graph_node_label(plan.task_graph, node_id) for node_id in node_ids]
        return tuple(labels)

    critical_path = _labels(diagnostics.critical_path or ())
    critical_path_task_names = tuple(plan.critical_path_task_names)
    weak_components = tuple(
        tuple(sorted(task_graph_node_label(plan.task_graph, node_id) for node_id in component))
        for component in diagnostics.weak_components
    )
    reduction_node_map = {
        str(key): value
        for key, value in sorted(
            plan.reduction_node_map.items(),
            key=lambda item: item[0],
        )
    }
    reduced_edge_delta = plan.reduction_edge_count - plan.reduction_removed_edge_count
    reduced_edge_count = max(reduced_edge_delta, 0)
    schedule_metadata = [
        {
            "task_name": name,
            "schedule_index": meta.schedule_index,
            "generation_index": meta.generation_index,
            "generation_order": meta.generation_order,
            "generation_size": meta.generation_size,
        }
        for name, meta in sorted(plan.schedule_metadata.items())
    ]
    plan_fingerprints = {
        name: plan.plan_fingerprints[name] for name in sorted(plan.plan_fingerprints)
    }
    bottom_level_costs = {
        name: plan.bottom_level_costs[name] for name in sorted(plan.bottom_level_costs)
    }
    return {
        "run_id": run_id,
        "plan_signature": plan.plan_signature,
        "task_dependency_signature": plan.task_dependency_signature,
        "reduced_task_dependency_signature": plan.reduced_task_dependency_signature,
        "task_count": len(plan.task_schedule.ordered_tasks),
        "generation_count": len(plan.task_schedule.generations),
        "missing_tasks": list(plan.task_schedule.missing_tasks),
        "schedule_metadata": schedule_metadata,
        "critical_path": list(critical_path),
        "critical_path_length": diagnostics.critical_path_length,
        "critical_path_task_names": list(critical_path_task_names),
        "critical_path_length_weighted": plan.critical_path_length_weighted,
        "weak_components": [list(component) for component in weak_components],
        "isolates": list(diagnostics.isolate_labels),
        "reduction_node_map": reduction_node_map,
        "reduction_edge_count": plan.reduction_edge_count,
        "reduction_removed_edge_count": plan.reduction_removed_edge_count,
        "reduction_reduced_edge_count": reduced_edge_count,
        "dot": diagnostics.dot,
        "node_link_json": diagnostics.node_link_json,
        "active_tasks": sorted(plan.active_tasks),
        "plan_fingerprints": plan_fingerprints,
        "bottom_level_costs": bottom_level_costs,
    }


@dataclass
class PlanDiagnosticsHook(lifecycle_api.GraphExecutionHook):
    """Record plan diagnostics at graph execution time."""

    plan: ExecutionPlan
    profile: DataFusionRuntimeProfile
    collector: DiagnosticsCollector | None = None

    def run_before_graph_execution(
        self,
        *,
        run_id: str,
        **kwargs: object,
    ) -> None:
        """Record plan diagnostics before graph execution."""
        _ = kwargs
        from datafusion_engine.diagnostics import record_artifact

        record_artifact(
            self.profile,
            "task_graph_diagnostics_v2",
            _plan_diagnostics_payload(self.plan, run_id=run_id),
        )

    def run_after_graph_execution(
        self,
        *,
        run_id: str,
        **kwargs: object,
    ) -> None:
        """Record scheduling diagnostics after graph execution."""
        _ = kwargs
        _flush_plan_events(
            self.plan,
            profile=self.profile,
            collector=self.collector,
            run_id=run_id,
        )


def _flush_plan_events(
    plan: ExecutionPlan,
    *,
    profile: DataFusionRuntimeProfile,
    collector: DiagnosticsCollector | None,
    run_id: str,
) -> None:
    if collector is None:
        return
    from datafusion_engine.diagnostics import record_artifact, record_events

    plan_event_names = (
        "hamilton_task_submission_v1",
        "hamilton_task_grouping_v1",
        "hamilton_task_expansion_v1",
    )
    events_snapshot = collector.events_snapshot()
    event_counts: dict[str, int] = {}
    for name in plan_event_names:
        rows = events_snapshot.get(name, [])
        event_counts[name] = len(rows)
        if not rows:
            continue
        normalized_rows = [{str(key): value for key, value in row.items()} for row in rows]
        record_events(profile, name, normalized_rows)
    record_artifact(
        profile,
        "hamilton_plan_drift_v1",
        _plan_drift_payload(
            plan,
            events_snapshot=events_snapshot,
            run_id=run_id,
        ),
    )
    record_artifact(
        profile,
        "hamilton_plan_events_v1",
        {
            "run_id": run_id,
            "plan_signature": plan.plan_signature,
            "reduced_task_dependency_signature": (plan.reduced_task_dependency_signature),
            "event_counts": event_counts,
        },
    )


def _plan_drift_payload(
    plan: ExecutionPlan,
    *,
    events_snapshot: Mapping[str, list[Mapping[str, object]]],
    run_id: str,
) -> dict[str, object]:
    submission_rows = events_snapshot.get("hamilton_task_submission_v1", [])
    grouping_rows = events_snapshot.get("hamilton_task_grouping_v1", [])
    expansion_rows = events_snapshot.get("hamilton_task_expansion_v1", [])
    plan_active_tasks = set(plan.active_tasks)
    admitted_tasks = _admitted_task_names(submission_rows)
    missing_tasks = tuple(sorted(plan_active_tasks - admitted_tasks))
    unexpected_tasks = tuple(sorted(admitted_tasks - plan_active_tasks))
    plan_generations = _plan_generation_indices(plan)
    admitted_generations = _admitted_generation_indices(submission_rows)
    missing_generations = tuple(sorted(plan_generations - admitted_generations))
    plan_task_count = len(plan_active_tasks)
    admitted_task_count = len(admitted_tasks)
    coverage_ratio = (
        float(admitted_task_count) / float(plan_task_count) if plan_task_count > 0 else 1.0
    )
    return {
        "run_id": run_id,
        "plan_signature": plan.plan_signature,
        "reduced_plan_signature": (plan.reduced_task_dependency_signature),
        "plan_task_count": plan_task_count,
        "admitted_task_count": admitted_task_count,
        "admitted_task_coverage_ratio": coverage_ratio,
        "missing_admitted_tasks": list(missing_tasks),
        "unexpected_admitted_tasks": list(unexpected_tasks),
        "plan_generation_count": len(plan.task_schedule.generations),
        "plan_generations": sorted(plan_generations),
        "admitted_generation_count": len(admitted_generations),
        "admitted_generations": sorted(admitted_generations),
        "missing_generations": list(missing_generations),
        "submission_event_count": len(submission_rows),
        "grouping_event_count": len(grouping_rows),
        "expansion_event_count": len(expansion_rows),
    }


def _plan_generation_indices(plan: ExecutionPlan) -> set[int]:
    return {meta.generation_index for meta in plan.schedule_metadata.values()}


def _admitted_task_names(rows: list[Mapping[str, object]]) -> set[str]:
    admitted: set[str] = set()
    for row in rows:
        value = row.get("admitted_tasks")
        if isinstance(value, list):
            admitted.update(name for name in value if isinstance(name, str))
    return admitted


def _admitted_generation_indices(rows: list[Mapping[str, object]]) -> set[int]:
    generations: set[int] = set()
    for row in rows:
        value = row.get("task_facts")
        if not isinstance(value, list):
            continue
        for item in value:
            if not isinstance(item, Mapping):
                continue
            generation_value = item.get("generation_index")
            if isinstance(generation_value, int) and not isinstance(generation_value, bool):
                generations.add(generation_value)
                continue
            if isinstance(generation_value, str) and generation_value.isdigit():
                generations.add(int(generation_value))
    return generations


__all__ = [
    "DiagnosticsNodeHook",
    "PlanDiagnosticsHook",
    "get_hamilton_diagnostics_collector",
    "set_hamilton_diagnostics_collector",
]
