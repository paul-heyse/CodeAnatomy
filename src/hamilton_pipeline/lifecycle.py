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
        self.collector.record_events(
            "hamilton_node_start_v1",
            [
                {
                    "run_id": run_id,
                    "task_id": task_id,
                    "node_name": node_name,
                    "node_tags": dict(node_tags),
                    "node_input_types": {k: str(v) for k, v in node_input_types.items()},
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


def _plan_diagnostics_payload(plan: ExecutionPlan, *, run_id: str) -> dict[str, object]:
    diagnostics = plan.diagnostics
    from relspec.rustworkx_graph import task_graph_node_label

    def _labels(node_ids: tuple[int, ...]) -> tuple[str, ...]:
        labels = [task_graph_node_label(plan.task_graph, node_id) for node_id in node_ids]
        return tuple(labels)

    critical_path = _labels(diagnostics.critical_path or ())
    weak_components = tuple(
        tuple(sorted(task_graph_node_label(plan.task_graph, node_id) for node_id in component))
        for component in diagnostics.weak_components
    )
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
        "task_count": len(plan.task_schedule.ordered_tasks),
        "generation_count": len(plan.task_schedule.generations),
        "missing_tasks": list(plan.task_schedule.missing_tasks),
        "schedule_metadata": schedule_metadata,
        "critical_path": list(critical_path),
        "critical_path_length": diagnostics.critical_path_length,
        "weak_components": [list(component) for component in weak_components],
        "isolates": list(diagnostics.isolate_labels),
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


__all__ = [
    "DiagnosticsNodeHook",
    "PlanDiagnosticsHook",
    "get_hamilton_diagnostics_collector",
    "set_hamilton_diagnostics_collector",
]
