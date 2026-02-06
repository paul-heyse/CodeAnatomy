"""Hamilton lifecycle hooks for pipeline diagnostics."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from hamilton.lifecycle import api as lifecycle_api

from obs.diagnostics import DiagnosticsCollector
from obs.otel.heartbeat import set_active_task_count, set_total_task_count

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from hamilton_pipeline.plan_artifacts import PlanArtifactBundle
    from relspec.execution_plan import ExecutionPlan

_DIAGNOSTICS_STATE: dict[str, DiagnosticsCollector | None] = {"collector": None}


def set_hamilton_diagnostics_collector(collector: DiagnosticsCollector | None) -> None:
    """Set the global diagnostics collector for Hamilton hooks."""
    _DIAGNOSTICS_STATE["collector"] = collector


def get_hamilton_diagnostics_collector() -> DiagnosticsCollector | None:
    """Return the configured diagnostics collector for Hamilton hooks.

    Returns:
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


def _artifact_payload(payload: object) -> Mapping[str, object]:
    if isinstance(payload, Mapping):
        return {str(key): value for key, value in payload.items()}
    return {"payload": payload}


@dataclass
class PlanDiagnosticsHook(lifecycle_api.GraphExecutionHook):
    """Record plan diagnostics at graph execution time."""

    plan: ExecutionPlan
    profile: DataFusionRuntimeProfile
    collector: DiagnosticsCollector | None = None
    plan_artifact_bundle: PlanArtifactBundle | None = None

    def run_before_graph_execution(
        self,
        *,
        run_id: str,
        **kwargs: object,
    ) -> None:
        """Record plan diagnostics before graph execution."""
        _ = kwargs
        set_active_task_count(len(self.plan.active_tasks))
        set_total_task_count(len(self.plan.active_tasks))
        from datafusion_engine.lineage.diagnostics import record_artifact
        from hamilton_pipeline.plan_artifacts import build_plan_artifact_bundle
        from serde_msgspec import to_builtins

        bundle = build_plan_artifact_bundle(plan=self.plan, run_id=run_id)
        self.plan_artifact_bundle = bundle
        record_artifact(
            self.profile,
            "plan_expected_tasks_v1",
            {
                "run_id": run_id,
                "plan_signature": self.plan.plan_signature,
                "task_count": len(self.plan.active_tasks),
                "tasks": sorted(self.plan.active_tasks),
            },
        )
        record_artifact(
            self.profile,
            "plan_schedule_v1",
            _artifact_payload(to_builtins(bundle.schedule_envelope, str_keys=True)),
        )
        record_artifact(
            self.profile,
            "plan_validation_v1",
            _artifact_payload(to_builtins(bundle.validation_envelope, str_keys=True)),
        )

    def run_after_graph_execution(
        self,
        *,
        run_id: str,
        **kwargs: object,
    ) -> None:
        """Record scheduling diagnostics after graph execution."""
        _ = kwargs
        set_active_task_count(None)
        set_total_task_count(None)
        _flush_plan_events(
            self.plan,
            profile=self.profile,
            collector=self.collector,
            run_id=run_id,
            plan_artifact_bundle=self.plan_artifact_bundle,
        )


def _flush_plan_events(
    plan: ExecutionPlan,
    *,
    profile: DataFusionRuntimeProfile,
    collector: DiagnosticsCollector | None,
    run_id: str,
    plan_artifact_bundle: PlanArtifactBundle | None,
) -> None:
    if collector is None:
        return
    from datafusion_engine.lineage.diagnostics import record_artifact, record_events

    plan_event_names = (
        "hamilton_task_submission_v1",
        "hamilton_task_grouping_v1",
        "hamilton_task_expansion_v1",
        "hamilton_task_routing_v1",
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
    if plan_artifact_bundle is not None:
        event_counts["plan_schedule_v1"] = 1
        event_counts["plan_validation_v1"] = 1
    plan_drift = _plan_drift_payload(
        plan,
        events_snapshot=events_snapshot,
        run_id=run_id,
    )
    record_artifact(
        profile,
        "hamilton_plan_drift_v1",
        plan_drift,
    )
    events_for_persistence: dict[str, Sequence[object]] = dict(events_snapshot)
    events_for_persistence["hamilton_plan_drift_v1"] = [plan_drift]
    if plan_artifact_bundle is not None:
        events_for_persistence["plan_schedule_v1"] = [plan_artifact_bundle.schedule_envelope]
        events_for_persistence["plan_validation_v1"] = [plan_artifact_bundle.validation_envelope]
        persisted_event_names = (
            *plan_event_names,
            "hamilton_plan_drift_v1",
            "plan_schedule_v1",
            "plan_validation_v1",
        )
    else:
        persisted_event_names = (*plan_event_names, "hamilton_plan_drift_v1")
    try:
        from datafusion_engine.plan.artifact_store import (
            HamiltonEventsRequest,
            persist_hamilton_events,
        )

        session = profile.session_context()
        request = HamiltonEventsRequest(
            run_id=run_id,
            plan_signature=plan.plan_signature,
            reduced_plan_signature=plan.reduced_task_dependency_signature,
            events_snapshot=events_for_persistence,
            event_names=persisted_event_names,
        )
        persist_hamilton_events(
            session,
            profile,
            request=request,
        )
    except (RuntimeError, ValueError, TypeError, OSError, KeyError, ImportError) as exc:
        record_artifact(
            profile,
            "hamilton_events_store_failed_v2",
            {
                "run_id": run_id,
                "plan_signature": plan.plan_signature,
                "reduced_plan_signature": plan.reduced_task_dependency_signature,
                "event_names": list(persisted_event_names),
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
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
