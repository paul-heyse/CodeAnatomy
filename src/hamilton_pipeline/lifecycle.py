"""Hamilton lifecycle hooks for pipeline diagnostics."""

from __future__ import annotations

import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, cast

from hamilton.lifecycle import api as lifecycle_api

from obs.diagnostics import DiagnosticsCollector
from obs.otel.heartbeat import set_active_task_count, set_total_task_count
from serde_artifact_specs import (
    HAMILTON_EVENTS_STORE_FAILED_SPEC,
    HAMILTON_PLAN_DRIFT_SPEC,
    HAMILTON_PLAN_EVENTS_SPEC,
    PLAN_EXPECTED_TASKS_SPEC,
    PLAN_SCHEDULE_SPEC,
    PLAN_VALIDATION_SPEC,
    POLICY_CALIBRATION_RESULT_SPEC,
    PRUNING_METRICS_SPEC,
)

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from hamilton_pipeline.plan_artifacts import PlanArtifactBundle
    from relspec.execution_plan import ExecutionPlan
    from relspec.policy_calibrator import ExecutionMetricsSummary

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
            PLAN_EXPECTED_TASKS_SPEC,
            {
                "run_id": run_id,
                "plan_signature": self.plan.plan_signature,
                "task_count": len(self.plan.active_tasks),
                "tasks": sorted(self.plan.active_tasks),
            },
        )
        record_artifact(
            self.profile,
            PLAN_SCHEDULE_SPEC,
            _artifact_payload(to_builtins(bundle.schedule_envelope, str_keys=True)),
        )
        record_artifact(
            self.profile,
            PLAN_VALIDATION_SPEC,
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
        _record_pruning_metrics_artifact(
            plan=self.plan,
            profile=self.profile,
        )
        _record_policy_calibration_artifact(
            plan=self.plan,
            profile=self.profile,
            collector=self.collector,
        )
        _flush_plan_events(
            self.plan,
            profile=self.profile,
            collector=self.collector,
            run_id=run_id,
            plan_artifact_bundle=self.plan_artifact_bundle,
        )


def _record_pruning_metrics_artifact(
    *,
    plan: ExecutionPlan,
    profile: DataFusionRuntimeProfile,
) -> None:
    from datafusion_engine.lineage.diagnostics import record_artifact
    from serde_artifacts import PruningMetricsArtifact
    from serde_msgspec import to_builtins_mapping

    if not plan.scan_units:
        return
    total_files = sum(max(int(unit.total_files), 0) for unit in plan.scan_units)
    pruned_files = sum(max(int(unit.pruned_file_count), 0) for unit in plan.scan_units)
    candidate_files = sum(max(int(unit.candidate_file_count), 0) for unit in plan.scan_units)
    filters_pushed = sum(len(unit.pushed_filters) for unit in plan.scan_units)
    pruning_effectiveness = (float(pruned_files) / float(total_files)) if total_files > 0 else 0.0

    payload = PruningMetricsArtifact(
        view_name="__plan__",
        row_groups_total=total_files,
        row_groups_pruned=pruned_files,
        pages_total=total_files,
        pages_pruned=max(total_files - candidate_files, 0),
        filters_pushed=filters_pushed,
        statistics_available=total_files > 0,
        pruning_effectiveness=pruning_effectiveness,
    )
    record_artifact(
        profile,
        PRUNING_METRICS_SPEC,
        to_builtins_mapping(payload, str_keys=True),
    )


def _record_policy_calibration_artifact(
    *,
    plan: ExecutionPlan,
    profile: DataFusionRuntimeProfile,
    collector: DiagnosticsCollector | None,
) -> None:
    from datafusion_engine.lineage.diagnostics import record_artifact
    from relspec.calibration_bounds import DEFAULT_CALIBRATION_BOUNDS
    from relspec.policy_calibrator import (
        CalibrationThresholds,
        calibrate_from_execution_metrics,
    )
    from serde_msgspec import to_builtins_mapping

    metrics = _execution_metrics_summary(
        plan=plan,
        collector=collector,
    )
    if metrics is None:
        return
    result = calibrate_from_execution_metrics(
        metrics=metrics,
        current_thresholds=CalibrationThresholds(),
        bounds=DEFAULT_CALIBRATION_BOUNDS,
        mode=_policy_calibration_mode(),
    )
    record_artifact(
        profile,
        POLICY_CALIBRATION_RESULT_SPEC,
        to_builtins_mapping(result, str_keys=True),
    )


def _policy_calibration_mode() -> Literal["off", "warn", "enforce", "observe", "apply"]:
    raw = os.environ.get("CODEANATOMY_POLICY_CALIBRATION_MODE", "warn").strip().lower()
    if raw in {"off", "warn", "enforce"}:
        return cast('Literal["off", "warn", "enforce"]', raw)
    if raw == "observe":
        return "warn"
    if raw == "apply":
        return "enforce"
    return "warn"


def _execution_metrics_summary(
    *,
    plan: ExecutionPlan,
    collector: DiagnosticsCollector | None,
) -> ExecutionMetricsSummary | None:
    from relspec.policy_calibrator import ExecutionMetricsSummary

    finish_rows = (
        () if collector is None else collector.events_snapshot().get("hamilton_node_finish_v1", ())
    )
    duration_samples: list[float] = [
        float(duration_ms)
        for row in finish_rows
        if row.get("success", True)
        and isinstance((duration_ms := row.get("duration_ms")), (int, float))
    ]
    if not duration_samples:
        duration_samples = [
            float(metric.duration_ms)
            for metric in plan.task_plan_metrics.values()
            if metric.duration_ms is not None and metric.duration_ms >= 0.0
        ]

    row_count_samples = [
        float(metric.output_rows)
        for metric in plan.task_plan_metrics.values()
        if metric.output_rows is not None and metric.output_rows >= 0
    ]
    predicted_cost = sum(float(cost) for cost in plan.task_costs.values())
    if predicted_cost <= 0.0:
        predicted_cost = float(len(plan.task_costs) or 1)
    actual_cost = sum(duration_samples) if duration_samples else predicted_cost
    observation_count = len(duration_samples) if duration_samples else len(plan.task_costs)
    if observation_count <= 0:
        return None
    mean_duration = (
        (sum(duration_samples) / float(len(duration_samples))) if duration_samples else None
    )
    mean_rows = (
        (sum(row_count_samples) / float(len(row_count_samples))) if row_count_samples else None
    )
    return ExecutionMetricsSummary(
        predicted_cost=predicted_cost,
        actual_cost=actual_cost,
        observation_count=observation_count,
        mean_duration_ms=mean_duration,
        mean_row_count=mean_rows,
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
        HAMILTON_PLAN_DRIFT_SPEC,
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
            HAMILTON_EVENTS_STORE_FAILED_SPEC,
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
        HAMILTON_PLAN_EVENTS_SPEC,
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
