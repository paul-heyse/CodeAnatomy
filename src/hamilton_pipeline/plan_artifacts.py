"""Plan schedule and validation artifact builders."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from relspec.execution_plan import ExecutionPlan
from relspec.graph_edge_validation import (
    EdgeValidationResult,
    GraphValidationSummary,
    TaskValidationResult,
    validate_graph_edges,
)
from serde_artifacts import (
    PlanScheduleArtifact,
    PlanScheduleEnvelope,
    PlanValidationArtifact,
    PlanValidationEnvelope,
    artifact_envelope_id,
)

PLAN_SCHEDULE_ARTIFACT_KEY = "plan_schedule"
PLAN_VALIDATION_ARTIFACT_KEY = "plan_validation"


@dataclass(frozen=True)
class PlanArtifactBundle:
    """Bundle of plan schedule/validation envelopes and IDs."""

    schedule_envelope: PlanScheduleEnvelope
    validation_envelope: PlanValidationEnvelope
    schedule_artifact_id: str
    validation_artifact_id: str

    def artifact_ids(self) -> dict[str, str]:
        """Return a mapping of artifact IDs for run manifests.

        Returns
        -------
        dict[str, str]
            Mapping of artifact keys to deterministic IDs.
        """
        return {
            PLAN_SCHEDULE_ARTIFACT_KEY: self.schedule_artifact_id,
            PLAN_VALIDATION_ARTIFACT_KEY: self.validation_artifact_id,
        }


def build_plan_artifact_bundle(*, plan: ExecutionPlan, run_id: str) -> PlanArtifactBundle:
    """Build schedule/validation artifacts and deterministic IDs.

    Returns
    -------
    PlanArtifactBundle
        Bundle of schedule/validation envelopes with their IDs.
    """
    schedule_payload = _plan_schedule_payload(plan=plan, run_id=run_id)
    validation_payload = _plan_validation_payload(plan=plan, run_id=run_id)
    schedule_envelope = PlanScheduleEnvelope(payload=schedule_payload)
    validation_envelope = PlanValidationEnvelope(payload=validation_payload)
    schedule_id = artifact_envelope_id(schedule_envelope)
    validation_id = artifact_envelope_id(validation_envelope)
    return PlanArtifactBundle(
        schedule_envelope=schedule_envelope,
        validation_envelope=validation_envelope,
        schedule_artifact_id=schedule_id,
        validation_artifact_id=validation_id,
    )


def _plan_schedule_payload(*, plan: ExecutionPlan, run_id: str) -> PlanScheduleArtifact:
    task_costs = {name: float(max(cost, 0.0)) for name, cost in sorted(plan.task_costs.items())}
    bottom_level_costs = {
        name: float(max(cost, 0.0)) for name, cost in sorted(plan.bottom_level_costs.items())
    }
    slack_by_task = {
        name: float(max(slack, 0.0)) for name, slack in sorted(plan.slack_by_task.items())
    }
    centrality_raw = plan.diagnostics.betweenness_centrality or {}
    task_centrality = {
        name: float(max(score, 0.0)) for name, score in sorted(centrality_raw.items())
    }
    dominators = dict(sorted((plan.diagnostics.dominators or {}).items()))
    bridge_edges = tuple(plan.diagnostics.bridge_edges)
    articulation_tasks = tuple(plan.diagnostics.articulation_tasks)
    return PlanScheduleArtifact(
        run_id=run_id,
        plan_signature=plan.plan_signature,
        reduced_plan_signature=plan.reduced_task_dependency_signature,
        task_count=len(plan.task_schedule.ordered_tasks),
        ordered_tasks=tuple(plan.task_schedule.ordered_tasks),
        generations=tuple(tuple(group) for group in plan.task_schedule.generations),
        critical_path_tasks=tuple(plan.critical_path_task_names),
        critical_path_length_weighted=plan.critical_path_length_weighted,
        task_costs=task_costs,
        bottom_level_costs=bottom_level_costs,
        slack_by_task=slack_by_task,
        task_centrality=task_centrality,
        task_dominators=dominators,
        bridge_edges=bridge_edges,
        articulation_tasks=articulation_tasks,
    )


def _plan_validation_payload(*, plan: ExecutionPlan, run_id: str) -> PlanValidationArtifact:
    summary = _validation_summary(plan)
    task_results = tuple(
        _task_validation_payload(result)
        for result in sorted(summary.task_results, key=lambda item: item.task_name)
    )
    return PlanValidationArtifact(
        run_id=run_id,
        plan_signature=plan.plan_signature,
        reduced_plan_signature=plan.reduced_task_dependency_signature,
        total_tasks=summary.total_tasks,
        valid_tasks=summary.valid_tasks,
        invalid_tasks=summary.invalid_tasks,
        total_edges=summary.total_edges,
        valid_edges=summary.valid_edges,
        invalid_edges=summary.invalid_edges,
        task_results=task_results,
    )


def _validation_summary(plan: ExecutionPlan) -> GraphValidationSummary:
    summary = plan.task_schedule.validation_summary
    if summary is not None:
        return summary
    return validate_graph_edges(plan.task_graph, catalog=plan.evidence.clone())


def _task_validation_payload(result: TaskValidationResult) -> dict[str, object]:
    return {
        "task_name": result.task_name,
        "is_valid": result.is_valid,
        "unsatisfied_edges": list(result.unsatisfied_edges),
        "contract_violations": list(result.contract_violations),
        "edge_results": [
            _edge_validation_payload(edge) for edge in _sorted_edge_results(result.edge_results)
        ],
    }


def _edge_validation_payload(result: EdgeValidationResult) -> dict[str, object]:
    return {
        "source_name": result.source_name,
        "target_task": result.target_task,
        "is_valid": result.is_valid,
        "missing_columns": list(result.missing_columns),
        "missing_types": [_column_type_payload(items) for items in result.missing_types],
        "missing_metadata": [_metadata_payload(items) for items in result.missing_metadata],
        "contract_violations": list(result.contract_violations),
        "available_columns": list(result.available_columns),
        "available_types": [_column_type_payload(items) for items in result.available_types],
        "available_metadata": [_metadata_payload(items) for items in result.available_metadata],
    }


def _sorted_edge_results(
    results: Sequence[EdgeValidationResult],
) -> Sequence[EdgeValidationResult]:
    return tuple(sorted(results, key=lambda item: (item.source_name, item.target_task)))


def _column_type_payload(pair: tuple[str, str]) -> dict[str, str]:
    column, dtype = pair
    return {"column": column, "dtype": dtype}


def _metadata_payload(pair: tuple[bytes, bytes]) -> dict[str, str]:
    key, value = pair
    return {"key_hex": key.hex(), "value_hex": value.hex()}


__all__ = [
    "PLAN_SCHEDULE_ARTIFACT_KEY",
    "PLAN_VALIDATION_ARTIFACT_KEY",
    "PlanArtifactBundle",
    "build_plan_artifact_bundle",
]
