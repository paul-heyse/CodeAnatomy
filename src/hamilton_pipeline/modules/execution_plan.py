"""Hamilton nodes that inject the compiled execution plan."""

from __future__ import annotations

import sys
from collections.abc import Mapping
from dataclasses import dataclass
from types import FunctionType, ModuleType
from typing import TYPE_CHECKING, get_origin, get_type_hints

from hamilton.function_modifiers import cache, extract_fields

from engine.runtime_profile import RuntimeProfileSpec
from hamilton_pipeline.modules import task_execution
from hamilton_pipeline.plan_artifacts import build_plan_artifact_bundle
from hamilton_pipeline.tag_policy import TagPolicy, apply_tag
from relspec.evidence import EvidenceCatalog
from relspec.execution_plan import ExecutionPlan
from relspec.graph_edge_validation import validate_graph_edges

if TYPE_CHECKING:
    from datafusion_engine.lineage.scan import ScanUnit
    from datafusion_engine.plan.bundle import DataFusionPlanBundle
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.views.graph import ViewNode
    from relspec.rustworkx_graph import TaskGraph
    from relspec.rustworkx_schedule import TaskSchedule
    from relspec.schedule_events import TaskScheduleMetadata
    from schema_spec.system import DatasetSpec
    from semantics.incremental.plan_fingerprints import PlanFingerprintSnapshot
else:
    DataFusionPlanBundle = object
    DataFusionRuntimeProfile = object
    ScanUnit = object
    ViewNode = object
    PlanFingerprintSnapshot = object
    TaskGraph = object
    TaskSchedule = object
    TaskScheduleMetadata = object
    DatasetSpec = object


@dataclass(frozen=True)
class PlanModuleOptions:
    """Options controlling plan module construction."""

    module_name: str = "hamilton_pipeline.generated_plan"
    record_evidence_artifacts: bool = True


def build_execution_plan_module(
    plan: ExecutionPlan,
    *,
    plan_module_options: PlanModuleOptions | None = None,
) -> ModuleType:
    """Build a Hamilton module that provides the execution plan contract.

    Returns
    -------
    ModuleType
        Module containing plan-level nodes for Hamilton execution.
    """
    resolved = plan_module_options or PlanModuleOptions()
    module, exports = _create_module(resolved.module_name)
    for name, fn in _plan_node_functions(plan, resolved):
        _register_module_symbol(module, exports, name, fn)
    return module


def _create_module(module_name: str) -> tuple[ModuleType, list[str]]:
    module = ModuleType(module_name)
    sys.modules[module_name] = module
    exports: list[str] = []
    module.__dict__["__all__"] = exports
    return module, exports


def _register_module_symbol(
    module: ModuleType,
    exports: list[str],
    name: str,
    fn: object,
) -> None:
    if callable(fn):
        fn.__module__ = module.__name__
        _normalize_hamilton_annotations(fn)
    module.__dict__[name] = fn
    exports.append(name)


def _normalize_hamilton_annotations(fn: object) -> None:
    if not isinstance(fn, FunctionType):
        return
    annotations = fn.__annotations__
    if not annotations:
        return
    try:
        resolved = get_type_hints(fn, globalns=fn.__globals__, localns=fn.__globals__)
    except (NameError, TypeError, ValueError):
        return
    normalized: dict[str, object] = {}
    for key, value in resolved.items():
        origin = get_origin(value)
        normalized[key] = origin if isinstance(origin, type) else value
    if normalized:
        fn.__annotations__ = normalized


def _plan_node_functions(
    plan: ExecutionPlan,
    options: PlanModuleOptions,
) -> tuple[tuple[str, object], ...]:
    return (
        ("execution_plan", _execution_plan_node(plan)),
        ("view_nodes", _view_nodes_node()),
        ("plan_bundles_by_task", _plan_bundles_by_task_node()),
        ("dataset_specs", _dataset_specs_node()),
        ("plan_scan_units", _plan_scan_units_node()),
        ("plan_scan_keys_by_task", _plan_scan_keys_by_task_node()),
        ("plan_scan_units_by_task_name", _plan_scan_units_by_task_name_node()),
        ("plan_context", _plan_context_node()),
        ("task_graph", _task_graph_node()),
        ("evidence_catalog", _evidence_catalog_node(options)),
        ("plan_artifacts", _plan_artifacts_node()),
        ("plan_artifact_ids", _plan_artifact_ids_node()),
        ("plan_signature_value", _plan_signature_value_node(plan.plan_signature)),
        ("plan_signature", _plan_signature_node()),
        ("task_schedule", _task_schedule_node()),
        ("task_generations", _task_generations_node()),
        ("schedule_metadata", _schedule_metadata_node()),
        ("plan_fingerprints", _plan_fingerprints_node()),
        ("plan_task_signatures", _plan_task_signatures_node()),
        ("plan_snapshots", _plan_snapshots_node()),
        ("bottom_level_costs", _bottom_level_costs_node()),
        ("dependency_map", _dependency_map_node()),
        ("plan_active_task_names", _plan_active_task_names_node()),
        ("active_task_names", _active_task_names_node()),
    )


def _execution_plan_node(plan: ExecutionPlan) -> object:
    @apply_tag(TagPolicy(layer="plan", kind="object", artifact="execution_plan"))
    def execution_plan() -> ExecutionPlan:
        return plan

    return execution_plan


def _view_nodes_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="catalog", artifact="view_nodes"))
    def view_nodes(execution_plan: ExecutionPlan) -> tuple[ViewNode, ...]:
        return execution_plan.view_nodes

    return view_nodes


def _plan_bundles_by_task_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="mapping", artifact="plan_bundles_by_task"))
    def plan_bundles_by_task(
        execution_plan: ExecutionPlan,
    ) -> Mapping[str, DataFusionPlanBundle]:
        bundles: dict[str, DataFusionPlanBundle] = {}
        for node in execution_plan.view_nodes:
            if node.plan_bundle is None:
                continue
            bundles[node.name] = node.plan_bundle
        return bundles

    return plan_bundles_by_task


def _dataset_specs_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="catalog", artifact="dataset_specs"))
    def dataset_specs(execution_plan: ExecutionPlan) -> tuple[DatasetSpec, ...]:
        mapping = execution_plan.dataset_specs
        return tuple(mapping[name] for name in sorted(mapping))

    return dataset_specs


def _plan_scan_units_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="catalog", artifact="plan_scan_units"))
    def plan_scan_units(execution_plan: ExecutionPlan) -> tuple[ScanUnit, ...]:
        return execution_plan.scan_units

    return plan_scan_units


def _plan_scan_keys_by_task_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="mapping", artifact="plan_scan_keys_by_task"))
    def plan_scan_keys_by_task(
        execution_plan: ExecutionPlan,
    ) -> Mapping[str, tuple[str, ...]]:
        return execution_plan.scan_keys_by_task

    return plan_scan_keys_by_task


def _plan_scan_units_by_task_name_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="mapping", artifact="plan_scan_units_by_task_name"))
    def plan_scan_units_by_task_name(
        execution_plan: ExecutionPlan,
    ) -> Mapping[str, ScanUnit]:
        return execution_plan.scan_task_units_by_name

    return plan_scan_units_by_task_name


def _plan_context_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="context", artifact="plan_context"))
    def plan_context(
        plan_signature: str,
        active_task_names: frozenset[str],
        plan_bundles_by_task: Mapping[str, DataFusionPlanBundle],
        plan_scan_inputs: task_execution.PlanScanInputs,
    ) -> task_execution.PlanExecutionContext:
        return task_execution.PlanExecutionContext(
            plan_signature=plan_signature,
            active_task_names=active_task_names,
            plan_bundles_by_task=dict(plan_bundles_by_task),
            plan_scan_inputs=plan_scan_inputs,
        )

    return plan_context


def _task_graph_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="graph", artifact="task_graph"))
    def task_graph(execution_plan: ExecutionPlan) -> TaskGraph:
        return execution_plan.task_graph

    return task_graph


def _evidence_catalog_node(options: PlanModuleOptions) -> object:
    @apply_tag(TagPolicy(layer="plan", kind="catalog", artifact="evidence_catalog"))
    def evidence_catalog(
        execution_plan: ExecutionPlan,
        runtime_profile_spec: RuntimeProfileSpec,
    ) -> EvidenceCatalog:
        evidence = execution_plan.evidence
        profile = runtime_profile_spec.datafusion
        if options.record_evidence_artifacts:
            _record_evidence_contract_violations(profile, evidence)
        return evidence

    return evidence_catalog


def _plan_signature_node() -> object:
    @cache(format="pickle", behavior="recompute")
    @apply_tag(TagPolicy(layer="plan", kind="scalar", artifact="plan_signature"))
    def plan_signature(plan_signature_value: str) -> str:
        return plan_signature_value

    return plan_signature


def _plan_signature_value_node(plan_signature: str) -> object:
    @cache(format="pickle", behavior="recompute")
    @apply_tag(TagPolicy(layer="plan", kind="scalar", artifact="plan_signature_value"))
    def plan_signature_value() -> str:
        return plan_signature

    return plan_signature_value


def _plan_artifacts_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="mapping", artifact="plan_artifacts"))
    @extract_fields(
        {
            "plan_task_dependency_signature": str,
            "plan_reduced_task_dependency_signature": str,
            "plan_task_count": int,
            "plan_generation_count": int,
            "plan_reduction_edge_count": int,
            "plan_reduction_removed_edge_count": int,
            "plan_reduction_reduced_edge_count": int,
            "plan_critical_path_task_names": tuple,
            "plan_critical_path_length_weighted": object,
            "plan_scan_unit_count": int,
            "plan_scan_file_candidate_count": int,
            "plan_task_signature_count": int,
            "plan_session_runtime_hash": object,
            "plan_total_edges": int,
            "plan_valid_edges": int,
            "plan_invalid_edges": int,
            "plan_total_tasks": int,
            "plan_valid_tasks": int,
            "plan_invalid_tasks": int,
        }
    )
    def plan_artifacts(execution_plan: ExecutionPlan) -> dict[str, object]:
        reduced_edge_count = max(
            execution_plan.reduction_edge_count - execution_plan.reduction_removed_edge_count,
            0,
        )
        scan_unit_count = len(execution_plan.scan_units)
        scan_file_candidate_count = sum(
            len(unit.candidate_files) for unit in execution_plan.scan_units
        )
        plan_task_signature_count = len(execution_plan.plan_task_signatures)
        summary = execution_plan.task_schedule.validation_summary
        if summary is None:
            summary = validate_graph_edges(
                execution_plan.task_graph,
                catalog=execution_plan.evidence.clone(),
            )
        return {
            "plan_task_dependency_signature": (execution_plan.task_dependency_signature),
            "plan_reduced_task_dependency_signature": (
                execution_plan.reduced_task_dependency_signature
            ),
            "plan_task_count": len(execution_plan.active_tasks),
            "plan_generation_count": len(execution_plan.task_schedule.generations),
            "plan_reduction_edge_count": execution_plan.reduction_edge_count,
            "plan_reduction_removed_edge_count": (execution_plan.reduction_removed_edge_count),
            "plan_reduction_reduced_edge_count": reduced_edge_count,
            "plan_critical_path_task_names": (execution_plan.critical_path_task_names),
            "plan_critical_path_length_weighted": (execution_plan.critical_path_length_weighted),
            "plan_scan_unit_count": scan_unit_count,
            "plan_scan_file_candidate_count": scan_file_candidate_count,
            "plan_task_signature_count": plan_task_signature_count,
            "plan_session_runtime_hash": execution_plan.session_runtime_hash,
            "plan_total_edges": summary.total_edges,
            "plan_valid_edges": summary.valid_edges,
            "plan_invalid_edges": summary.invalid_edges,
            "plan_total_tasks": summary.total_tasks,
            "plan_valid_tasks": summary.valid_tasks,
            "plan_invalid_tasks": summary.invalid_tasks,
        }

    return plan_artifacts


def _plan_artifact_ids_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="mapping", artifact="plan_artifact_ids"))
    def plan_artifact_ids(
        execution_plan: ExecutionPlan,
        run_id: str,
    ) -> Mapping[str, str]:
        if not run_id:
            return {}
        bundle = build_plan_artifact_bundle(plan=execution_plan, run_id=run_id)
        return bundle.artifact_ids()

    return plan_artifact_ids


def _task_schedule_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="schedule", artifact="task_schedule"))
    def task_schedule(execution_plan: ExecutionPlan) -> TaskSchedule:
        return execution_plan.task_schedule

    return task_schedule


def _task_generations_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="schedule", artifact="task_generations"))
    def task_generations(execution_plan: ExecutionPlan) -> tuple[tuple[str, ...], ...]:
        return execution_plan.task_schedule.generations

    return task_generations


def _schedule_metadata_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="schedule", artifact="schedule_metadata"))
    def schedule_metadata(
        execution_plan: ExecutionPlan,
    ) -> Mapping[str, TaskScheduleMetadata]:
        return execution_plan.schedule_metadata

    return schedule_metadata


def _plan_fingerprints_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="mapping", artifact="plan_fingerprints"))
    def plan_fingerprints(execution_plan: ExecutionPlan) -> Mapping[str, str]:
        return execution_plan.plan_fingerprints

    return plan_fingerprints


def _plan_task_signatures_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="mapping", artifact="plan_task_signatures"))
    def plan_task_signatures(execution_plan: ExecutionPlan) -> Mapping[str, str]:
        return execution_plan.plan_task_signatures

    return plan_task_signatures


def _plan_snapshots_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="mapping", artifact="plan_snapshots"))
    def plan_snapshots(
        execution_plan: ExecutionPlan,
    ) -> Mapping[str, PlanFingerprintSnapshot]:
        return execution_plan.plan_snapshots

    return plan_snapshots


def _bottom_level_costs_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="mapping", artifact="bottom_level_costs"))
    def bottom_level_costs(execution_plan: ExecutionPlan) -> Mapping[str, float]:
        return execution_plan.bottom_level_costs

    return bottom_level_costs


def _dependency_map_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="mapping", artifact="dependency_map"))
    def dependency_map(
        execution_plan: ExecutionPlan,
    ) -> Mapping[str, tuple[str, ...]]:
        return execution_plan.dependency_map

    return dependency_map


def _plan_active_task_names_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="set", artifact="plan_active_task_names"))
    def plan_active_task_names(execution_plan: ExecutionPlan) -> frozenset[str]:
        return execution_plan.active_tasks

    return plan_active_task_names


def _active_task_names_node() -> object:
    @apply_tag(TagPolicy(layer="plan", kind="set", artifact="active_task_names"))
    def active_task_names(execution_plan: ExecutionPlan) -> frozenset[str]:
        return execution_plan.active_tasks

    return active_task_names


def _record_evidence_contract_violations(
    profile: DataFusionRuntimeProfile,
    evidence: EvidenceCatalog,
) -> None:
    if not evidence.contract_violations_by_dataset:
        return
    from datafusion_engine.lineage.diagnostics import record_artifact

    payload = [
        {
            "dataset": name,
            "violations": [str(item) for item in violations],
        }
        for name, violations in evidence.contract_violations_by_dataset.items()
    ]
    record_artifact(
        profile,
        "evidence_contract_violations_v1",
        {"violations": payload},
    )


def _record_udf_parity(profile: DataFusionRuntimeProfile) -> None:
    from datafusion_engine.lineage.diagnostics import record_artifact
    from datafusion_engine.udf.parity import udf_parity_report
    from datafusion_engine.udf.runtime import rust_udf_snapshot

    session = profile.session_context()
    registry_snapshot = rust_udf_snapshot(session)
    report = udf_parity_report(session, snapshot=registry_snapshot)
    record_artifact(profile, "udf_parity_v1", report.payload())


__all__ = ["PlanModuleOptions", "build_execution_plan_module"]
