"""Hamilton nodes that inject the compiled execution plan."""

from __future__ import annotations

import sys
from collections.abc import Mapping
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING

from hamilton.function_modifiers import extract_fields, tag

from relspec.evidence import EvidenceCatalog
from relspec.execution_plan import (
    ExecutionPlan,
    downstream_task_closure,
    upstream_task_closure,
)

if TYPE_CHECKING:
    from arrowdsl.core.execution_context import ExecutionContext
    from datafusion_engine.plan_bundle import DataFusionPlanBundle
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from datafusion_engine.scan_planner import ScanUnit
    from datafusion_engine.view_graph_registry import ViewNode
    from incremental.plan_fingerprints import PlanFingerprintSnapshot
    from incremental.types import IncrementalConfig
    from relspec.incremental import IncrementalDiff
    from relspec.rustworkx_graph import TaskGraph
    from relspec.rustworkx_schedule import TaskSchedule
    from relspec.schedule_events import TaskScheduleMetadata
    from schema_spec.system import DatasetSpec
else:
    ExecutionContext = object
    DataFusionPlanBundle = object
    DataFusionRuntimeProfile = object
    ViewNode = object
    PlanFingerprintSnapshot = object
    IncrementalConfig = object
    IncrementalDiff = object
    TaskGraph = object
    TaskSchedule = object
    TaskScheduleMetadata = object
    DatasetSpec = object


@dataclass(frozen=True)
class PlanModuleOptions:
    """Options controlling plan module construction."""

    module_name: str = "hamilton_pipeline.generated_plan"
    record_evidence_artifacts: bool = True
    record_incremental_artifacts: bool = True


def build_execution_plan_module(
    plan: ExecutionPlan,
    *,
    options: PlanModuleOptions | None = None,
) -> ModuleType:
    """Build a Hamilton module that provides the execution plan contract.

    Returns
    -------
    ModuleType
        Module containing plan-level nodes for Hamilton execution.
    """
    resolved = options or PlanModuleOptions()
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
    module.__dict__[name] = fn
    exports.append(name)


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
        ("task_graph", _task_graph_node()),
        ("evidence_catalog", _evidence_catalog_node(options)),
        ("plan_artifacts", _plan_artifacts_node()),
        ("plan_signature", _plan_signature_node()),
        ("task_schedule", _task_schedule_node()),
        ("task_generations", _task_generations_node()),
        ("schedule_metadata", _schedule_metadata_node()),
        ("plan_fingerprints", _plan_fingerprints_node()),
        ("plan_snapshots", _plan_snapshots_node()),
        ("bottom_level_costs", _bottom_level_costs_node()),
        ("dependency_map", _dependency_map_node()),
        ("plan_active_task_names", _plan_active_task_names_node()),
        ("incremental_plan_diff", _incremental_plan_diff_node(options)),
        ("active_task_names", _active_task_names_node()),
    )


def _execution_plan_node(plan: ExecutionPlan) -> object:
    @tag(layer="plan", artifact="execution_plan", kind="object")
    def execution_plan() -> ExecutionPlan:
        return plan

    return execution_plan


def _view_nodes_node() -> object:
    @tag(layer="plan", artifact="view_nodes", kind="catalog")
    def view_nodes(execution_plan: ExecutionPlan) -> tuple[ViewNode, ...]:
        return execution_plan.view_nodes

    return view_nodes


def _plan_bundles_by_task_node() -> object:
    @tag(layer="plan", artifact="plan_bundles_by_task", kind="mapping")
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
    @tag(layer="plan", artifact="dataset_specs", kind="catalog")
    def dataset_specs(execution_plan: ExecutionPlan) -> tuple[DatasetSpec, ...]:
        mapping = execution_plan.dataset_specs
        return tuple(mapping[name] for name in sorted(mapping))

    return dataset_specs


def _plan_scan_units_node() -> object:
    @tag(layer="plan", artifact="plan_scan_units", kind="catalog")
    def plan_scan_units(execution_plan: ExecutionPlan) -> tuple[ScanUnit, ...]:
        return execution_plan.scan_units

    return plan_scan_units


def _plan_scan_keys_by_task_node() -> object:
    @tag(layer="plan", artifact="plan_scan_keys_by_task", kind="mapping")
    def plan_scan_keys_by_task(
        execution_plan: ExecutionPlan,
    ) -> Mapping[str, tuple[str, ...]]:
        return execution_plan.scan_keys_by_task

    return plan_scan_keys_by_task


def _task_graph_node() -> object:
    @tag(layer="plan", artifact="task_graph", kind="graph")
    def task_graph(execution_plan: ExecutionPlan) -> TaskGraph:
        return execution_plan.task_graph

    return task_graph


def _evidence_catalog_node(options: PlanModuleOptions) -> object:
    @tag(layer="plan", artifact="evidence_catalog", kind="catalog")
    def evidence_catalog(
        execution_plan: ExecutionPlan,
        ctx: ExecutionContext,
    ) -> EvidenceCatalog:
        evidence = execution_plan.evidence
        profile = ctx.runtime.datafusion
        if profile is None:
            return evidence
        _ensure_view_graph(profile)
        if options.record_evidence_artifacts:
            _record_evidence_contract_violations(profile, evidence)
            _record_udf_parity(profile)
        return evidence

    return evidence_catalog


def _plan_signature_node() -> object:
    @tag(layer="plan", artifact="plan_signature", kind="scalar")
    def plan_signature(execution_plan: ExecutionPlan) -> str:
        return execution_plan.plan_signature

    return plan_signature


def _plan_artifacts_node() -> object:
    @tag(layer="plan", artifact="plan_artifacts", kind="mapping")
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
            "plan_critical_path_length_weighted": float | None,
            "plan_scan_unit_count": int,
            "plan_scan_file_candidate_count": int,
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
        }

    return plan_artifacts


def _task_schedule_node() -> object:
    @tag(layer="plan", artifact="task_schedule", kind="schedule")
    def task_schedule(execution_plan: ExecutionPlan) -> TaskSchedule:
        return execution_plan.task_schedule

    return task_schedule


def _task_generations_node() -> object:
    @tag(layer="plan", artifact="task_generations", kind="schedule")
    def task_generations(execution_plan: ExecutionPlan) -> tuple[tuple[str, ...], ...]:
        return execution_plan.task_schedule.generations

    return task_generations


def _schedule_metadata_node() -> object:
    @tag(layer="plan", artifact="schedule_metadata", kind="schedule")
    def schedule_metadata(
        execution_plan: ExecutionPlan,
    ) -> Mapping[str, TaskScheduleMetadata]:
        return execution_plan.schedule_metadata

    return schedule_metadata


def _plan_fingerprints_node() -> object:
    @tag(layer="plan", artifact="plan_fingerprints", kind="mapping")
    def plan_fingerprints(execution_plan: ExecutionPlan) -> Mapping[str, str]:
        return execution_plan.plan_fingerprints

    return plan_fingerprints


def _plan_snapshots_node() -> object:
    @tag(layer="plan", artifact="plan_snapshots", kind="mapping")
    def plan_snapshots(
        execution_plan: ExecutionPlan,
    ) -> Mapping[str, PlanFingerprintSnapshot]:
        return execution_plan.plan_snapshots

    return plan_snapshots


def _bottom_level_costs_node() -> object:
    @tag(layer="plan", artifact="bottom_level_costs", kind="mapping")
    def bottom_level_costs(execution_plan: ExecutionPlan) -> Mapping[str, float]:
        return execution_plan.bottom_level_costs

    return bottom_level_costs


def _dependency_map_node() -> object:
    @tag(layer="plan", artifact="dependency_map", kind="mapping")
    def dependency_map(
        execution_plan: ExecutionPlan,
    ) -> Mapping[str, tuple[str, ...]]:
        return execution_plan.dependency_map

    return dependency_map


def _plan_active_task_names_node() -> object:
    @tag(layer="plan", artifact="plan_active_task_names", kind="set")
    def plan_active_task_names(execution_plan: ExecutionPlan) -> frozenset[str]:
        return execution_plan.active_tasks

    return plan_active_task_names


def _incremental_plan_diff_node(options: PlanModuleOptions) -> object:
    @tag(layer="incremental", artifact="incremental_plan_diff", kind="mapping")
    def incremental_plan_diff(
        execution_plan: ExecutionPlan,
        incremental_config: IncrementalConfig,
        ctx: ExecutionContext,
    ) -> IncrementalDiff | None:
        if not incremental_config.enabled or incremental_config.state_dir is None:
            return None
        from incremental.delta_context import DeltaAccessContext
        from incremental.plan_fingerprints import (
            read_plan_snapshots,
            write_plan_snapshots,
        )
        from incremental.runtime import IncrementalRuntime
        from incremental.state_store import StateStore
        from relspec.incremental import diff_plan_snapshots

        try:
            runtime = IncrementalRuntime.build(ctx=ctx)
        except ValueError:
            return None
        state_store = StateStore(root=incremental_config.state_dir)
        context = DeltaAccessContext(runtime=runtime)
        current = execution_plan.plan_snapshots
        plan_state_dir = execution_plan.incremental_state_dir
        config_state_dir = str(incremental_config.state_dir)
        precomputed = execution_plan.incremental_diff
        if precomputed is not None and plan_state_dir == config_state_dir:
            diff = precomputed
        else:
            previous = read_plan_snapshots(state_store, context=context)
            diff = diff_plan_snapshots(previous, current)
        if options.record_incremental_artifacts:
            _record_plan_diff(
                diff,
                ctx=ctx,
                plan_signature=execution_plan.plan_signature,
                total_tasks=len(current),
            )
        write_plan_snapshots(state_store, current)
        return diff

    return incremental_plan_diff


def _active_task_names_node() -> object:
    @tag(layer="plan", artifact="active_task_names", kind="set")
    def active_task_names(
        execution_plan: ExecutionPlan,
        incremental_plan_diff: IncrementalDiff | None,
    ) -> frozenset[str]:
        active = set(execution_plan.active_tasks)
        requested_anchor: set[str] = set()
        if execution_plan.requested_task_names:
            requested_anchor = upstream_task_closure(
                execution_plan.task_graph,
                execution_plan.requested_task_names,
            )
            requested_anchor &= active
        diff = incremental_plan_diff
        if diff is None:
            return frozenset(active)
        rebuild = set(diff.tasks_requiring_rebuild())
        if not rebuild:
            return frozenset(active | requested_anchor)
        rebuild &= active
        if not rebuild:
            return frozenset(active | requested_anchor)
        impacted = downstream_task_closure(execution_plan.task_graph, rebuild)
        impacted &= active
        if not impacted:
            return frozenset(active | requested_anchor)
        impacted_with_deps = upstream_task_closure(execution_plan.task_graph, impacted)
        impacted_with_deps &= active
        if requested_anchor:
            impacted_with_deps |= requested_anchor
        return frozenset(impacted_with_deps)

    return active_task_names


def _ensure_view_graph(profile: DataFusionRuntimeProfile) -> None:
    from datafusion_engine.view_registry import ensure_view_graph

    session = profile.session_context()
    ensure_view_graph(
        session,
        runtime_profile=profile,
        include_registry_views=True,
    )


def _record_evidence_contract_violations(
    profile: DataFusionRuntimeProfile,
    evidence: EvidenceCatalog,
) -> None:
    if not evidence.contract_violations_by_dataset:
        return
    from datafusion_engine.diagnostics import record_artifact

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
    from datafusion_engine.diagnostics import record_artifact
    from datafusion_engine.udf_parity import udf_parity_report
    from datafusion_engine.udf_runtime import register_rust_udfs

    session = profile.session_context()
    async_timeout_ms = None
    async_batch_size = None
    if profile.enable_async_udfs:
        async_timeout_ms = profile.async_udf_timeout_ms
        async_batch_size = profile.async_udf_batch_size
    registry_snapshot = register_rust_udfs(
        session,
        enable_async=profile.enable_async_udfs,
        async_udf_timeout_ms=async_timeout_ms,
        async_udf_batch_size=async_batch_size,
    )
    report = udf_parity_report(session, snapshot=registry_snapshot)
    record_artifact(profile, "udf_parity_v1", report.payload())


def _record_plan_diff(
    diff: IncrementalDiff,
    *,
    ctx: ExecutionContext,
    plan_signature: str,
    total_tasks: int,
) -> None:
    profile = ctx.runtime.datafusion
    if profile is None:
        return
    from datafusion_engine.diagnostics import record_artifact
    from datafusion_engine.semantic_diff import ChangeCategory, RebuildPolicy

    payload: dict[str, object] = {
        "plan_signature": plan_signature,
        "total_tasks": total_tasks,
        "changed_tasks": list(diff.changed_tasks),
        "added_tasks": list(diff.added_tasks),
        "removed_tasks": list(diff.removed_tasks),
        "unchanged_tasks": list(diff.unchanged_tasks),
        "changed_count": len(diff.changed_tasks),
        "added_count": len(diff.added_tasks),
        "removed_count": len(diff.removed_tasks),
        "unchanged_count": len(diff.unchanged_tasks),
    }
    if diff.semantic_changes:
        payload["semantic_changes"] = [
            {
                "task_name": name,
                "overall_category": change.overall_category.name,
                "breaking": change.is_breaking(),
                "rebuild_needed": change.requires_rebuild(RebuildPolicy.CONSERVATIVE),
                "summary": change.summary(),
                "change_categories": [
                    item.category.name
                    for item in change.changes
                    if item.category != ChangeCategory.NONE
                ],
            }
            for name, change in diff.semantic_changes.items()
        ]
    record_artifact(profile, "incremental_plan_diff_v2", payload)


__all__ = ["PlanModuleOptions", "build_execution_plan_module"]
