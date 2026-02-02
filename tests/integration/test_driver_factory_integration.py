"""Integration tests for driver factory chains."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pytest

from tests.test_helpers.optional_deps import require_datafusion_udfs

require_datafusion_udfs()
pytest.importorskip("rustworkx")

import rustworkx as rx
from hamilton import driver

from core_types import DeterminismTier, JsonValue
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from engine.runtime_profile import RuntimeProfileSpec
from hamilton_pipeline.driver_factory import (
    DriverBuildRequest,
    ExecutionMode,
    ViewGraphContext,
    build_driver,
)
from relspec.evidence import EvidenceCatalog
from relspec.execution_plan import ExecutionPlan
from relspec.rustworkx_graph import GraphDiagnostics, TaskGraph
from relspec.rustworkx_schedule import TaskSchedule, task_schedule_metadata
from semantics.incremental.plan_fingerprints import PlanFingerprintSnapshot

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import SessionRuntime
    from datafusion_engine.views.graph import ViewNode as RegistryViewNode


@dataclass(frozen=True)
class ViewNode:
    """Minimal view node stub for driver factory tests."""

    name: str
    deps: tuple[str, ...]
    builder: Callable[[object], object]
    contract_builder: Callable[[object], object] | None = None
    plan_bundle: object | None = None


if not TYPE_CHECKING:
    RegistryViewNode = ViewNode


def _stub_view_node(name: str) -> ViewNode:
    def _build_view(_ctx: object) -> object:
        msg = "view build should not be invoked during driver factory tests"
        raise RuntimeError(msg)

    builder = cast("Callable[[object], object]", _build_view)
    return ViewNode(name=name, deps=(), builder=builder)


def _stub_execution_plan() -> ExecutionPlan:
    view_nodes = (_stub_view_node("task_a"), _stub_view_node("task_b"))
    resolved_view_nodes = cast("tuple[RegistryViewNode, ...]", view_nodes)
    ordered_tasks = ("task_a", "task_b")
    schedule = TaskSchedule(ordered_tasks=ordered_tasks, generations=(ordered_tasks,))
    schedule_metadata = task_schedule_metadata(schedule)
    plan_fingerprints = {name: f"fp:{name}" for name in ordered_tasks}
    plan_task_signatures = {name: f"sig:{name}" for name in ordered_tasks}
    plan_snapshots = {
        name: PlanFingerprintSnapshot(
            plan_fingerprint=plan_fingerprints[name],
            plan_task_signature=plan_task_signatures[name],
        )
        for name in ordered_tasks
    }
    empty_graph = rx.PyDiGraph(multigraph=False, check_cycle=False)
    task_graph = TaskGraph(
        graph=empty_graph,
        evidence_idx={},
        task_idx={},
        output_policy="all_producers",
    )
    return ExecutionPlan(
        view_nodes=resolved_view_nodes,
        task_graph=task_graph,
        task_dependency_graph=empty_graph,
        reduced_task_dependency_graph=empty_graph,
        evidence=EvidenceCatalog(),
        task_schedule=schedule,
        schedule_metadata=schedule_metadata,
        plan_fingerprints=plan_fingerprints,
        plan_task_signatures=plan_task_signatures,
        plan_snapshots=plan_snapshots,
        output_contracts={},
        plan_signature="plan:driver_factory:test",
        task_dependency_signature="deps:driver_factory:test",
        reduced_task_dependency_signature="deps:driver_factory:reduced:test",
        reduction_node_map={},
        reduction_edge_count=0,
        reduction_removed_edge_count=0,
        diagnostics=GraphDiagnostics(status="ok"),
        critical_path_task_names=("task_b",),
        critical_path_length_weighted=1.0,
        bottom_level_costs={"task_a": 1.0, "task_b": 1.0},
        slack_by_task={"task_a": 0.0, "task_b": 0.0},
        task_plan_metrics={},
        task_costs={},
        dependency_map={"task_a": (), "task_b": ("task_a",)},
        dataset_specs={},
        active_tasks=frozenset(ordered_tasks),
    )


def _stub_view_context(plan: ExecutionPlan) -> ViewGraphContext:
    profile = DataFusionRuntimeProfile(
        enable_function_factory=False,
        enable_udfs=False,
        enable_schema_registry=False,
        enable_expr_planners=False,
        enable_metrics=False,
        enable_tracing=False,
        enable_cache_manager=False,
        capture_plan_artifacts=False,
    )
    profile_spec = RuntimeProfileSpec(
        name="driver_factory_test",
        datafusion=profile,
        determinism_tier=DeterminismTier.BEST_EFFORT,
        tracker_config=None,
    )
    return ViewGraphContext(
        profile=profile,
        session_runtime=cast("SessionRuntime", object()),
        determinism_tier=DeterminismTier.BEST_EFFORT,
        snapshot={},
        view_nodes=plan.view_nodes,
        runtime_profile_spec=profile_spec,
    )


def _base_config() -> dict[str, JsonValue]:
    return {
        "enable_semantic_registry": False,
        "enable_hamilton_tracker": False,
        "enable_hamilton_type_checker": False,
        "enable_hamilton_node_diagnostics": False,
        "enable_plan_diagnostics": False,
        "enable_otel_node_tracing": False,
        "enable_otel_plan_tracing": False,
    }


@pytest.mark.integration
def test_build_driver_with_stub_plan() -> None:
    """Ensure build_driver assembles a driver with a stub plan."""
    plan = _stub_execution_plan()
    view_ctx = _stub_view_context(plan)
    request = DriverBuildRequest(
        config=_base_config(),
        plan=plan,
        view_ctx=view_ctx,
        execution_mode=ExecutionMode.DETERMINISTIC_SERIAL,
    )
    driver_instance = build_driver(request=request)
    assert isinstance(driver_instance, driver.Driver)
