"""Tests for plan artifact extraction in the Hamilton plan module."""

from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, cast

import rustworkx as rx
from hamilton import driver

import test_support.datafusion_ext_stub as _datafusion_ext_stub
import test_support.view_specs_stub as _view_specs_stub

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

    from datafusion_engine.view_graph_registry import ViewNode as RegistryViewNode
    from relspec.execution_plan import ExecutionPlan

_ = (_datafusion_ext_stub, _view_specs_stub)


@dataclass(frozen=True)
class ViewNode:
    """Minimal view node stub for plan artifact module tests."""

    name: str
    deps: tuple[str, ...]
    builder: Callable[[SessionContext], DataFrame]
    contract_builder: Callable[[object], object] | None = None


if not TYPE_CHECKING:
    RegistryViewNode = ViewNode


def _install_introspection_stub() -> None:
    if "datafusion_engine.introspection" in sys.modules:
        return
    stub = ModuleType("datafusion_engine.introspection")

    class _Snapshot:
        routines: object | None = None
        settings: object | None = None
        tables: object | None = None
        providers: object | None = None
        constraints: object | None = None
        parameters: object | None = None

    class _IntrospectionCache:
        snapshot: _Snapshot = _Snapshot()

    def introspection_cache_for_ctx(
        _ctx: object,
        *,
        sql_options: object | None = None,
    ) -> _IntrospectionCache:
        _ = sql_options
        return _IntrospectionCache()

    def invalidate_introspection_cache(_ctx: object | None = None) -> None:
        return None

    stub.__dict__["introspection_cache_for_ctx"] = introspection_cache_for_ctx
    stub.__dict__["invalidate_introspection_cache"] = invalidate_introspection_cache
    stub.__dict__["IntrospectionCache"] = _IntrospectionCache
    stub.__dict__["IntrospectionSnapshot"] = object
    sys.modules[stub.__name__] = stub


_install_introspection_stub()


def _dummy_view_node(name: str) -> ViewNode:
    def _build_view(_ctx: object) -> object:
        msg = "view build should not be invoked during plan artifact tests"
        raise RuntimeError(msg)

    builder = cast("Callable[[SessionContext], DataFrame]", _build_view)
    return ViewNode(name=name, deps=(), builder=builder)


def _plan_for_tests() -> ExecutionPlan:
    from incremental.plan_fingerprints import PlanFingerprintSnapshot
    from relspec.evidence import EvidenceCatalog
    from relspec.execution_plan import ExecutionPlan
    from relspec.rustworkx_graph import GraphDiagnostics, TaskGraph
    from relspec.rustworkx_schedule import TaskSchedule, task_schedule_metadata

    view_nodes = (_dummy_view_node(name="task_a"), _dummy_view_node(name="task_b"))
    resolved_view_nodes = cast("tuple[RegistryViewNode, ...]", view_nodes)
    ordered_tasks = ("task_a", "task_b")
    schedule = TaskSchedule(ordered_tasks=ordered_tasks, generations=(ordered_tasks,))
    schedule_metadata = task_schedule_metadata(schedule)
    plan_fingerprints = {name: f"fp:{name}" for name in ordered_tasks}
    plan_snapshots = {
        name: PlanFingerprintSnapshot(plan_fingerprint=fingerprint)
        for name, fingerprint in plan_fingerprints.items()
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
        plan_snapshots=plan_snapshots,
        output_contracts={},
        plan_signature="plan:artifacts:test",
        task_dependency_signature="deps:artifacts:test",
        reduced_task_dependency_signature="deps:artifacts:reduced:test",
        reduction_node_map={},
        reduction_edge_count=3,
        reduction_removed_edge_count=1,
        diagnostics=GraphDiagnostics(status="ok"),
        critical_path_task_names=("task_b",),
        critical_path_length_weighted=4.0,
        bottom_level_costs={"task_a": 1.0, "task_b": 2.0},
        dependency_map={"task_a": (), "task_b": ("task_a",)},
        dataset_specs={},
        active_tasks=frozenset(ordered_tasks),
    )


def test_plan_artifact_extract_fields_expose_plan_nodes() -> None:
    """Extracted plan artifacts are exposed as first-class Hamilton nodes."""
    plan = _plan_for_tests()
    from hamilton_pipeline.modules.execution_plan import (
        PlanModuleOptions,
        build_execution_plan_module,
    )

    module = build_execution_plan_module(
        plan,
        options=PlanModuleOptions(module_name="hamilton_pipeline.generated_plan_artifacts_test"),
    )
    drv = driver.Builder().allow_module_overrides().with_modules(module).with_config({}).build()
    result = drv.execute(
        [
            "plan_task_dependency_signature",
            "plan_reduction_reduced_edge_count",
            "plan_generation_count",
        ]
    )
    expected_reduced_edge_count = plan.reduction_edge_count - plan.reduction_removed_edge_count
    expected_generation_count = len(plan.task_schedule.generations)
    assert result["plan_task_dependency_signature"] == "deps:artifacts:test"
    assert result["plan_reduction_reduced_edge_count"] == expected_reduced_edge_count
    assert result["plan_generation_count"] == expected_generation_count
