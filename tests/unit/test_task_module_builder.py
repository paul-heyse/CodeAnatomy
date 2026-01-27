"""Unit tests for dynamic Hamilton task module generation."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, cast

import rustworkx as rx
from hamilton import driver

if TYPE_CHECKING:
    from collections.abc import Callable

    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

    from datafusion_engine.view_graph_registry import ViewNode as RegistryViewNode
    from relspec.execution_plan import ExecutionPlan
    from relspec.schedule_events import TaskScheduleMetadata
    from schema_spec.system import DatasetSpec


@dataclass(frozen=True)
class ViewNode:
    """Minimal view node stub for task module tests."""

    name: str
    deps: tuple[str, ...]
    builder: Callable[[SessionContext], DataFrame]
    contract_builder: Callable[[object], object] | None = None


if not TYPE_CHECKING:
    from relspec.schedule_events import TaskScheduleMetadata

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
    stub.__dict__["IntrospectionSnapshot"] = object
    sys.modules[stub.__name__] = stub


_install_introspection_stub()


def _dummy_view_node(name: str) -> ViewNode:
    def _build_view(_ctx: object) -> object:
        msg = "view build should not be invoked during module generation"
        raise RuntimeError(msg)

    builder = cast("Callable[[SessionContext], DataFrame]", _build_view)
    return ViewNode(name=name, deps=(), builder=builder)


def _driver_for_module(module: ModuleType) -> driver.Driver:
    return driver.Builder().with_modules(module).build()


def _plan_for_module(
    *,
    view_nodes: tuple[ViewNode, ...],
    dependency_map: dict[str, tuple[str, ...]],
    schedule_metadata: dict[str, TaskScheduleMetadata] | None = None,
) -> ExecutionPlan:
    from incremental.plan_fingerprints import PlanFingerprintSnapshot
    from relspec.evidence import EvidenceCatalog
    from relspec.execution_plan import ExecutionPlan
    from relspec.rustworkx_graph import GraphDiagnostics, TaskGraph
    from relspec.rustworkx_schedule import TaskSchedule, task_schedule_metadata

    resolved_view_nodes = cast("tuple[RegistryViewNode, ...]", view_nodes)
    plan_fingerprints = {node.name: f"{node.name}_fingerprint" for node in view_nodes}
    plan_task_signatures = {name: f"{name}_signature" for name in plan_fingerprints}
    plan_snapshots = {
        name: PlanFingerprintSnapshot(
            plan_fingerprint=plan_fingerprints[name],
            plan_task_signature=plan_task_signatures[name],
        )
        for name in plan_fingerprints
    }
    ordered_tasks = tuple(sorted(plan_fingerprints))
    task_schedule = TaskSchedule(ordered_tasks=ordered_tasks, generations=(ordered_tasks,))
    resolved_metadata: dict[str, TaskScheduleMetadata]
    if schedule_metadata is not None:
        resolved_metadata = schedule_metadata
    else:
        resolved_metadata = task_schedule_metadata(task_schedule)
    evidence_idx: dict[str, int] = {}
    task_idx: dict[str, int] = {}
    task_graph = TaskGraph(
        graph=rx.PyDiGraph(multigraph=False, check_cycle=False),
        evidence_idx=evidence_idx,
        task_idx=task_idx,
        output_policy="all_producers",
    )
    task_dependency_graph = rx.PyDiGraph(multigraph=False, check_cycle=False)
    reduced_task_dependency_graph = rx.PyDiGraph(multigraph=False, check_cycle=False)
    bottom_level_costs = dict.fromkeys(ordered_tasks, 1.0)
    dataset_specs: dict[str, DatasetSpec] = {}
    output_contracts: dict[str, object] = {}
    return ExecutionPlan(
        view_nodes=resolved_view_nodes,
        task_graph=task_graph,
        task_dependency_graph=task_dependency_graph,
        reduced_task_dependency_graph=reduced_task_dependency_graph,
        evidence=EvidenceCatalog(),
        task_schedule=task_schedule,
        schedule_metadata=resolved_metadata,
        plan_fingerprints=plan_fingerprints,
        plan_task_signatures=plan_task_signatures,
        plan_snapshots=plan_snapshots,
        output_contracts=output_contracts,
        plan_signature="plan:test",
        task_dependency_signature="task_dep:test",
        reduced_task_dependency_signature="task_dep:reduced:test",
        reduction_node_map={},
        reduction_edge_count=0,
        reduction_removed_edge_count=0,
        diagnostics=GraphDiagnostics(status="ok"),
        critical_path_task_names=(),
        critical_path_length_weighted=0.0,
        bottom_level_costs=bottom_level_costs,
        dependency_map=dependency_map,
        dataset_specs=dataset_specs,
        active_tasks=frozenset(plan_fingerprints),
    )


def test_task_module_builder_wires_dependencies() -> None:
    """Ensure dependency mapping creates upstream edges in the graph."""
    from hamilton_pipeline.task_module_builder import build_task_execution_module

    view_nodes = (
        _dummy_view_node("out_alpha"),
        _dummy_view_node("out_beta"),
    )
    plan = _plan_for_module(
        view_nodes=view_nodes,
        dependency_map={"out_beta": ("out_alpha",)},
    )
    module = build_task_execution_module(
        plan=plan,
    )
    graph = _driver_for_module(module).graph
    node = graph.nodes["out_beta"]
    deps = {dep.name for dep in node.dependencies}
    assert "out_alpha" in deps


def test_task_module_builder_applies_schedule_tags() -> None:
    """Ensure schedule metadata is propagated to task node tags."""
    from hamilton_pipeline.task_module_builder import (
        TaskExecutionModuleOptions,
        build_task_execution_module,
    )

    view_nodes = (
        _dummy_view_node("out_alpha"),
        _dummy_view_node("out_beta"),
    )
    schedule_metadata = {
        "out_alpha": TaskScheduleMetadata(
            schedule_index=0,
            generation_index=0,
            generation_order=0,
            generation_size=1,
        ),
        "out_beta": TaskScheduleMetadata(
            schedule_index=1,
            generation_index=1,
            generation_order=0,
            generation_size=1,
        ),
    }
    plan = _plan_for_module(
        view_nodes=view_nodes,
        dependency_map={"out_beta": ("out_alpha",)},
        schedule_metadata=schedule_metadata,
    )
    module = build_task_execution_module(
        plan=plan,
        options=TaskExecutionModuleOptions(),
    )
    graph = _driver_for_module(module).graph
    tags = graph.nodes["out_beta"].tags
    assert tags["generation_index"] == "1"
    assert tags["generation_order"] == "0"
    assert tags["generation_size"] == "1"
