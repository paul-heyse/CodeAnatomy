"""Tests for plan-native Hamilton task admission hooks."""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, cast

import pytest
import rustworkx as rx

import test_support.datafusion_ext_stub as _datafusion_ext_stub
import test_support.view_specs_stub as _view_specs_stub
from obs.diagnostics import DiagnosticsCollector

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

    from datafusion_engine.view_graph_registry import ViewNode as RegistryViewNode
    from hamilton_pipeline.scheduling_hooks import PlanTaskSubmissionHook
    from relspec.execution_plan import ExecutionPlan

_ = (_datafusion_ext_stub, _view_specs_stub)


@dataclass(frozen=True)
class ViewNode:
    """Minimal view node stub for plan admission tests."""

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

    class _IntrospectionCache:
        snapshot: object | None = None

    def introspection_cache_for_ctx(_ctx: object) -> _IntrospectionCache:
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
        msg = "view build should not be invoked during admission tests"
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
    schedule = TaskSchedule(
        ordered_tasks=ordered_tasks,
        generations=(("task_a",), ("task_b",)),
    )
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
        plan_signature="plan:test",
        task_dependency_signature="deps:test",
        reduced_task_dependency_signature="deps:reduced:test",
        reduction_node_map={},
        reduction_edge_count=1,
        reduction_removed_edge_count=0,
        diagnostics=GraphDiagnostics(status="ok"),
        critical_path_task_names=("task_b",),
        critical_path_length_weighted=2.0,
        bottom_level_costs={"task_a": 1.0, "task_b": 3.0},
        dependency_map={"task_a": (), "task_b": ("task_a",)},
        dataset_specs={},
        active_tasks=frozenset(ordered_tasks),
    )


@dataclass(frozen=True)
class _StubNode:
    """Minimal Hamilton node stub for admission hook testing."""

    name: str
    tags: Mapping[str, object]


def _execution_node(name: str) -> _StubNode:
    return _StubNode(name=name, tags={"layer": "execution", "kind": "task"})


def _submission_hook(
    *,
    plan: ExecutionPlan,
    diagnostics: DiagnosticsCollector,
    enforce_active: bool,
) -> PlanTaskSubmissionHook:
    from hamilton_pipeline.scheduling_hooks import PlanTaskSubmissionHook

    return PlanTaskSubmissionHook(
        plan=plan,
        diagnostics=diagnostics,
        enforce_active=enforce_active,
    )


def test_submission_hook_rejects_inactive_tasks_and_records_events() -> None:
    """Admission control rejects inactive tasks while still recording diagnostics."""
    plan = _plan_for_tests()
    diagnostics = DiagnosticsCollector()
    hook = _submission_hook(
        plan=plan,
        diagnostics=diagnostics,
        enforce_active=True,
    )
    nodes = [_execution_node("task_a"), _execution_node("task_b")]
    with pytest.raises(
        ValueError,
        match="Plan admission control rejected tasks not in the active set",
    ):
        hook.run_before_task_submission(
            run_id="run-1",
            task_id="task-1",
            nodes=nodes,
            inputs={"active_task_names": ("task_a",)},
            overrides={},
            spawning_task_id=None,
            purpose=None,
        )
    events = diagnostics.events_snapshot().get("hamilton_task_submission_v1", [])
    assert len(events) == 1
    row = events[0]
    assert row.get("admitted_tasks") == ["task_a"]
    assert row.get("rejected_tasks") == ["task_b"]
    assert row.get("unknown_tasks") == []


def test_submission_hook_allows_inactive_tasks_when_not_enforced() -> None:
    """Admission diagnostics still emit when enforcement is disabled."""
    plan = _plan_for_tests()
    diagnostics = DiagnosticsCollector()
    hook = _submission_hook(
        plan=plan,
        diagnostics=diagnostics,
        enforce_active=False,
    )
    nodes = [_execution_node("task_a"), _execution_node("task_b")]
    hook.run_before_task_submission(
        run_id="run-2",
        task_id="task-2",
        nodes=nodes,
        inputs={"active_task_names": ["task_a"]},
        overrides={},
        spawning_task_id=None,
        purpose=None,
    )
    events = diagnostics.events_snapshot().get("hamilton_task_submission_v1", [])
    assert len(events) == 1
    row = events[0]
    assert row.get("admitted_tasks") == ["task_a"]
    assert row.get("rejected_tasks") == ["task_b"]
