"""Tests for plan-vs-runtime drift diagnostics payloads."""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, cast

import rustworkx as rx

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

    from datafusion_engine.views.graph import ViewNode as RegistryViewNode
    from relspec.execution_plan import ExecutionPlan


@dataclass(frozen=True)
class ViewNode:
    """Minimal view node stub for drift diagnostics tests."""

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
        msg = "view build should not be invoked during drift diagnostics tests"
        raise RuntimeError(msg)

    builder = cast("Callable[[SessionContext], DataFrame]", _build_view)
    return ViewNode(name=name, deps=(), builder=builder)


def _plan_for_tests() -> ExecutionPlan:
    from relspec.evidence import EvidenceCatalog
    from relspec.execution_plan import ExecutionPlan
    from relspec.rustworkx_graph import GraphDiagnostics, TaskGraph
    from relspec.rustworkx_schedule import TaskSchedule, task_schedule_metadata
    from semantics.incremental.plan_fingerprints import PlanFingerprintSnapshot

    view_nodes = (_dummy_view_node(name="task_a"), _dummy_view_node(name="task_b"))
    resolved_view_nodes = cast("tuple[RegistryViewNode, ...]", view_nodes)
    ordered_tasks = ("task_a", "task_b")
    schedule = TaskSchedule(
        ordered_tasks=ordered_tasks,
        generations=(("task_a",), ("task_b",)),
    )
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
        plan_signature="plan:drift:test",
        task_dependency_signature="deps:drift:test",
        reduced_task_dependency_signature="deps:drift:reduced:test",
        reduction_node_map={},
        reduction_edge_count=2,
        reduction_removed_edge_count=1,
        diagnostics=GraphDiagnostics(status="ok"),
        critical_path_task_names=("task_b",),
        critical_path_length_weighted=2.0,
        bottom_level_costs={"task_a": 1.0, "task_b": 2.0},
        slack_by_task={"task_a": 0.0, "task_b": 0.0},
        task_plan_metrics={},
        task_costs={},
        dependency_map={"task_a": (), "task_b": ("task_a",)},
        dataset_specs={},
        active_tasks=frozenset(ordered_tasks),
    )


def test_plan_drift_payload_reports_missing_and_unexpected_tasks() -> None:
    """Drift payload captures admission gaps and generation coverage."""
    plan = _plan_for_tests()
    from hamilton_pipeline.lifecycle import _plan_drift_payload

    events_snapshot: dict[str, list[Mapping[str, object]]] = {
        "hamilton_task_submission_v1": [
            {
                "admitted_tasks": ["task_a", "task_extra"],
                "task_facts": [
                    {"generation_index": 0},
                    {"generation_index": 99},
                ],
            }
        ],
        "hamilton_task_grouping_v1": [{"run_id": "run-1"}],
        "hamilton_task_expansion_v1": [{"run_id": "run-1"}, {"run_id": "run-1"}],
    }
    payload = _plan_drift_payload(
        plan,
        events_snapshot=events_snapshot,
        run_id="run-1",
    )
    plan_task_count = len(plan.active_tasks)
    submission_event_count = len(events_snapshot["hamilton_task_submission_v1"])
    grouping_event_count = len(events_snapshot["hamilton_task_grouping_v1"])
    expansion_event_count = len(events_snapshot["hamilton_task_expansion_v1"])
    expected_generations = sorted(
        {meta.generation_index for meta in plan.schedule_metadata.values()}
    )
    admitted_generation_indices: set[int] = set()
    for row in events_snapshot["hamilton_task_submission_v1"]:
        task_facts_value = row.get("task_facts")
        if not isinstance(task_facts_value, list):
            continue
        for fact in task_facts_value:
            if not isinstance(fact, Mapping):
                continue
            generation_value = fact.get("generation_index")
            if isinstance(generation_value, int) and not isinstance(generation_value, bool):
                admitted_generation_indices.add(generation_value)
    expected_missing_generations = sorted(set(expected_generations) - admitted_generation_indices)
    expected_coverage_ratio = float(plan_task_count) / float(plan_task_count)
    assert payload["plan_task_count"] == plan_task_count
    assert payload["admitted_task_count"] == plan_task_count
    assert payload["admitted_task_coverage_ratio"] == expected_coverage_ratio
    assert payload["missing_admitted_tasks"] == ["task_b"]
    assert payload["unexpected_admitted_tasks"] == ["task_extra"]
    assert payload["plan_generations"] == expected_generations
    assert payload["missing_generations"] == expected_missing_generations
    assert payload["submission_event_count"] == submission_event_count
    assert payload["grouping_event_count"] == grouping_event_count
    assert payload["expansion_event_count"] == expansion_event_count
