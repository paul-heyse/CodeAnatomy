"""Unit tests for plan-driven scheduling and semantic observability."""

from __future__ import annotations

import shutil
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, cast

import pytest
import rustworkx as rx
from hamilton import driver
from hamilton.caching.cache_key import decode_key
from hamilton.caching.stores.file import FileResultStore
from hamilton.caching.stores.sqlite import SQLiteMetadataStore
from hamilton.function_modifiers import cache

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
    """Minimal view node stub for plan unification tests."""

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
    stub.__dict__["IntrospectionSnapshot"] = object
    sys.modules[stub.__name__] = stub


_install_introspection_stub()


def _dummy_view_node(name: str) -> ViewNode:
    def _build_view(_ctx: object) -> object:
        msg = "view build should not be invoked during module generation"
        raise RuntimeError(msg)

    builder = cast("Callable[[SessionContext], DataFrame]", _build_view)
    return ViewNode(name=name, deps=(), builder=builder)


def _driver_for_modules(*modules: ModuleType) -> driver.Driver:
    return driver.Builder().allow_module_overrides().with_modules(*modules).build()


def _plan_for_tests(
    *,
    view_nodes: tuple[ViewNode, ...],
    dependency_map: dict[str, tuple[str, ...]],
    schedule_metadata: dict[str, TaskScheduleMetadata] | None = None,
    bottom_level_costs: dict[str, float] | None = None,
    plan_signature: str = "plan:test",
) -> ExecutionPlan:
    from incremental.plan_fingerprints import PlanFingerprintSnapshot
    from relspec.evidence import EvidenceCatalog
    from relspec.execution_plan import ExecutionPlan
    from relspec.rustworkx_graph import GraphDiagnostics, TaskGraph
    from relspec.rustworkx_schedule import TaskSchedule, task_schedule_metadata

    resolved_view_nodes = cast("tuple[RegistryViewNode, ...]", view_nodes)
    plan_fingerprints = {node.name: f"{node.name}_fingerprint" for node in view_nodes}
    plan_snapshots = {
        name: PlanFingerprintSnapshot(plan_fingerprint=fingerprint)
        for name, fingerprint in plan_fingerprints.items()
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
    resolved_bottom_costs = bottom_level_costs or dict.fromkeys(ordered_tasks, 1.0)
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
        plan_snapshots=plan_snapshots,
        output_contracts=output_contracts,
        plan_signature=plan_signature,
        task_dependency_signature="task_dep:test",
        reduced_task_dependency_signature="task_dep:reduced:test",
        reduction_node_map={},
        reduction_edge_count=0,
        reduction_removed_edge_count=0,
        diagnostics=GraphDiagnostics(status="ok"),
        critical_path_task_names=(),
        critical_path_length_weighted=0.0,
        bottom_level_costs=resolved_bottom_costs,
        dependency_map=dependency_map,
        dataset_specs=dataset_specs,
        active_tasks=frozenset(plan_fingerprints),
    )


def _cache_module(module_name: str) -> ModuleType:
    module = ModuleType(module_name)
    sys.modules[module_name] = module

    @cache(format="pickle", behavior="default")
    def cached_value(plan_signature: str) -> str:
        return plan_signature

    cached_value.__module__ = module_name
    module.__dict__["cached_value"] = cached_value
    module.__dict__["__all__"] = ["cached_value"]
    return module


def _apply_cache(builder: driver.Builder, cache_dir: Path) -> driver.Builder:
    cache_dir.mkdir(parents=True, exist_ok=True)
    metadata_store = SQLiteMetadataStore(path=str(cache_dir / "meta.sqlite"))
    results_dir = cache_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    result_store = FileResultStore(path=str(results_dir))
    return builder.with_cache(
        path=str(cache_dir),
        metadata_store=metadata_store,
        result_store=result_store,
        default_behavior="disable",
        log_to_file=True,
    )


def _run_and_get_run_id(driver_instance: driver.Driver) -> str:
    before = set(driver_instance.cache.logs(run_id=None, level="debug"))
    driver_instance.execute(["cached_value"])
    after = set(driver_instance.cache.logs(run_id=None, level="debug"))
    new_ids = after - before
    if len(new_ids) != 1:
        msg = f"Expected exactly one new run id, found {sorted(new_ids)}."
        raise ValueError(msg)
    run_id = next(iter(new_ids))
    if not isinstance(run_id, str):
        msg = f"Unexpected run id type: {type(run_id).__name__}."
        raise TypeError(msg)
    return run_id


def _plan_signature_data_version(driver_instance: driver.Driver, run_id: str) -> str:
    cache_key = driver_instance.cache.get_cache_key(
        run_id=run_id,
        node_name="cached_value",
        task_id=None,
    )
    if not isinstance(cache_key, str):
        msg = f"Expected string cache key, found {type(cache_key).__name__}."
        raise TypeError(msg)
    decoded = decode_key(cache_key)
    dependencies = decoded.get("dependencies_data_versions")
    if not isinstance(dependencies, Mapping):
        msg = "Cache key decode missing dependency data versions."
        raise TypeError(msg)
    value = dependencies.get("plan_signature")
    if not isinstance(value, str):
        msg = "Plan signature data version missing from cache key decode."
        raise TypeError(msg)
    return value


def _build_cached_driver(plan: ExecutionPlan, cache_dir: Path, *, suffix: str) -> driver.Driver:
    from hamilton_pipeline.modules.execution_plan import (
        PlanModuleOptions,
        build_execution_plan_module,
    )

    plan_module = build_execution_plan_module(
        plan,
        options=PlanModuleOptions(module_name=f"hamilton_pipeline.generated_plan_cache_{suffix}"),
    )
    cache_module = _cache_module(f"hamilton_pipeline.generated_cache_module_{suffix}")
    builder = (
        driver.Builder()
        .allow_module_overrides()
        .with_modules(plan_module, cache_module)
        .with_config({})
    )
    return _apply_cache(builder, cache_dir).build()


def _data_versions_for_runs(driver_instance: driver.Driver, *, runs: int) -> tuple[str, ...]:
    versions: list[str] = []
    for _ in range(runs):
        run_id = _run_and_get_run_id(driver_instance)
        versions.append(_plan_signature_data_version(driver_instance, run_id))
    return tuple(versions)


def test_semantic_registry_captures_semantic_outputs() -> None:
    """Ensure semantic-tagged outputs appear in the compiled registry."""
    from hamilton_pipeline import modules as hamilton_modules
    from hamilton_pipeline.modules.execution_plan import (
        PlanModuleOptions,
        build_execution_plan_module,
    )
    from hamilton_pipeline.semantic_registry import compile_semantic_registry
    from hamilton_pipeline.task_module_builder import (
        TaskExecutionModuleOptions,
        build_task_execution_module,
    )

    view_nodes = (
        _dummy_view_node("out_alpha"),
        _dummy_view_node("out_beta"),
    )
    plan = _plan_for_tests(
        view_nodes=view_nodes,
        dependency_map={"out_beta": ("out_alpha",)},
    )
    plan_module = build_execution_plan_module(
        plan,
        options=PlanModuleOptions(module_name="hamilton_pipeline.generated_plan_semantic_test"),
    )
    task_module = build_task_execution_module(
        plan=plan,
        options=TaskExecutionModuleOptions(
            module_name="hamilton_pipeline.generated_tasks_semantic_test",
        ),
    )
    driver_instance = _driver_for_modules(
        hamilton_modules.task_execution,
        hamilton_modules.outputs,
        plan_module,
        task_module,
    )
    registry = compile_semantic_registry(
        driver_instance.graph.nodes,
        plan_signature=plan.plan_signature,
    )
    semantic_ids = {record.semantic_id for record in registry.records}
    assert {"cpg.nodes.v1", "cpg.edges.v1", "cpg.props.v1"} <= semantic_ids
    assert registry.errors == ()


def test_plan_grouping_strategy_orders_by_generation_and_cost() -> None:
    """Ensure plan grouping honors generations and bottom-level criticality."""
    from hamilton_pipeline.modules.execution_plan import (
        PlanModuleOptions,
        build_execution_plan_module,
    )
    from hamilton_pipeline.scheduling_hooks import plan_grouping_strategy
    from hamilton_pipeline.task_module_builder import (
        TaskExecutionModuleOptions,
        build_task_execution_module,
    )
    from relspec.schedule_events import TaskScheduleMetadata

    view_nodes = (
        _dummy_view_node("out_alpha"),
        _dummy_view_node("out_beta"),
        _dummy_view_node("out_gamma"),
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
            generation_order=1,
            generation_size=2,
        ),
        "out_gamma": TaskScheduleMetadata(
            schedule_index=2,
            generation_index=1,
            generation_order=0,
            generation_size=2,
        ),
    }
    plan = _plan_for_tests(
        view_nodes=view_nodes,
        dependency_map={"out_beta": ("out_alpha",), "out_gamma": ("out_alpha",)},
        schedule_metadata=schedule_metadata,
        bottom_level_costs={"out_alpha": 2.0, "out_beta": 1.0, "out_gamma": 5.0},
    )
    plan_module = build_execution_plan_module(
        plan,
        options=PlanModuleOptions(module_name="hamilton_pipeline.generated_plan_grouping_test"),
    )
    task_module = build_task_execution_module(
        plan=plan,
        options=TaskExecutionModuleOptions(
            module_name="hamilton_pipeline.generated_tasks_grouping_test",
        ),
    )
    driver_instance = _driver_for_modules(plan_module, task_module)
    strategy = plan_grouping_strategy(plan)
    groups = strategy.group_nodes(list(driver_instance.graph.nodes.values()))
    generation_groups = {
        group.base_id: group for group in groups if group.base_id.startswith("generation_")
    }
    gen_one = generation_groups["generation_1"]
    gen_one_nodes = [node.name for node in gen_one.nodes]
    assert gen_one_nodes == ["out_gamma", "out_beta"]


def test_task_execution_rejects_plan_signature_mismatch() -> None:
    """Ensure plan signature mismatches fail fast before execution."""
    from hamilton_pipeline.modules.task_execution import (
        TaskExecutionInputs,
        TaskExecutionSpec,
        execute_task_from_catalog,
    )
    from relspec.evidence import EvidenceCatalog
    from relspec.runtime_artifacts import RuntimeArtifacts

    inputs = TaskExecutionInputs(
        runtime=RuntimeArtifacts(),
        evidence=EvidenceCatalog(),
        plan_signature="plan:a",
        active_task_names=frozenset({"out_alpha"}),
        scan_units=(),
        scan_keys_by_task={},
        scan_units_hash=None,
    )
    spec = TaskExecutionSpec(
        task_name="out_alpha",
        task_output="out_alpha",
        plan_fingerprint="fingerprint:a",
    )
    with pytest.raises(ValueError, match="Plan signature mismatch"):
        execute_task_from_catalog(
            inputs=inputs,
            dependencies=(),
            plan_signature="plan:b",
            spec=spec,
        )


def test_plan_signature_participates_in_cache_keys(tmp_path: Path) -> None:
    """Ensure plan signatures flow into cache key dependency versions."""
    cache_dir = tmp_path / "plan_signature_cache"
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
    view_nodes = (_dummy_view_node("out_alpha"),)
    dependency_map: dict[str, tuple[str, ...]] = {}

    plan_a = _plan_for_tests(
        view_nodes=view_nodes,
        dependency_map=dependency_map,
        plan_signature="plan:a",
    )
    driver_a = _build_cached_driver(plan_a, cache_dir, suffix="a")
    versions_a = _data_versions_for_runs(driver_a, runs=2)
    assert versions_a[0] == versions_a[1]

    plan_b = _plan_for_tests(
        view_nodes=view_nodes,
        dependency_map=dependency_map,
        plan_signature="plan:b",
    )
    driver_b = _build_cached_driver(plan_b, cache_dir, suffix="b")
    version_b = _data_versions_for_runs(driver_b, runs=1)[0]
    assert version_b != versions_a[0]
