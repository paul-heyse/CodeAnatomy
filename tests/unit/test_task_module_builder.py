"""Unit tests for dynamic Hamilton task module generation."""

from __future__ import annotations

from types import ModuleType

from hamilton import driver

from hamilton_pipeline.task_module_builder import (
    TaskExecutionModuleOptions,
    build_task_execution_module,
)
from ibis_engine.plan import IbisPlan
from relspec.schedule_events import TaskScheduleMetadata
from relspec.task_catalog import TaskBuildContext, TaskCatalog, TaskSpec


def _dummy_build(_: TaskBuildContext) -> IbisPlan:
    msg = "build should not be invoked during module generation"
    raise RuntimeError(msg)


def _driver_for_module(module: ModuleType) -> driver.Driver:
    return driver.Builder().with_modules(module).build()


def test_task_module_builder_wires_dependencies() -> None:
    """Ensure dependency mapping creates upstream edges in the graph."""
    catalog = TaskCatalog(
        tasks=(
            TaskSpec(name="task.alpha", output="out_alpha", build=_dummy_build),
            TaskSpec(name="task.beta", output="out_beta", build=_dummy_build),
        )
    )
    module = build_task_execution_module(
        dependency_map={"out_beta": ("out_alpha",)},
        task_catalog=catalog,
    )
    graph = _driver_for_module(module).graph
    node = graph.nodes["out_beta"]
    deps = {dep.name for dep in node.dependencies}
    assert "out_alpha" in deps


def test_task_module_builder_applies_schedule_tags() -> None:
    """Ensure schedule metadata is propagated to task node tags."""
    catalog = TaskCatalog(
        tasks=(
            TaskSpec(name="task.alpha", output="out_alpha", build=_dummy_build),
            TaskSpec(name="task.beta", output="out_beta", build=_dummy_build),
        )
    )
    schedule_metadata = {
        "task.alpha": TaskScheduleMetadata(
            schedule_index=0,
            generation_index=0,
            generation_order=0,
            generation_size=1,
        ),
        "task.beta": TaskScheduleMetadata(
            schedule_index=1,
            generation_index=1,
            generation_order=0,
            generation_size=1,
        ),
    }
    module = build_task_execution_module(
        dependency_map={"out_beta": ("out_alpha",)},
        task_catalog=catalog,
        options=TaskExecutionModuleOptions(schedule_metadata=schedule_metadata),
    )
    graph = _driver_for_module(module).graph
    tags = graph.nodes["out_beta"].tags
    assert tags["generation_index"] == "1"
    assert tags["generation_order"] == "0"
    assert tags["generation_size"] == "1"
