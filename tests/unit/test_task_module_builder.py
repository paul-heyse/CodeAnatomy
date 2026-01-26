"""Unit tests for dynamic Hamilton task module generation."""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING, cast

from hamilton import driver

from datafusion_engine.view_graph_registry import ViewNode
from hamilton_pipeline.task_module_builder import (
    TaskExecutionModuleOptions,
    build_task_execution_module,
)
from relspec.schedule_events import TaskScheduleMetadata

if TYPE_CHECKING:
    from collections.abc import Callable

    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame


def _dummy_view_node(name: str) -> ViewNode:
    def _build_view(_ctx: object) -> object:
        msg = "view build should not be invoked during module generation"
        raise RuntimeError(msg)

    builder = cast("Callable[[SessionContext], DataFrame]", _build_view)
    return ViewNode(name=name, deps=(), builder=builder)


def _driver_for_module(module: ModuleType) -> driver.Driver:
    return driver.Builder().with_modules(module).build()


def test_task_module_builder_wires_dependencies() -> None:
    """Ensure dependency mapping creates upstream edges in the graph."""
    view_nodes = (
        _dummy_view_node("out_alpha"),
        _dummy_view_node("out_beta"),
    )
    module = build_task_execution_module(
        dependency_map={"out_beta": ("out_alpha",)},
        view_nodes=view_nodes,
    )
    graph = _driver_for_module(module).graph
    node = graph.nodes["out_beta"]
    deps = {dep.name for dep in node.dependencies}
    assert "out_alpha" in deps


def test_task_module_builder_applies_schedule_tags() -> None:
    """Ensure schedule metadata is propagated to task node tags."""
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
    module = build_task_execution_module(
        dependency_map={"out_beta": ("out_alpha",)},
        view_nodes=view_nodes,
        options=TaskExecutionModuleOptions(schedule_metadata=schedule_metadata),
    )
    graph = _driver_for_module(module).graph
    tags = graph.nodes["out_beta"].tags
    assert tags["generation_index"] == "1"
    assert tags["generation_order"] == "0"
    assert tags["generation_size"] == "1"
