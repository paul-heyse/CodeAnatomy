"""Tests for extract planning helpers."""

from __future__ import annotations

from datafusion_engine.extract.bundles import dataset_name_for_output
from relspec.extract_plan import (
    extract_inferred_deps,
    extract_output_task_map,
    extract_task_kind_map,
)
from relspec.rustworkx_graph import (
    TaskGraphBuildOptions,
    build_task_graph_from_inferred_deps,
    task_graph_snapshot,
)


def test_extract_output_task_dependencies_include_python_imports() -> None:
    """Ensure python_external extract tasks depend on python_imports outputs."""
    task_map = extract_output_task_map()
    external_output = dataset_name_for_output("python_external_interfaces")
    python_imports_output = dataset_name_for_output("python_imports")
    assert external_output is not None
    assert python_imports_output is not None
    task = task_map[external_output]
    assert python_imports_output in task.required_inputs


def test_extract_tasks_marked_as_extract_kind_in_graph() -> None:
    """Ensure extract tasks are labeled with task_kind=extract in graph snapshots."""
    inferred = extract_inferred_deps()
    graph = build_task_graph_from_inferred_deps(
        inferred,
        options=TaskGraphBuildOptions(task_kinds=extract_task_kind_map()),
    )
    snapshot = task_graph_snapshot(graph, label="extract")
    task_nodes = [node for node in snapshot.nodes if node.get("kind") == "task"]
    assert task_nodes
    for node in task_nodes:
        assert node.get("task_kind") == "extract"
