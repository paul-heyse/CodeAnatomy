"""Tests for incremental plan diff gating."""

from __future__ import annotations

from hamilton_pipeline.modules import task_execution
from relspec.incremental import IncrementalDiff
from relspec.inferred_deps import InferredDeps
from relspec.rustworkx_graph import TaskGraph, build_task_graph_from_inferred_deps


def _task_graph() -> TaskGraph:
    deps_a = InferredDeps(
        task_name="task.alpha",
        output="out_alpha",
        inputs=("seed_input",),
        plan_fingerprint="fp-alpha",
    )
    deps_b = InferredDeps(
        task_name="task.beta",
        output="out_beta",
        inputs=("out_alpha",),
        plan_fingerprint="fp-beta",
    )
    return build_task_graph_from_inferred_deps((deps_a, deps_b))


def test_task_graph_for_diff_from_diff() -> None:
    """Include changed tasks and their task ancestors/descendants."""
    task_graph = _task_graph()
    diff = IncrementalDiff(changed_tasks=("task.beta",))
    impact_graph = task_execution.task_graph_for_diff(task_graph, diff)
    assert impact_graph is not task_graph
    assert set(impact_graph.task_idx) == {"task.alpha", "task.beta"}


def test_task_graph_for_diff_when_empty() -> None:
    """Return the original graph when there are no changed or added tasks."""
    task_graph = _task_graph()
    diff = IncrementalDiff(changed_tasks=())
    assert task_execution.task_graph_for_diff(task_graph, diff) is task_graph


def test_task_graph_for_diff_when_diff_missing() -> None:
    """Return the original graph when diff is not provided."""
    task_graph = _task_graph()
    assert task_execution.task_graph_for_diff(task_graph, None) is task_graph
