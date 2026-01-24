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


def test_allowed_task_names_from_diff() -> None:
    """Allow changed tasks and their task ancestors/descendants."""
    task_graph = _task_graph()
    diff = IncrementalDiff(changed_tasks=("task.beta",))
    allowed = task_execution.allowed_task_names(task_graph, diff)
    assert allowed is not None
    assert "task.alpha" in allowed
    assert "task.beta" in allowed


def test_allowed_task_names_none_when_empty() -> None:
    """Return None when there are no changed or added tasks."""
    task_graph = _task_graph()
    diff = IncrementalDiff(changed_tasks=())
    assert task_execution.allowed_task_names(task_graph, diff) is None


def test_allowed_task_names_none_when_diff_missing() -> None:
    """Return None when diff is not provided."""
    task_graph = _task_graph()
    assert task_execution.allowed_task_names(task_graph, None) is None
