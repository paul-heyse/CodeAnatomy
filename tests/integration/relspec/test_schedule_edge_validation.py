"""Edge validation in scheduling context integration tests.

Scope: Validate validate_edge_requirements() behavior when used within
schedule_tasks() to filter ready tasks. Tests exercise selective column
requirements, catalog mutation unblocking, and validation summary reflection.

NOTE: These tests work with the automatic evidence registration behavior
of schedule_tasks(), which registers output evidence nodes as tasks complete.
The validation_summary field is the primary contract for reporting validation
failures.

Key API references:
- validate_edge_requirements() in relspec.graph_edge_validation (line 114)
- EvidenceCatalog in relspec.evidence (line 35)
- TaskSchedule in relspec.rustworkx_schedule (line 28)
- schedule_tasks() in relspec.rustworkx_schedule (line 60)
- GraphValidationSummary in relspec.graph_edge_validation (line 84)
"""

from __future__ import annotations

import pytest

from relspec.evidence import EvidenceCatalog
from relspec.inferred_deps import InferredDeps
from relspec.rustworkx_graph import build_task_graph_from_inferred_deps
from relspec.rustworkx_schedule import ScheduleOptions, schedule_tasks


@pytest.mark.integration
def test_schedule_with_selective_column_requirements() -> None:
    """Task with available columns schedules; task missing columns does not.

    With automatic output-evidence registration disabled, internal dependency
    requirements must already be satisfied by the evidence catalog. This test
    verifies that behavior is deterministic and that enabling auto-registration
    restores the legacy scheduling behavior.
    """
    deps = (
        InferredDeps(
            task_name="task_a",
            output="ev_a",
            inputs=(),
            plan_fingerprint="fp-a",
        ),
        InferredDeps(
            task_name="task_b",
            output="ev_b",
            inputs=("ev_a",),
            required_columns={"ev_a": ("required_col",)},
            plan_fingerprint="fp-b",
        ),
    )
    graph = build_task_graph_from_inferred_deps(deps)

    baseline = schedule_tasks(
        graph,
        evidence=EvidenceCatalog(),
        options=ScheduleOptions(
            allow_partial=True,
            auto_register_output_evidence=True,
        ),
    )
    assert "task_b" in baseline.ordered_tasks

    strict = schedule_tasks(
        graph,
        evidence=EvidenceCatalog(),
        options=ScheduleOptions(
            allow_partial=True,
            auto_register_output_evidence=False,
        ),
    )
    assert "task_a" in strict.ordered_tasks
    assert "task_b" not in strict.ordered_tasks
    assert "task_b" in strict.missing_tasks


@pytest.mark.integration
def test_catalog_mutation_unblocks_dependent() -> None:
    """After catalog update, previously blocked task becomes schedulable.

    Setup:
    - task_a produces ev_a (no dependencies)
    - task_b requires ev_a (depends on task_a output)

    Wave 1: empty catalog means only task_a is truly schedulable
    (task_b needs ev_a which isn't in the catalog yet).

    Wave 2: catalog updated with ev_a, task_b now schedulable.

    This demonstrates the catalog mutation -> unblocking flow that
    schedule_tasks() uses internally.
    """
    deps = (
        InferredDeps(
            task_name="task_a",
            output="ev_a",
            inputs=(),
            plan_fingerprint="fp-a",
        ),
        InferredDeps(
            task_name="task_b",
            output="ev_b",
            inputs=("ev_a",),
            plan_fingerprint="fp-b",
        ),
    )
    graph = build_task_graph_from_inferred_deps(deps)

    # Wave 1: empty catalog
    catalog_wave1 = EvidenceCatalog()
    schedule_wave1 = schedule_tasks(
        graph,
        evidence=catalog_wave1,
        options=ScheduleOptions(allow_partial=True),
    )

    # task_a is always schedulable (no deps)
    assert "task_a" in schedule_wave1.ordered_tasks

    # Wave 2: catalog updated with ev_a (simulating task_a completion)
    catalog_wave2 = EvidenceCatalog()
    catalog_wave2.sources.add("ev_a")
    catalog_wave2.columns_by_dataset["ev_a"] = {"some_col"}

    schedule_wave2 = schedule_tasks(
        graph,
        evidence=catalog_wave2,
        options=ScheduleOptions(allow_partial=True),
    )

    # Now both tasks are schedulable
    assert "task_a" in schedule_wave2.ordered_tasks
    assert "task_b" in schedule_wave2.ordered_tasks
    assert "task_b" not in schedule_wave2.missing_tasks


@pytest.mark.integration
def test_validation_summary_reflects_blocked_edges() -> None:
    """Validation summary reports edge validation state post-schedule.

    Setup:
    - 3 tasks in dependency chain
    - All tasks schedule (evidence nodes registered automatically)
    - validation_summary captures contract/metadata validation state

    Expected:
    - validation_summary present and non-None
    - summary.task_results has entry for each task
    - invalid_tasks count reflects validation failures
    - task_results entries show edge-level validation details
    """
    deps = (
        InferredDeps(
            task_name="task_a",
            output="ev_a",
            inputs=(),
            plan_fingerprint="fp-a",
        ),
        InferredDeps(
            task_name="task_b",
            output="ev_b",
            inputs=("ev_a",),
            plan_fingerprint="fp-b",
        ),
        InferredDeps(
            task_name="task_c",
            output="ev_c",
            inputs=("ev_b",),
            plan_fingerprint="fp-c",
        ),
    )
    graph = build_task_graph_from_inferred_deps(deps)

    # Empty catalog: schedule_tasks will register evidence automatically
    catalog = EvidenceCatalog()

    schedule = schedule_tasks(
        graph,
        evidence=catalog,
        options=ScheduleOptions(allow_partial=True),
    )

    # All tasks scheduled (evidence nodes registered during loop)
    assert "task_a" in schedule.ordered_tasks
    assert "task_b" in schedule.ordered_tasks
    assert "task_c" in schedule.ordered_tasks

    # Validation summary present
    assert schedule.validation_summary is not None
    summary = schedule.validation_summary

    # Summary has entries for all tasks
    assert summary.total_tasks == 3
    assert len(summary.task_results) == 3

    # Each task has validation result with edge details
    for task_result in summary.task_results:
        assert task_result.task_name in {"task_a", "task_b", "task_c"}
        # Each task has edges (input + output)
        assert len(task_result.edge_results) >= 1
        # Edge results have required fields
        for edge_result in task_result.edge_results:
            assert isinstance(edge_result.source_name, str)
            assert isinstance(edge_result.target_task, str)
            assert isinstance(edge_result.is_valid, bool)
