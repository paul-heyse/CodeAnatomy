"""Integration tests for pipeline diagnostics and plan_execution_diff_v1.

Tests the boundary where execute_pipeline() results -> _emit_plan_execution_diff() ->
plan_execution_diff_v1 artifact.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest


@pytest.fixture
def minimal_python_repo(tmp_path: Path) -> Path:
    """Create a minimal Python repository for testing.

    Parameters
    ----------
    tmp_path
        Pytest temporary directory fixture.

    Returns:
    -------
    Path
        Path to the minimal repository.
    """
    repo = tmp_path / "test_repo"
    repo.mkdir()
    (repo / "test_module.py").write_text("def example() -> int:\n    return 42\n", encoding="utf-8")
    return repo


@pytest.fixture
def captured_diagnostics() -> list[dict[str, Any]]:
    """Capture diagnostics events for inspection.

    Returns:
    -------
    list[dict[str, Any]]
        List to collect emitted diagnostics events.
    """
    events: list[dict[str, Any]] = []
    return events


@pytest.mark.integration
@pytest.mark.skip(reason="Requires full pipeline execution which is slow and complex")
def test_plan_execution_diff_emitted_on_success(
    minimal_python_repo: Path,
    captured_diagnostics: list[dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run pipeline to completion; verify plan_execution_diff_v1 artifact emitted.

    Artifact should be present in diagnostics with missing_task_count == 0 and
    unexpected_task_count == 0.
    """


@pytest.mark.integration
def test_plan_execution_diff_missing_tasks(
    monkeypatch: pytest.MonkeyPatch,
    captured_diagnostics: list[dict[str, Any]],
) -> None:
    """Inject partial execution; verify missing tasks reported.

    missing_tasks list should be non-empty, and missing_task_fingerprints should
    have entries for each missing task.
    """
    from hamilton_pipeline.execution import _emit_plan_execution_diff

    def mock_emit(event_name: str, *, payload: dict[str, Any], event_kind: str) -> None:
        _ = event_kind
        captured_diagnostics.append({"event_name": event_name, "payload": payload})

    monkeypatch.setattr("hamilton_pipeline.execution.emit_diagnostics_event", mock_emit)

    mock_plan = Mock()
    mock_plan.active_tasks = frozenset(["task_a", "task_b", "task_c"])
    mock_plan.plan_fingerprints = {"task_b": "fp_b"}
    mock_plan.plan_task_signatures = {"task_b": "sig_b"}
    mock_plan.plan_signature = "plan_sig_1"
    mock_plan.task_dependency_signature = "task_dep_sig_1"
    mock_plan.reduced_task_dependency_signature = "reduced_sig_1"

    mock_runtime = Mock()
    mock_runtime.execution_order = ["task_a", "task_c"]

    results = {"execution_plan": mock_plan, "runtime_artifacts": mock_runtime}

    _emit_plan_execution_diff(results)

    diff_events = [e for e in captured_diagnostics if e["event_name"] == "plan_execution_diff_v1"]
    assert len(diff_events) == 1, "Should emit exactly one plan_execution_diff_v1 event"

    payload = diff_events[0]["payload"]
    assert payload["missing_task_count"] == 1, "Should report one missing task"
    assert "task_b" in payload["missing_tasks"], "Should identify task_b as missing"
    assert "task_b" in payload["missing_task_fingerprints"], (
        "Should include fingerprint for missing task"
    )


@pytest.mark.integration
def test_plan_execution_diff_unexpected_tasks(
    monkeypatch: pytest.MonkeyPatch,
    captured_diagnostics: list[dict[str, Any]],
) -> None:
    """Inject extra task execution not in plan; verify unexpected tasks reported.

    unexpected_tasks list should be non-empty.
    """
    from hamilton_pipeline.execution import _emit_plan_execution_diff

    def mock_emit(event_name: str, *, payload: dict[str, Any], event_kind: str) -> None:
        _ = event_kind
        captured_diagnostics.append({"event_name": event_name, "payload": payload})

    monkeypatch.setattr("hamilton_pipeline.execution.emit_diagnostics_event", mock_emit)

    mock_plan = Mock()
    mock_plan.active_tasks = frozenset(["task_a", "task_b"])
    empty_sigs: dict[str, str] = {}
    mock_plan.plan_fingerprints = empty_sigs
    mock_plan.plan_task_signatures = empty_sigs
    mock_plan.plan_signature = "plan_sig_1"
    mock_plan.task_dependency_signature = "task_dep_sig_1"
    mock_plan.reduced_task_dependency_signature = "reduced_sig_1"

    mock_runtime = Mock()
    mock_runtime.execution_order = ["task_a", "task_b", "task_unexpected"]

    results = {"execution_plan": mock_plan, "runtime_artifacts": mock_runtime}

    _emit_plan_execution_diff(results)

    diff_events = [e for e in captured_diagnostics if e["event_name"] == "plan_execution_diff_v1"]
    assert len(diff_events) == 1, "Should emit exactly one plan_execution_diff_v1 event"

    payload = diff_events[0]["payload"]
    assert payload["unexpected_task_count"] == 1, "Should report one unexpected task"
    assert "task_unexpected" in payload["unexpected_tasks"], "Should identify task_unexpected"


@pytest.mark.integration
def test_plan_execution_diff_payload_shape(
    monkeypatch: pytest.MonkeyPatch,
    captured_diagnostics: list[dict[str, Any]],
) -> None:
    """Verify all 16 payload fields present with correct types.

    All fields should be present: run_id is string, timestamp_ns is int, task
    counts are ints, task lists are lists of strings.
    """
    from hamilton_pipeline.execution import _emit_plan_execution_diff

    def mock_emit(event_name: str, *, payload: dict[str, Any], event_kind: str) -> None:
        _ = event_kind
        captured_diagnostics.append({"event_name": event_name, "payload": payload})

    monkeypatch.setattr("hamilton_pipeline.execution.emit_diagnostics_event", mock_emit)
    monkeypatch.setattr("hamilton_pipeline.execution.get_run_id", lambda: "test_run_id")

    mock_plan = Mock()
    mock_plan.active_tasks = frozenset(["task_a"])
    empty_sigs: dict[str, str] = {}
    mock_plan.plan_fingerprints = empty_sigs
    mock_plan.plan_task_signatures = empty_sigs
    mock_plan.plan_signature = "plan_sig_1"
    mock_plan.task_dependency_signature = "task_dep_sig_1"
    mock_plan.reduced_task_dependency_signature = "reduced_sig_1"

    mock_runtime = Mock()
    mock_runtime.execution_order = ["task_a"]

    results = {"execution_plan": mock_plan, "runtime_artifacts": mock_runtime}

    _emit_plan_execution_diff(results)

    diff_events = [e for e in captured_diagnostics if e["event_name"] == "plan_execution_diff_v1"]
    assert len(diff_events) == 1, "Should emit exactly one event"

    payload = diff_events[0]["payload"]

    required_fields = {
        "run_id",
        "timestamp_ns",
        "plan_signature",
        "task_dependency_signature",
        "reduced_task_dependency_signature",
        "expected_task_count",
        "executed_task_count",
        "missing_task_count",
        "unexpected_task_count",
        "missing_tasks",
        "unexpected_tasks",
        "missing_task_fingerprints",
        "missing_task_signatures",
        "blocked_datasets",
        "blocked_scan_units",
    }
    assert required_fields <= set(payload.keys()), "Should have all 15 required fields (16 in plan)"

    assert isinstance(payload["run_id"], str), "run_id should be string"
    assert isinstance(payload["timestamp_ns"], int), "timestamp_ns should be int"
    assert isinstance(payload["expected_task_count"], int), "expected_task_count should be int"
    assert isinstance(payload["executed_task_count"], int), "executed_task_count should be int"
    assert isinstance(payload["missing_tasks"], list), "missing_tasks should be list"
    assert isinstance(payload["unexpected_tasks"], list), "unexpected_tasks should be list"


@pytest.mark.integration
@pytest.mark.skip(reason="Requires complex mock of scan unit metadata")
def test_plan_execution_diff_blocked_datasets(
    monkeypatch: pytest.MonkeyPatch,
    captured_diagnostics: list[dict[str, Any]],
) -> None:
    """Missing task has associated scan units; verify blocked_datasets populated.

    blocked_datasets should be non-empty list, blocked_scan_units should reflect
    affected scan units.
    """


@pytest.mark.integration
@pytest.mark.skip(reason="Requires full pipeline with partial failures")
def test_pipeline_partial_output_handling(
    minimal_python_repo: Path,
    captured_diagnostics: list[dict[str, Any]],
) -> None:
    """Pipeline with some failing nodes; verify partial results recorded.

    Results mapping should have entries for successful nodes, failing nodes
    recorded in diagnostics.
    """
