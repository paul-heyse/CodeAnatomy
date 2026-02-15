"""Tests for neighborhood contracts and capability helpers."""

from __future__ import annotations

from tools.cq.neighborhood.contracts import (
    TreeSitterNeighborhoodCollectRequest,
    TreeSitterNeighborhoodCollectResult,
    normalize_capability_snapshot,
    plan_feasible_slices,
)


def test_collect_request_defaults() -> None:
    request = TreeSitterNeighborhoodCollectRequest(
        root=".",
        target_name="foo",
        target_file="src/foo.py",
    )
    assert request.language == "python"
    assert request.target_line is None
    assert request.target_col is None
    assert request.max_per_slice == 50
    assert request.slice_limits is None


def test_collect_result_defaults() -> None:
    result = TreeSitterNeighborhoodCollectResult()
    assert result.subject is None
    assert result.slices == ()
    assert result.diagnostics == ()
    assert result.structural_export is None
    assert result.cst_tokens == ()
    assert result.cst_diagnostics == ()
    assert result.cst_query_hits == ()


def test_normalize_capability_snapshot_bool_coercion() -> None:
    snapshot = normalize_capability_snapshot({"parents": 1, "children": 0, "siblings": ""})
    assert snapshot == {"parents": True, "children": False, "siblings": False}


def test_plan_feasible_slices_fail_open_with_degrades() -> None:
    requested = ("parents", "children", "siblings")
    feasible, degrades = plan_feasible_slices(
        requested_slices=requested,
        capabilities={"parents": True, "children": False},
        stage="test.stage",
    )
    assert feasible == ("parents",)
    assert len(degrades) == 2
    assert all(row.stage == "test.stage" for row in degrades)
    assert {row.category for row in degrades} == {"unavailable"}
