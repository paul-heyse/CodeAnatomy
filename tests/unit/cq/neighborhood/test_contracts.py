"""Tests for neighborhood contracts and capability helpers."""

from __future__ import annotations

from tools.cq.neighborhood.contracts import (
    TreeSitterNeighborhoodCollectRequest,
    TreeSitterNeighborhoodCollectResult,
    normalize_capability_snapshot,
    plan_feasible_slices,
)

DEFAULT_MAX_PER_SLICE = 50
EXPECTED_UNAVAILABLE_DEGRADES = 2


def test_collect_request_defaults() -> None:
    """Test collect request defaults."""
    request = TreeSitterNeighborhoodCollectRequest(
        root=".",
        target_name="foo",
        target_file="src/foo.py",
    )
    assert request.language == "python"
    assert request.target_line is None
    assert request.target_col is None
    assert request.max_per_slice == DEFAULT_MAX_PER_SLICE
    assert request.slice_limits is None


def test_collect_result_defaults() -> None:
    """Test collect result defaults."""
    result = TreeSitterNeighborhoodCollectResult()
    assert result.subject is None
    assert result.slices == ()
    assert result.diagnostics == ()
    assert result.structural_export is None
    assert result.cst_tokens == ()
    assert result.cst_diagnostics == ()
    assert result.cst_query_hits == ()


def test_normalize_capability_snapshot_bool_coercion() -> None:
    """Test normalize capability snapshot bool coercion."""
    snapshot = normalize_capability_snapshot({"parents": 1, "children": 0, "siblings": ""})
    assert snapshot == {"parents": True, "children": False, "siblings": False}


def test_plan_feasible_slices_fail_open_with_degrades() -> None:
    """Test plan feasible slices fail open with degrades."""
    requested = ("parents", "children", "siblings")
    feasible, degrades = plan_feasible_slices(
        requested_slices=requested,
        capabilities={"parents": True, "children": False},
        stage="test.stage",
    )
    assert feasible == ("parents",)
    assert len(degrades) == EXPECTED_UNAVAILABLE_DEGRADES
    assert all(row.stage == "test.stage" for row in degrades)
    assert {row.category for row in degrades} == {"unavailable"}
