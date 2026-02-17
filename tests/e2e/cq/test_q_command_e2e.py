"""Command-level e2e tests for `cq q` on hermetic golden workspaces."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from tools.cq.core.schema import CqResult
from tools.cq.core.summary_contract import SemanticTelemetryV1

from tests.e2e.cq._support.goldens import assert_json_snapshot_data, load_golden_spec
from tests.e2e.cq._support.projections import result_snapshot_projection
from tests.e2e.cq._support.specs import assert_result_matches_spec


@pytest.mark.e2e
def test_q_python_resolve_entities_golden(
    run_cq_result: Callable[..., CqResult],
    *,
    update_golden: bool,
) -> None:
    """Test q python resolve entities golden."""
    result = run_cq_result(
        [
            "q",
            "entity=function name=resolve in=tests/e2e/cq/_golden_workspace/python_project/app",
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )

    spec = load_golden_spec("golden_specs/q_python_resolve_spec.json")
    assert_result_matches_spec(result, spec)
    insight = result.summary.get("front_door_insight")
    assert isinstance(insight, dict)
    degradation = insight.get("degradation")
    assert isinstance(degradation, dict)
    assert degradation.get("scope_filter") == "partial"
    notes = degradation.get("notes")
    assert isinstance(notes, list)
    assert any("missing_languages=rust" in str(note) for note in notes)
    assert_json_snapshot_data(
        "q_python_resolve.json",
        result_snapshot_projection(result),
        update=update_golden,
    )


@pytest.mark.e2e
def test_q_rust_compile_target_golden(
    run_cq_result: Callable[..., CqResult],
    *,
    update_golden: bool,
) -> None:
    """Test q rust compile target golden."""
    result = run_cq_result(
        [
            "q",
            (
                "entity=function name=compile_target lang=rust "
                "in=tests/e2e/cq/_golden_workspace/rust_workspace"
            ),
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )

    spec = load_golden_spec("golden_specs/q_rust_compile_target_spec.json")
    assert_result_matches_spec(result, spec)
    insight = result.summary.get("front_door_insight")
    assert isinstance(insight, dict)
    rust_semantic_telemetry = result.summary.get("rust_semantic_telemetry")
    assert isinstance(rust_semantic_telemetry, SemanticTelemetryV1)
    neighborhood = insight.get("neighborhood")
    assert isinstance(neighborhood, dict)
    callers = neighborhood.get("callers")
    assert isinstance(callers, dict)
    assert callers.get("availability") in {"partial", "full", "unavailable"}
    assert_json_snapshot_data(
        "q_rust_compile_target.json",
        result_snapshot_projection(result),
        update=update_golden,
    )


@pytest.mark.e2e
def test_q_pattern_query_excludes_front_door_insight(
    run_cq_result: Callable[..., CqResult],
) -> None:
    """Test q pattern query excludes front door insight."""
    result = run_cq_result(
        [
            "q",
            "pattern='getattr($X, $Y)' lang=auto",
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )
    assert result.summary.get("mode") == "pattern"
    assert result.summary.front_door_insight is None


@pytest.mark.e2e
def test_q_top_level_any_composite_executes(
    run_cq_result: Callable[..., CqResult],
) -> None:
    """Test q top level any composite executes."""
    result = run_cq_result(
        [
            "q",
            (
                "any='def $F($$$),class $C' "
                "in=tests/e2e/cq/_golden_workspace/python_project/app "
                "lang=python"
            ),
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )
    assert result.summary.get("mode") == "pattern"
    assert isinstance(result.summary.get("matches"), int)
