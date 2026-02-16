# ruff: noqa: FBT001
"""Command-level e2e tests for `cq run` over hermetic workspaces."""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

import pytest
from tools.cq.core.schema import CqResult

from tests.e2e.cq._support.goldens import assert_json_snapshot_data, load_golden_spec
from tests.e2e.cq._support.projections import result_snapshot_projection
from tests.e2e.cq._support.specs import assert_result_matches_spec

EXPECTED_RUN_STEPS = 2


@pytest.mark.e2e
def test_run_inline_q_and_search_steps_golden(
    run_cq_result: Callable[..., CqResult],
    update_golden: bool,
) -> None:
    """Test run inline q and search steps golden."""
    result = run_cq_result(
        [
            "run",
            "--steps",
            (
                "["
                '{"type":"q","query":"entity=function name=resolve '
                'in=tests/e2e/cq/_golden_workspace/python_project/app"},'
                '{"type":"search","query":"AsyncService",'
                '"in_dir":"tests/e2e/cq/_golden_workspace/python_project",'
                '"lang_scope":"python"}'
                "]"
            ),
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )

    spec = load_golden_spec("golden_specs/run_q_search_python_spec.json")
    assert_result_matches_spec(result, spec)
    steps = result.summary.get("steps")
    assert isinstance(steps, list)
    assert len(steps) == EXPECTED_RUN_STEPS

    assert_json_snapshot_data(
        "run_q_search_python.json",
        result_snapshot_projection(result),
        update=update_golden,
    )


@pytest.mark.e2e
def test_run_inline_neighborhood_step_golden(
    run_cq_result: Callable[..., CqResult],
    update_golden: bool,
) -> None:
    """Test run inline neighborhood step golden."""
    result = run_cq_result(
        [
            "run",
            "--step",
            (
                '{"type":"neighborhood",'
                '"target":"tests/e2e/cq/_golden_workspace/rust_workspace/crates/corelib/src/lib.rs:9",'
                '"lang":"rust","top_k":4,"no_semantic_enrichment":true}'
            ),
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )

    spec = load_golden_spec("golden_specs/run_neighborhood_rust_spec.json")
    assert_result_matches_spec(result, spec)
    steps = result.summary.get("steps")
    assert isinstance(steps, list)
    assert len(steps) == 1

    assert_json_snapshot_data(
        "run_neighborhood_rust.json",
        result_snapshot_projection(result),
        update=update_golden,
    )


@pytest.mark.e2e
def test_run_top_level_semantic_telemetry_matches_step_summaries(
    run_cq_result: Callable[..., CqResult],
) -> None:
    """Test run top level semantic telemetry matches step summaries."""
    result = run_cq_result(
        [
            "run",
            "--steps",
            (
                "["
                '{"type":"search","query":"AsyncService",'
                '"in_dir":"tests/e2e/cq/_golden_workspace/python_project",'
                '"lang_scope":"python"},'
                '{"type":"q","query":"entity=function name=resolve '
                'in=tests/e2e/cq/_golden_workspace/python_project/app"}'
                "]"
            ),
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )

    summary = result.summary
    step_summaries = summary.get("step_summaries")
    assert isinstance(step_summaries, dict)
    typed_step_summaries = cast("dict[str, object]", step_summaries)

    def sum_telemetry(key: str) -> dict[str, int]:
        totals = {"attempted": 0, "applied": 0, "failed": 0, "skipped": 0, "timed_out": 0}
        for step_summary in typed_step_summaries.values():
            if not isinstance(step_summary, dict):
                continue
            raw = step_summary.get(key)
            if not isinstance(raw, dict):
                continue
            for field in totals:
                value = raw.get(field)
                if isinstance(value, int):
                    totals[field] += value
        return totals

    assert summary.get("python_semantic_telemetry") == sum_telemetry("python_semantic_telemetry")
    assert summary.get("rust_semantic_telemetry") == sum_telemetry("rust_semantic_telemetry")
