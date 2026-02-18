"""Integration tests for run summary metadata over typed summary contracts."""

from __future__ import annotations

from tools.cq.core.schema import CqResult, mk_result, mk_runmeta, update_result_summary
from tools.cq.core.summary_types import SemanticTelemetryV1
from tools.cq.run.run_summary import populate_run_summary_metadata

EXPECTED_ATTEMPTED = 3
EXPECTED_APPLIED = 2


def _result(macro: str) -> CqResult:
    run = mk_runmeta(
        macro=macro,
        argv=[],
        root=".",
        started_ms=0.0,
        toolchain={},
        run_id="run-1",
    )
    return mk_result(run)


def test_populate_run_summary_aggregates_semantic_telemetry() -> None:
    """Run summary metadata aggregates semantic telemetry across steps."""
    merged = _result("run")
    step_a = _result("search")
    step_b = _result("q")

    step_a = update_result_summary(
        step_a,
        {
            "mode": "identifier",
            "query": "build_graph",
            "lang_scope": "python",
            "language_order": ["python"],
        },
    )

    step_b = update_result_summary(
        step_b,
        {
            "mode": "identifier",
            "query": "build_graph",
            "lang_scope": "python",
            "language_order": ["python"],
        },
    )

    merged = update_result_summary(
        merged,
        {
            "steps": ["a", "b"],
            "step_summaries": {
                "a": {
                    "python_semantic_telemetry": SemanticTelemetryV1(
                        attempted=2,
                        applied=1,
                        failed=1,
                    ),
                    "semantic_planes": {"python": {"ast": 1}},
                },
                "b": {
                    "python_semantic_telemetry": SemanticTelemetryV1(
                        attempted=1,
                        applied=1,
                        failed=0,
                    ),
                    "semantic_planes": {"python": {"ast": 2, "imports": 1}},
                },
            },
        },
    )

    merged = populate_run_summary_metadata(
        merged,
        [("a", step_a), ("b", step_b)],
        total_steps=2,
    )

    assert merged.summary.mode == "identifier"
    assert merged.summary.query == "build_graph"
    assert merged.summary.lang_scope == "python"
    assert merged.summary.python_semantic_telemetry is not None
    assert merged.summary.python_semantic_telemetry.attempted == EXPECTED_ATTEMPTED
    assert merged.summary.python_semantic_telemetry.applied == EXPECTED_APPLIED
    assert merged.summary.semantic_planes == {"python": {"ast": 2, "imports": 1}}
