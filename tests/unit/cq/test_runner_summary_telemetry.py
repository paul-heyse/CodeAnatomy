"""Tests for run summary telemetry aggregation."""

from __future__ import annotations

from tools.cq.core.schema import CqResult, RunMeta
from tools.cq.core.summary_contract import SemanticTelemetryV1, summary_from_mapping
from tools.cq.run.run_summary import populate_run_summary_metadata

EXPECTED_PYTHON_ATTEMPTED = 3
EXPECTED_PYTHON_APPLIED = 2
EXPECTED_RUST_ATTEMPTED = 4
EXPECTED_RUST_APPLIED = 2
EXPECTED_RUST_FAILED = 2


def test_populate_run_summary_aggregates_step_semantic_telemetry() -> None:
    """Test populate run summary aggregates step semantic telemetry."""
    run = RunMeta(
        macro="run",
        argv=["cq", "run"],
        root=".",
        started_ms=0.0,
        elapsed_ms=1.0,
        toolchain={},
    )
    merged = CqResult(run=run)
    merged.summary = summary_from_mapping(
        {
            "mode": "run",
            "query": "multi-step plan (2 steps)",
            "steps": ["search_0", "q_1"],
            "step_summaries": {
                "search_0": {
                    "python_semantic_telemetry": {
                        "attempted": 2,
                        "applied": 1,
                        "failed": 1,
                        "skipped": 0,
                        "timed_out": 0,
                    },
                    "rust_semantic_telemetry": {
                        "attempted": 1,
                        "applied": 1,
                        "failed": 0,
                        "skipped": 0,
                        "timed_out": 0,
                    },
                    "semantic_planes": {"semantic_tokens_count": 6},
                },
                "q_1": {
                    "python_semantic_telemetry": {
                        "attempted": 1,
                        "applied": 1,
                        "failed": 0,
                        "skipped": 0,
                        "timed_out": 1,
                    },
                    "rust_semantic_telemetry": {
                        "attempted": 3,
                        "applied": 1,
                        "failed": 2,
                        "skipped": 0,
                        "timed_out": 1,
                    },
                    "semantic_planes": {"semantic_tokens_count": 2, "inlay_hints_count": 1},
                },
            },
        }
    )

    populate_run_summary_metadata(merged, executed_results=[], total_steps=2)

    python_telemetry = merged.summary["python_semantic_telemetry"]
    rust_telemetry = merged.summary["rust_semantic_telemetry"]
    assert isinstance(python_telemetry, SemanticTelemetryV1)
    assert isinstance(rust_telemetry, SemanticTelemetryV1)
    assert python_telemetry.attempted == EXPECTED_PYTHON_ATTEMPTED
    assert python_telemetry.applied == EXPECTED_PYTHON_APPLIED
    assert python_telemetry.failed == 1
    assert python_telemetry.skipped == 0
    assert python_telemetry.timed_out == 1
    assert rust_telemetry.attempted == EXPECTED_RUST_ATTEMPTED
    assert rust_telemetry.applied == EXPECTED_RUST_APPLIED
    assert rust_telemetry.failed == EXPECTED_RUST_FAILED
    assert rust_telemetry.skipped == 0
    assert rust_telemetry.timed_out == 1
    assert merged.summary["semantic_planes"] == {"semantic_tokens_count": 6}
