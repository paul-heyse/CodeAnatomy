"""Tests for run summary telemetry aggregation."""

from __future__ import annotations

import logging
from pathlib import Path

import msgspec
import pytest
from tools.cq.cli_app.context import CliContext
from tools.cq.core.schema import CqResult, RunMeta
from tools.cq.core.summary_types import SemanticTelemetryV1, summary_from_mapping
from tools.cq.run.run_summary import populate_run_summary_metadata
from tools.cq.run.runner import execute_run_plan
from tools.cq.run.spec import QStep, RunPlan

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
    merged = msgspec.structs.replace(
        merged,
        summary=summary_from_mapping(
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
            },
        ),
    )

    merged = populate_run_summary_metadata(merged, executed_results=[], total_steps=2)

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


def test_runner_logs_immediate_q_step_errors(
    caplog: pytest.LogCaptureFixture, tmp_path: Path
) -> None:
    """Immediate q-step errors emit warning logs with step id and error text."""
    ctx = CliContext.build(argv=["cq", "run"], root=tmp_path)
    plan = RunPlan(
        in_dir="missing_dir",
        steps=(QStep(id="q_fail_0", query="entity=function name=foo"),),
    )
    with caplog.at_level(logging.WARNING):
        execute_run_plan(plan, ctx)
    messages = [record.getMessage() for record in caplog.records]
    assert any("Immediate q-step error step_id=q_fail_0" in msg for msg in messages)
