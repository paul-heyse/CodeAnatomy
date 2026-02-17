"""Unit tests for DataFusion run envelopes."""

from __future__ import annotations

from datafusion_engine.obs.datafusion_runs import create_run_context, finish_run, start_run


def test_start_and_finish_run_lifecycle() -> None:
    """Run lifecycle helpers transition status and completion timestamps."""
    run = start_run(label="unit-test")
    assert run.status == "running"
    assert run.end_time_unix_ms is None

    finished = finish_run(run, status="completed")
    assert finished.status == "completed"
    assert finished.end_time_unix_ms is not None
    assert finished.end_time_unix_ms >= finished.start_time_unix_ms


def test_create_run_context_and_commit_sequence() -> None:
    """Run context issues deterministic idempotent commit sequencing."""
    run = create_run_context(label="delta-commit")
    options, next_run = run.next_commit_version()
    assert options.app_id == run.run_id
    assert options.version == 0
    assert next_run.commit_sequence == 1
