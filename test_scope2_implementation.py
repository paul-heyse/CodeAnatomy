"""Simple test to verify Scope 2 implementation."""

import pytest

from datafusion_engine.compile_options import DataFusionCompileOptions
from datafusion_engine.schema_registry import has_schema
from obs.datafusion_runs import finish_run, start_run, tracked_run
from obs.diagnostics import DiagnosticsCollector
from obs.diagnostics_tables import datafusion_plan_artifacts_table, datafusion_runs_table


def test_imports() -> None:
    """Test that all new modules can be imported."""
    assert has_schema("datafusion_runs_v1")
    assert has_schema("datafusion_plan_artifacts_v1")

    opts = DataFusionCompileOptions(run_id="test-run-123")
    assert opts.run_id == "test-run-123"


def test_run_lifecycle() -> None:
    """Test the run lifecycle functions."""
    sink = DiagnosticsCollector()
    run = start_run(label="test_run", sink=sink, metadata={"test": "value"})

    assert run.run_id is not None
    assert run.label == "test_run"
    assert run.status == "running"
    assert run.metadata["test"] == "value"

    finish_run(run, sink=sink, status="completed")
    assert run.status == "completed"
    assert run.end_time_unix_ms is not None

    # Check artifacts were recorded
    artifacts = sink.artifacts_snapshot()
    assert "datafusion_run_started_v1" in artifacts
    assert "datafusion_run_finished_v1" in artifacts


def _run_tracked_error(*, sink: DiagnosticsCollector) -> None:
    error_msg = "test error"
    with tracked_run(label="error_test", sink=sink) as run:
        assert run.status == "running"
        raise ValueError(error_msg)


def test_tracked_run_context() -> None:
    """Test the tracked_run context manager."""
    sink = DiagnosticsCollector()

    with tracked_run(label="context_test", sink=sink) as run:
        assert run.status == "running"

    assert run.status == "completed"

    # Test error handling
    with pytest.raises(ValueError, match="test error"):
        _run_tracked_error(sink=sink)
    assert run.status == "completed"


def test_table_builders() -> None:
    """Test diagnostics table builders."""
    # Test runs table
    runs_data = [
        {
            "run_id": "test-1",
            "label": "query",
            "start_time_unix_ms": 1000,
            "end_time_unix_ms": 2000,
            "status": "completed",
            "duration_ms": 1000,
            "metadata": {"key": "value"},
        }
    ]
    runs_table = datafusion_runs_table(runs_data)
    assert runs_table.num_rows == 1

    # Test plan artifacts table
    artifacts_data = [
        {
            "run_id": "test-1",
            "plan_hash": "abc123",
            "sql": "SELECT 1",
            "logical_plan": "LogicalPlan(...)",
        }
    ]
    artifacts_table = datafusion_plan_artifacts_table(artifacts_data)
    assert artifacts_table.num_rows == 1
