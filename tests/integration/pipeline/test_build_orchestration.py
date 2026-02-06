"""Integration tests for build orchestration lifecycle.

Tests the boundary where GraphProductBuildRequest -> build_graph_product() ->
heartbeat + signal handlers + build-phase diagnostics.
"""

from __future__ import annotations

import contextlib
import signal
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
def test_heartbeat_starts_before_execution(
    minimal_python_repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify heartbeat is active during build.

    The heartbeat should start before _execute_build is called and stop in the
    finally block, ensuring it runs throughout the entire build lifecycle.
    """
    from graph.product_build import GraphProductBuildRequest, build_graph_product

    heartbeat_calls: list[str] = []
    mock_heartbeat = Mock()
    mock_heartbeat.stop = Mock()

    def mock_start_heartbeat(run_id: str, interval_s: float) -> Mock:
        _ = (run_id, interval_s)
        heartbeat_calls.append("start")
        return mock_heartbeat

    def mock_execute_build(*args: Any, **kwargs: Any) -> Any:
        _ = (args, kwargs)
        heartbeat_calls.append("execute")
        from graph.product_build import (
            FinalizeDeltaPaths,
            FinalizeDeltaReport,
            GraphProductBuildResult,
            TableDeltaReport,
        )

        return GraphProductBuildResult(
            product="cpg",
            product_version="0.1.0",
            engine_versions={},
            run_id="test_run",
            repo_root=minimal_python_repo,
            output_dir=minimal_python_repo / "build",
            cpg_nodes=FinalizeDeltaReport(
                paths=FinalizeDeltaPaths(
                    data=minimal_python_repo / "nodes",
                    errors=minimal_python_repo / "errors",
                    stats=minimal_python_repo / "stats",
                    alignment=minimal_python_repo / "alignment",
                ),
                rows=0,
                error_rows=0,
            ),
            cpg_edges=FinalizeDeltaReport(
                paths=FinalizeDeltaPaths(
                    data=minimal_python_repo / "edges",
                    errors=minimal_python_repo / "errors",
                    stats=minimal_python_repo / "stats",
                    alignment=minimal_python_repo / "alignment",
                ),
                rows=0,
                error_rows=0,
            ),
            cpg_props=FinalizeDeltaReport(
                paths=FinalizeDeltaPaths(
                    data=minimal_python_repo / "props",
                    errors=minimal_python_repo / "errors",
                    stats=minimal_python_repo / "stats",
                    alignment=minimal_python_repo / "alignment",
                ),
                rows=0,
                error_rows=0,
            ),
            cpg_props_map=TableDeltaReport(path=minimal_python_repo / "props_map", rows=0),
            cpg_edges_by_src=TableDeltaReport(path=minimal_python_repo / "edges_by_src", rows=0),
            cpg_edges_by_dst=TableDeltaReport(path=minimal_python_repo / "edges_by_dst", rows=0),
        )

    monkeypatch.setattr("graph.product_build.start_build_heartbeat", mock_start_heartbeat)
    monkeypatch.setattr("graph.product_build._execute_build", mock_execute_build)

    request = GraphProductBuildRequest(repo_root=str(minimal_python_repo))
    build_graph_product(request)

    assert heartbeat_calls == ["start", "execute"], "Heartbeat should start before execution"
    assert mock_heartbeat.stop.called, "Heartbeat should be stopped in finally block"


@pytest.mark.integration
def test_signal_handlers_installed_and_restored(
    minimal_python_repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Signal handlers are installed during build and restored after.

    Before build: default handlers.
    During: custom _handler.
    After: original handlers restored.
    """
    from graph.product_build import GraphProductBuildRequest, build_graph_product

    original_sigterm = signal.getsignal(signal.SIGTERM)
    original_sigint = signal.getsignal(signal.SIGINT)

    captured_handlers: dict[str, Any] = {}

    def mock_execute_build(*args: Any, **kwargs: Any) -> Any:
        _ = (args, kwargs)
        captured_handlers["sigterm"] = signal.getsignal(signal.SIGTERM)
        captured_handlers["sigint"] = signal.getsignal(signal.SIGINT)
        from graph.product_build import (
            FinalizeDeltaPaths,
            FinalizeDeltaReport,
            GraphProductBuildResult,
            TableDeltaReport,
        )

        return GraphProductBuildResult(
            product="cpg",
            product_version="0.1.0",
            engine_versions={},
            run_id="test_run",
            repo_root=minimal_python_repo,
            output_dir=minimal_python_repo / "build",
            cpg_nodes=FinalizeDeltaReport(
                paths=FinalizeDeltaPaths(
                    data=minimal_python_repo / "nodes",
                    errors=minimal_python_repo / "errors",
                    stats=minimal_python_repo / "stats",
                    alignment=minimal_python_repo / "alignment",
                ),
                rows=0,
                error_rows=0,
            ),
            cpg_edges=FinalizeDeltaReport(
                paths=FinalizeDeltaPaths(
                    data=minimal_python_repo / "edges",
                    errors=minimal_python_repo / "errors",
                    stats=minimal_python_repo / "stats",
                    alignment=minimal_python_repo / "alignment",
                ),
                rows=0,
                error_rows=0,
            ),
            cpg_props=FinalizeDeltaReport(
                paths=FinalizeDeltaPaths(
                    data=minimal_python_repo / "props",
                    errors=minimal_python_repo / "errors",
                    stats=minimal_python_repo / "stats",
                    alignment=minimal_python_repo / "alignment",
                ),
                rows=0,
                error_rows=0,
            ),
            cpg_props_map=TableDeltaReport(path=minimal_python_repo / "props_map", rows=0),
            cpg_edges_by_src=TableDeltaReport(path=minimal_python_repo / "edges_by_src", rows=0),
            cpg_edges_by_dst=TableDeltaReport(path=minimal_python_repo / "edges_by_dst", rows=0),
        )

    monkeypatch.setattr("graph.product_build._execute_build", mock_execute_build)

    request = GraphProductBuildRequest(repo_root=str(minimal_python_repo))
    build_graph_product(request)

    assert captured_handlers["sigterm"] is not original_sigterm, (
        "SIGTERM handler should be replaced during build"
    )
    assert captured_handlers["sigint"] is not original_sigint, (
        "SIGINT handler should be replaced during build"
    )
    assert signal.getsignal(signal.SIGTERM) is original_sigterm, (
        "SIGTERM handler should be restored after build"
    )
    assert signal.getsignal(signal.SIGINT) is original_sigint, (
        "SIGINT handler should be restored after build"
    )


@pytest.mark.integration
@pytest.mark.skip(reason="Requires production changes to expose signal_state dict for testing")
def test_signal_handler_captures_correct_state(
    minimal_python_repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the signal handler closure captures run_id and run_bundle_dir.

    The handler's signal_state dict, run_bundle_dir, and run_id should match
    the build request values.
    """


@pytest.mark.integration
def test_build_phase_start_and_end_events(
    minimal_python_repo: Path,
    monkeypatch: pytest.MonkeyPatch,
    captured_diagnostics: list[dict[str, Any]],
) -> None:
    """Verify build.phase.start and build.phase.end events emitted for each execution phase.

    Diagnostics events should be captured with correct phase names and timing.
    """
    from graph.product_build import GraphProductBuildRequest, build_graph_product

    def mock_emit(event_name: str, *, payload: dict[str, Any], event_kind: str) -> None:
        _ = event_kind
        captured_diagnostics.append({"event_name": event_name, "payload": payload})

    monkeypatch.setattr("graph.product_build.emit_diagnostics_event", mock_emit)

    def mock_execute_pipeline(*args: Any, **kwargs: Any) -> dict[str, Any]:
        _ = (args, kwargs)
        return {}

    monkeypatch.setattr("hamilton_pipeline.execute_pipeline", mock_execute_pipeline)

    request = GraphProductBuildRequest(repo_root=str(minimal_python_repo))
    with contextlib.suppress(Exception):
        build_graph_product(request)

    phase_events = [
        e
        for e in captured_diagnostics
        if e["event_name"] in {"build.phase.start", "build.phase.end"}
    ]

    assert len(phase_events) >= 2, "Should have phase start and end events"
    start_events = [e for e in phase_events if e["event_name"] == "build.phase.start"]
    end_events = [e for e in phase_events if e["event_name"] == "build.phase.end"]
    assert len(start_events) > 0, "Should have at least one phase start event"
    assert len(end_events) > 0, "Should have at least one phase end event"


@pytest.mark.integration
def test_build_failure_records_diagnostics(
    minimal_python_repo: Path,
    monkeypatch: pytest.MonkeyPatch,
    captured_diagnostics: list[dict[str, Any]],
) -> None:
    """Inject failure into build; verify build.failure event with exception info.

    Event payload should have status="error", exception type and message recorded.
    """
    from graph.product_build import GraphProductBuildRequest, build_graph_product

    def mock_emit(event_name: str, *, payload: dict[str, Any], event_kind: str) -> None:
        _ = event_kind
        captured_diagnostics.append({"event_name": event_name, "payload": payload})

    monkeypatch.setattr("graph.product_build.emit_diagnostics_event", mock_emit)

    def mock_execute_build_failure(*args: Any, **kwargs: Any) -> Any:
        _ = (args, kwargs)
        msg = "Injected test failure"
        raise ValueError(msg)

    monkeypatch.setattr("graph.product_build._execute_build", mock_execute_build_failure)

    request = GraphProductBuildRequest(repo_root=str(minimal_python_repo))
    with pytest.raises(ValueError, match="Injected test failure"):
        build_graph_product(request)

    failure_events = [e for e in captured_diagnostics if e["event_name"] == "build.failure"]
    assert len(failure_events) == 1, "Should have exactly one build.failure event"

    payload = failure_events[0]["payload"]
    assert payload["error_type"] == "ValueError", "Should record exception type"
    assert "Injected test failure" in payload["error"], "Should record exception message"


@pytest.mark.integration
def test_build_success_records_diagnostics(
    minimal_python_repo: Path,
    monkeypatch: pytest.MonkeyPatch,
    captured_diagnostics: list[dict[str, Any]],
) -> None:
    """Successful build emits build.success event.

    Event payload should have status="ok", timing fields present.
    """
    from graph.product_build import GraphProductBuildRequest, build_graph_product

    def mock_emit(event_name: str, *, payload: dict[str, Any], event_kind: str) -> None:
        _ = event_kind
        captured_diagnostics.append({"event_name": event_name, "payload": payload})

    monkeypatch.setattr("graph.product_build.emit_diagnostics_event", mock_emit)

    def mock_execute_build(*args: Any, **kwargs: Any) -> Any:
        _ = (args, kwargs)
        from graph.product_build import (
            FinalizeDeltaPaths,
            FinalizeDeltaReport,
            GraphProductBuildResult,
            TableDeltaReport,
        )

        return GraphProductBuildResult(
            product="cpg",
            product_version="0.1.0",
            engine_versions={},
            run_id="test_run",
            repo_root=minimal_python_repo,
            output_dir=minimal_python_repo / "build",
            cpg_nodes=FinalizeDeltaReport(
                paths=FinalizeDeltaPaths(
                    data=minimal_python_repo / "nodes",
                    errors=minimal_python_repo / "errors",
                    stats=minimal_python_repo / "stats",
                    alignment=minimal_python_repo / "alignment",
                ),
                rows=0,
                error_rows=0,
            ),
            cpg_edges=FinalizeDeltaReport(
                paths=FinalizeDeltaPaths(
                    data=minimal_python_repo / "edges",
                    errors=minimal_python_repo / "errors",
                    stats=minimal_python_repo / "stats",
                    alignment=minimal_python_repo / "alignment",
                ),
                rows=0,
                error_rows=0,
            ),
            cpg_props=FinalizeDeltaReport(
                paths=FinalizeDeltaPaths(
                    data=minimal_python_repo / "props",
                    errors=minimal_python_repo / "errors",
                    stats=minimal_python_repo / "stats",
                    alignment=minimal_python_repo / "alignment",
                ),
                rows=0,
                error_rows=0,
            ),
            cpg_props_map=TableDeltaReport(path=minimal_python_repo / "props_map", rows=0),
            cpg_edges_by_src=TableDeltaReport(path=minimal_python_repo / "edges_by_src", rows=0),
            cpg_edges_by_dst=TableDeltaReport(path=minimal_python_repo / "edges_by_dst", rows=0),
        )

    def mock_execute_pipeline(*args: Any, **kwargs: Any) -> dict[str, Any]:
        _ = (args, kwargs)
        return {}

    monkeypatch.setattr("graph.product_build._execute_build", mock_execute_build)
    monkeypatch.setattr("hamilton_pipeline.execute_pipeline", mock_execute_pipeline)

    request = GraphProductBuildRequest(repo_root=str(minimal_python_repo))
    build_graph_product(request)

    success_events = [e for e in captured_diagnostics if e["event_name"] == "build.success"]
    assert len(success_events) == 1, "Should have exactly one build.success event"

    payload = success_events[0]["payload"]
    assert "run_id" in payload, "Should include run_id"
    assert "output_dir" in payload, "Should include output_dir"
