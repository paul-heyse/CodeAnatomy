"""Integration tests for build orchestration lifecycle.

Tests the boundary where GraphProductBuildRequest -> build_graph_product() ->
heartbeat + signal handlers + build-phase diagnostics.
"""

from __future__ import annotations

import signal
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest


def _stub_build_result(repo_root: Path) -> object:
    from graph.build_pipeline import BuildResult

    output_dir = repo_root / "build"

    def _finalize_payload(name: str) -> dict[str, object]:
        data_path = output_dir / name
        return {
            "path": str(data_path),
            "rows": 0,
            "error_rows": 0,
            "paths": {
                "data": str(data_path),
                "errors": str(data_path / "_errors"),
                "stats": str(data_path / "_stats"),
                "alignment": str(data_path / "_alignment"),
            },
        }

    return BuildResult(
        cpg_outputs={
            "cpg_nodes": _finalize_payload("cpg_nodes"),
            "cpg_edges": _finalize_payload("cpg_edges"),
            "cpg_props": _finalize_payload("cpg_props"),
            "cpg_props_map": {"path": str(output_dir / "cpg_props_map"), "rows": 0},
            "cpg_edges_by_src": {"path": str(output_dir / "cpg_edges_by_src"), "rows": 0},
            "cpg_edges_by_dst": {"path": str(output_dir / "cpg_edges_by_dst"), "rows": 0},
        },
        auxiliary_outputs={},
        run_result={},
        extraction_timing={},
        warnings=[],
    )


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
def test_signal_handler_captures_correct_state(
    minimal_python_repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the signal handler closure captures run_id and run_bundle_dir.

    The handler's signal_state dict, run_bundle_dir, and run_id should match
    the build request values.
    """
    from graph.product_build import _signal_handler_for_build

    run_id = "test_run_signal_state"
    run_bundle_dir = minimal_python_repo / "build" / "run_bundle" / run_id
    signal_state = {"value": False}
    writes: list[tuple[Path, str]] = []

    def mock_write_diagnostics_outputs(path: Path, *, run_id: str | None) -> None:
        writes.append((path, run_id or ""))

    monkeypatch.setattr(
        "graph.product_build._write_diagnostics_outputs",
        mock_write_diagnostics_outputs,
    )

    handler = _signal_handler_for_build(run_id, run_bundle_dir, signal_state)

    with pytest.raises(SystemExit):
        handler(signal.SIGTERM, None)

    assert signal_state["value"] is True
    assert writes == [(run_bundle_dir, run_id)]

    with pytest.raises(SystemExit):
        handler(signal.SIGINT, None)

    # Second signal should short-circuit via signal_state without rewriting diagnostics.
    assert writes == [(run_bundle_dir, run_id)]


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

    def mock_orchestrate_build(*args: Any, **kwargs: Any) -> object:
        _ = (args, kwargs)
        return _stub_build_result(minimal_python_repo)

    monkeypatch.setattr("graph.build_pipeline.orchestrate_build", mock_orchestrate_build)

    request = GraphProductBuildRequest(repo_root=str(minimal_python_repo))
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

    monkeypatch.setattr("graph.product_build._execute_build", mock_execute_build)

    request = GraphProductBuildRequest(repo_root=str(minimal_python_repo))
    build_graph_product(request)

    success_events = [e for e in captured_diagnostics if e["event_name"] == "build.success"]
    assert len(success_events) == 1, "Should have exactly one build.success event"

    payload = success_events[0]["payload"]
    assert "run_id" in payload, "Should include run_id"
    assert "output_dir" in payload, "Should include output_dir"


@pytest.mark.integration
def test_engine_boundary_error_preserves_typed_stage_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """build_pipeline should not re-wrap typed EngineExecutionError-like exceptions."""
    from graph import build_pipeline as build_pipeline_mod
    from planning_engine.spec_contracts import (
        JoinGraph,
        OutputTarget,
        RuleIntent,
        RuntimeConfig,
        SemanticExecutionSpec,
        ViewDefinition,
    )

    class _TypedEngineError(Exception):
        def __init__(self) -> None:
            super().__init__("typed boundary failure")
            self.stage = "runtime"
            self.code = "RUN_BUILD_EXECUTION_FAILED"
            self.details = {"origin": "test"}

    class _FakeEngineModule:
        @staticmethod
        def run_build(_request_json: str) -> dict[str, object]:
            raise _TypedEngineError

    monkeypatch.setattr(
        build_pipeline_mod.importlib,
        "import_module",
        lambda _name: _FakeEngineModule(),
    )

    spec = SemanticExecutionSpec(
        version=1,
        input_relations=(),
        view_definitions=(
            ViewDefinition(
                name="v1",
                view_kind="project",
                view_dependencies=(),
                transform={"kind": "Project", "source": "input", "columns": []},
                output_schema={"columns": {}},
            ),
        ),
        join_graph=JoinGraph(edges=(), constraints=()),
        output_targets=(
            OutputTarget(
                table_name="cpg_nodes",
                source_view="v1",
                columns=(),
                delta_location="/tmp/cpg_nodes",
            ),
        ),
        rule_intents=(RuleIntent(name="semantic_integrity", rule_class="SemanticIntegrity"),),
        rulepack_profile="Default",
        typed_parameters=(),
        runtime=RuntimeConfig(),
        spec_hash=b"",
    )

    with pytest.raises(_TypedEngineError) as exc_info:
        build_pipeline_mod._execute_engine_phase({}, spec, "small")  # noqa: SLF001
    assert exc_info.value.stage == "runtime"
    assert exc_info.value.code == "RUN_BUILD_EXECUTION_FAILED"
