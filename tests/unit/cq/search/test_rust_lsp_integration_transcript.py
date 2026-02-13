# ruff: noqa: SLF001
"""Transcript-style integration tests for Rust LSP stdio session."""

from __future__ import annotations

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest
from tools.cq.search import rust_lsp
from tools.cq.search.rust_lsp import RustLspRequest, _RustLspSession
from tools.cq.search.rust_lsp_contracts import RustLspEnrichmentPayload


def test_rust_lsp_stdio_transcript_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    file_path = repo / "lib.rs"
    file_path.write_text("pub fn compile_target(input: &str) -> String { input.to_string() }\n")

    server_script = (Path(__file__).parent / "lsp_harness" / "fake_stdio_lsp_server.py").resolve()
    real_popen = subprocess.Popen

    def fake_popen(_cmd: object, **kwargs: Any) -> subprocess.Popen[str]:
        return real_popen(
            [sys.executable, str(server_script), "--mode", "rust"],
            **kwargs,
        )

    monkeypatch.setattr(rust_lsp.subprocess, "Popen", fake_popen)

    session = _RustLspSession(repo)
    session.ensure_started(timeout_seconds=1.0)

    assert session._session_env.server_name == "fake-rust-analyzer"
    assert session._session_env.workspace_health == "ok"
    assert session._session_env.quiescent is True

    payload = session.probe(RustLspRequest(file_path=str(file_path), line=1, col=0))
    assert payload is not None
    assert payload.session_env.workspace_health == "ok"
    assert payload.symbol_grounding.definitions
    assert payload.symbol_grounding.references
    assert payload.call_graph.incoming_callers
    assert payload.call_graph.outgoing_callees
    assert payload.type_hierarchy.supertypes
    assert payload.type_hierarchy.subtypes
    assert payload.document_symbols
    assert payload.diagnostics
    assert "workspace/inlayHint/refresh" in payload.session_env.refresh_events

    status = session._send_request("cq/testStatus", {})
    assert isinstance(status, dict)
    assert status.get("refresh_acknowledged") is True

    session.shutdown()
    session.shutdown()
    assert session.is_running is False


def test_rust_lsp_parallel_probes_single_session_stable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo_parallel"
    repo.mkdir()
    file_path = repo / "lib.rs"
    file_path.write_text("pub fn compile_target(input: &str) -> String { input.to_string() }\n")

    server_script = (Path(__file__).parent / "lsp_harness" / "fake_stdio_lsp_server.py").resolve()
    real_popen = subprocess.Popen

    def fake_popen(_cmd: object, **kwargs: Any) -> subprocess.Popen[str]:
        return real_popen(
            [sys.executable, str(server_script), "--mode", "rust"],
            **kwargs,
        )

    monkeypatch.setattr(rust_lsp.subprocess, "Popen", fake_popen)

    session = _RustLspSession(repo)
    session.ensure_started(timeout_seconds=1.0)

    def _probe(_idx: int) -> RustLspEnrichmentPayload | None:
        return session.probe(RustLspRequest(file_path=str(file_path), line=1, col=0))

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(_probe, range(8)))

    assert all(result is not None for result in results)
    typed_results = [result for result in results if isinstance(result, RustLspEnrichmentPayload)]
    assert typed_results
    assert all(result.session_env.workspace_health == "ok" for result in typed_results)
    assert all(result.symbol_grounding.definitions for result in typed_results)

    session.shutdown()
