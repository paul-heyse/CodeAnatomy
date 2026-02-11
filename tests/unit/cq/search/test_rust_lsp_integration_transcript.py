# ruff: noqa: SLF001
"""Transcript-style integration tests for Rust LSP stdio session."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from tools.cq.search import rust_lsp
from tools.cq.search.rust_lsp import RustLspRequest, _RustLspSession


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

    session.shutdown()
    session.shutdown()
    assert session.is_running is False
