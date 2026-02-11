"""Transcript-style integration tests for Pyrefly LSP stdio session."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from tools.cq.search import pyrefly_lsp
from tools.cq.search.pyrefly_lsp import PyreflyLspRequest, _PyreflyLspSession


def test_pyrefly_lsp_stdio_transcript_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    file_path = repo / "module.py"
    file_path.write_text(
        "def resolve(payload: str) -> str:\n    return payload\n", encoding="utf-8"
    )

    server_script = (Path(__file__).parent / "lsp_harness" / "fake_stdio_lsp_server.py").resolve()
    real_popen = subprocess.Popen

    def fake_popen(_cmd: object, **kwargs: Any) -> subprocess.Popen[str]:
        return real_popen(
            [sys.executable, str(server_script), "--mode", "pyrefly"],
            **kwargs,
        )

    monkeypatch.setattr(pyrefly_lsp.subprocess, "Popen", fake_popen)

    session = _PyreflyLspSession(repo)
    session.ensure_started(timeout_seconds=1.0)

    caps = session.capabilities_snapshot()
    assert caps.get("definitionProvider") is True

    payload = session.probe(
        PyreflyLspRequest(
            root=repo,
            file_path=file_path,
            line=1,
            col=4,
            symbol_hint="resolve",
            timeout_seconds=0.8,
        )
    )
    assert payload is not None
    coverage = payload.get("coverage")
    assert isinstance(coverage, dict)
    assert coverage.get("status") in {"applied", "not_resolved"}

    symbol_grounding = payload.get("symbol_grounding")
    assert isinstance(symbol_grounding, dict)
    definitions = symbol_grounding.get("definition_targets")
    assert isinstance(definitions, list)
    assert definitions

    diagnostics = payload.get("anchor_diagnostics")
    assert isinstance(diagnostics, list)
    assert diagnostics

    file_path.write_text(
        "def resolve(payload: str) -> str:\n    return payload.upper()\n",
        encoding="utf-8",
    )
    payload_after_change = session.probe(
        PyreflyLspRequest(
            root=repo,
            file_path=file_path,
            line=1,
            col=4,
            symbol_hint="resolve",
            timeout_seconds=0.8,
        )
    )
    assert payload_after_change is not None

    session.close()
    assert session.is_running is False
