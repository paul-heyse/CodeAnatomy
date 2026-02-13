"""Tests for Pyrefly LSP enrichment integration."""

from __future__ import annotations

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest
from tools.cq.search import pyrefly_lsp
from tools.cq.search.pyrefly_lsp import (
    PyreflyLspRequest,
    _empty_probe_payload,
    close_pyrefly_lsp_sessions,
    enrich_with_pyrefly_lsp,
)


class _FakeSession:
    def __init__(self, payload: dict[str, object] | None) -> None:
        self.payload = payload

    def probe(self, _request: PyreflyLspRequest) -> dict[str, object] | None:
        return self.payload


def test_enrich_with_pyrefly_lsp_python_file_uses_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    file_path = repo / "module.py"
    file_path.write_text("def target(x: int) -> int:\n    return x\n", encoding="utf-8")

    payload: dict[str, object] = {
        "symbol_grounding": {"definition_targets": [{"file": str(file_path), "line": 1}]},
        "type_contract": {"resolved_type": "(x: int) -> int"},
        "call_graph": {"incoming_total": 0, "outgoing_total": 0},
        "class_method_context": {"enclosing_class": None},
        "local_scope_context": {"same_scope_symbols": list[object]()},
        "import_alias_resolution": {"resolved_path": str(file_path), "alias_chain": list[str]()},
        "anchor_diagnostics": list[object](),
        "coverage": {"status": "applied", "reason": None},
    }

    monkeypatch.setattr(
        "tools.cq.search.pyrefly_lsp._session_for_root",
        lambda *_args, **_kwargs: _FakeSession(payload),
    )

    result = enrich_with_pyrefly_lsp(
        PyreflyLspRequest(
            root=repo,
            file_path=file_path,
            line=1,
            col=4,
        )
    )

    assert isinstance(result, dict)
    assert "type_contract" in result
    assert "symbol_grounding" in result


def test_enrich_with_pyrefly_lsp_non_python_file_returns_none(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    file_path = repo / "module.rs"
    file_path.write_text("fn target() {}\n", encoding="utf-8")

    result = enrich_with_pyrefly_lsp(
        PyreflyLspRequest(
            root=repo,
            file_path=file_path,
            line=1,
            col=0,
        )
    )

    assert result is None


def test_close_pyrefly_lsp_sessions_smoke() -> None:
    close_pyrefly_lsp_sessions()


def test_enrich_with_pyrefly_lsp_concurrent_shared_root_is_stable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    file_path = repo / "module.py"
    file_path.write_text(
        "def resolve(payload: str) -> str:\n    return payload\n",
        encoding="utf-8",
    )

    server_script = (Path(__file__).parent / "lsp_harness" / "fake_stdio_lsp_server.py").resolve()
    real_popen = subprocess.Popen

    def fake_popen(_cmd: object, **kwargs: Any) -> subprocess.Popen[str]:
        return real_popen(
            [sys.executable, str(server_script), "--mode", "pyrefly"],
            **kwargs,
        )

    monkeypatch.setattr(pyrefly_lsp.subprocess, "Popen", fake_popen)
    close_pyrefly_lsp_sessions()

    requests = [
        PyreflyLspRequest(
            root=repo,
            file_path=file_path,
            line=1,
            col=4,
            symbol_hint="resolve",
            timeout_seconds=0.8,
        )
        for _ in range(8)
    ]

    def _run(request: PyreflyLspRequest) -> bool:
        payload = enrich_with_pyrefly_lsp(request)
        if not isinstance(payload, dict):
            return False
        coverage = payload.get("coverage")
        return isinstance(coverage, dict)

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(_run, requests))

    assert all(results)


def test_empty_probe_payload_uses_unsupported_capability_reason() -> None:
    payload = _empty_probe_payload(reason="unsupported_capability", position_encoding="utf-16")
    coverage = payload.get("coverage")
    assert isinstance(coverage, dict)
    assert coverage.get("status") == "not_resolved"
    assert coverage.get("reason") == "unsupported_capability"


def test_empty_probe_payload_uses_request_interface_reason() -> None:
    payload = _empty_probe_payload(
        reason="request_interface_unavailable",
        position_encoding="utf-16",
    )
    coverage = payload.get("coverage")
    assert isinstance(coverage, dict)
    assert coverage.get("status") == "not_resolved"
    assert coverage.get("reason") == "request_interface_unavailable"
