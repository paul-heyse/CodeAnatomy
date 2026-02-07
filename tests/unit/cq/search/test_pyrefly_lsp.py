"""Tests for Pyrefly LSP enrichment integration."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.search.pyrefly_lsp import (
    PyreflyLspRequest,
    close_pyrefly_lsp_sessions,
    enrich_with_pyrefly_lsp,
)


class _FakeSession:
    def __init__(self, payload: dict[str, object] | None) -> None:
        self.payload = payload

    def probe(self, request: PyreflyLspRequest) -> dict[str, object] | None:  # noqa: ARG002
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
