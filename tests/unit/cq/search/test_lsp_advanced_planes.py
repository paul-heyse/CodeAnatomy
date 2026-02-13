"""Unit tests for bounded advanced LSP plane collector."""

from __future__ import annotations

import pytest
from tools.cq.search.lsp_advanced_planes import collect_advanced_lsp_planes


def test_collect_advanced_lsp_planes_python(monkeypatch: pytest.MonkeyPatch) -> None:
    from tools.cq.search import lsp_advanced_planes as planes

    monkeypatch.setattr(
        planes,
        "fetch_semantic_tokens_range",
        lambda _session, _uri, _start_line, _end_line: (),
    )
    monkeypatch.setattr(
        planes,
        "fetch_inlay_hints_range",
        lambda _session, _uri, _start_line, _end_line: (),
    )
    monkeypatch.setattr(
        planes,
        "pull_text_document_diagnostics",
        lambda _session, _uri: ({"message": "x"},),
    )
    monkeypatch.setattr(
        planes,
        "pull_workspace_diagnostics",
        lambda _session: ({"message": "y"},),
    )

    payload = collect_advanced_lsp_planes(
        session=object(),
        language="python",
        uri="file:///tmp/a.py",
        line=10,
        col=0,
    )
    assert payload["document_diagnostics_count"] == 1
    assert payload["workspace_diagnostics_count"] == 1
    assert "macro_expansion_available" not in payload


def test_collect_advanced_lsp_planes_rust(monkeypatch: pytest.MonkeyPatch) -> None:
    from tools.cq.search import lsp_advanced_planes as planes
    from tools.cq.search.rust_extensions import RustMacroExpansionV1, RustRunnableV1

    monkeypatch.setattr(
        planes,
        "fetch_semantic_tokens_range",
        lambda _session, _uri, _start_line, _end_line: (),
    )
    monkeypatch.setattr(
        planes,
        "fetch_inlay_hints_range",
        lambda _session, _uri, _start_line, _end_line: (),
    )
    monkeypatch.setattr(
        planes,
        "pull_text_document_diagnostics",
        lambda _session, _uri: (),
    )
    monkeypatch.setattr(
        planes,
        "pull_workspace_diagnostics",
        lambda _session: (),
    )
    monkeypatch.setattr(
        planes,
        "expand_macro",
        lambda _session, _uri, _line, _col: RustMacroExpansionV1(name="m", expansion="x"),
    )
    monkeypatch.setattr(
        planes,
        "get_runnables",
        lambda _session, _uri: (RustRunnableV1(label="r", kind="cargo"),),
    )

    payload = collect_advanced_lsp_planes(
        session=object(),
        language="rust",
        uri="file:///tmp/a.rs",
        line=10,
        col=0,
    )
    assert payload["macro_expansion_available"] is True
    assert payload["runnables_count"] == 1
