"""Unit tests for language-aware front-door LSP adapter."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.search.lsp_front_door_adapter import (
    LanguageLspEnrichmentRequest,
    enrich_with_language_lsp,
    infer_language_for_path,
    provider_for_language,
)


def test_infer_language_for_path() -> None:
    assert infer_language_for_path(Path("a.py")) == "python"
    assert infer_language_for_path(Path("a.rs")) == "rust"
    assert infer_language_for_path(Path("a.txt")) is None


def test_provider_for_language() -> None:
    assert provider_for_language("python") == "pyrefly"
    assert provider_for_language("rust") == "rust_analyzer"
    assert provider_for_language("auto") == "none"


def test_enrich_with_language_lsp_python(monkeypatch: pytest.MonkeyPatch) -> None:
    from tools.cq.search import lsp_front_door_adapter as adapter

    monkeypatch.delenv("CQ_ENABLE_LSP", raising=False)
    monkeypatch.setattr(
        adapter,
        "enrich_with_pyrefly_lsp",
        lambda _request: {"call_graph": {"incoming_total": 1, "outgoing_total": 0}},
    )
    payload, timed_out = enrich_with_language_lsp(
        LanguageLspEnrichmentRequest(
            language="python",
            mode="search",
            root=Path(),
            file_path=Path("foo.py"),
            line=10,
            col=0,
            symbol_hint="foo",
        )
    )
    assert isinstance(payload, dict)
    assert timed_out is False


def test_enrich_with_language_lsp_rust(monkeypatch: pytest.MonkeyPatch) -> None:
    from tools.cq.search import lsp_front_door_adapter as adapter

    monkeypatch.delenv("CQ_ENABLE_LSP", raising=False)
    monkeypatch.setattr(
        adapter,
        "enrich_with_rust_lsp",
        lambda *_args, **_kwargs: {"call_graph": {"incoming_callers": []}},
    )
    payload, timed_out = enrich_with_language_lsp(
        LanguageLspEnrichmentRequest(
            language="rust",
            mode="search",
            root=Path(),
            file_path=Path("foo.rs"),
            line=10,
            col=0,
            symbol_hint="foo",
        )
    )
    assert isinstance(payload, dict)
    assert timed_out is False
