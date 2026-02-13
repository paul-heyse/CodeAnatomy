"""Pyrefly LSP recovery behavior tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.search.pyrefly_lsp import PyreflyLspRequest, enrich_with_pyrefly_lsp


class _FlakySession:
    def __init__(self) -> None:
        self.is_running = False
        self.calls = 0

    def probe(self, _request: PyreflyLspRequest) -> dict[str, object]:
        self.calls += 1
        if self.calls == 1:
            msg = "transient"
            raise RuntimeError(msg)
        self.is_running = True
        return {"coverage": {"status": "applied"}}


class _DeadSession:
    def __init__(self) -> None:
        self.is_running = False

    def probe(self, _request: PyreflyLspRequest) -> None:
        return None


def test_enrich_with_pyrefly_retries_after_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    session = _FlakySession()
    resets: list[str] = []

    monkeypatch.setattr(
        "tools.cq.search.pyrefly_lsp._session_for_root",
        lambda *_args, **_kwargs: session,
    )
    monkeypatch.setattr(
        "tools.cq.search.pyrefly_lsp._SESSION_MANAGER.reset_root",
        lambda _root: resets.append("reset"),
    )
    monkeypatch.setattr(
        "tools.cq.search.pyrefly_lsp.coerce_pyrefly_payload",
        lambda payload: payload,
    )
    monkeypatch.setattr(
        "tools.cq.search.pyrefly_lsp.pyrefly_payload_to_dict",
        lambda payload: payload,
    )

    payload = enrich_with_pyrefly_lsp(
        PyreflyLspRequest(
            root=tmp_path,
            file_path=tmp_path / "sample.py",
            line=1,
            col=0,
        )
    )
    assert payload == {"coverage": {"status": "applied"}}
    assert resets == ["reset"]


def test_enrich_with_pyrefly_resets_when_session_not_running(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    session = _DeadSession()
    resets: list[str] = []

    monkeypatch.setattr(
        "tools.cq.search.pyrefly_lsp._session_for_root",
        lambda *_args, **_kwargs: session,
    )
    monkeypatch.setattr(
        "tools.cq.search.pyrefly_lsp._SESSION_MANAGER.reset_root",
        lambda _root: resets.append("reset"),
    )

    payload = enrich_with_pyrefly_lsp(
        PyreflyLspRequest(
            root=tmp_path,
            file_path=tmp_path / "sample.py",
            line=1,
            col=0,
        )
    )
    assert payload is None
    assert resets == ["reset"]
