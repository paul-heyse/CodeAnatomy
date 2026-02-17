"""Unit tests for calls target-resolution wrappers."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.macros.calls.target_classification import classify_call_target_language
from tools.cq.macros.calls.target_resolution import resolve_call_target


def test_resolve_call_target_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolution helper should delegate to shared target resolver."""
    sentinel = ("a.py", 10)

    def fake_resolve(
        *,
        root: Path,
        function_name: str,
        target_language: str | None = None,
    ) -> tuple[str, int]:
        assert root == Path()
        assert function_name == "pkg.fn"
        assert target_language == "python"
        return sentinel

    monkeypatch.setattr(
        "tools.cq.macros.calls.target_resolution.resolve_target_definition",
        fake_resolve,
    )

    assert resolve_call_target(Path(), "pkg.fn", target_language="python") == sentinel


def test_classify_call_target_language_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Language classification helper should delegate to infer_target_language."""
    monkeypatch.setattr(
        "tools.cq.macros.calls.target_classification.infer_target_language",
        lambda _root, _name: "rust",
    )
    assert classify_call_target_language(Path(), "pkg.fn") == "rust"
