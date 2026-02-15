"""Tests for shared macro target resolution helpers."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.macros import shared


def test_resolve_target_files_prefers_existing_path(tmp_path: Path) -> None:
    target = tmp_path / "module.py"
    target.write_text("def foo():\n    pass\n", encoding="utf-8")
    rows = shared.resolve_target_files(
        root=tmp_path,
        target=str(target),
        max_files=5,
    )
    assert rows == [target.resolve()]


def test_resolve_target_files_scans_symbol_candidates(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    candidate = tmp_path / "candidate.py"
    candidate.write_text("def interesting_symbol():\n    pass\n", encoding="utf-8")
    monkeypatch.setattr(
        shared,
        "iter_files",
        lambda **_kwargs: [candidate],
    )
    rows = shared.resolve_target_files(
        root=tmp_path,
        target="interesting_symbol",
        max_files=5,
    )
    assert rows == [candidate]
