"""Tests for shared macro scan helpers."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.macros import shared


def test_iter_files_applies_max_files(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    files = [tmp_path / "a.py", tmp_path / "b.py", tmp_path / "c.py"]
    for path in files:
        path.write_text("pass\n", encoding="utf-8")

    monkeypatch.setattr(shared, "resolve_macro_files", lambda **_kwargs: files)
    rows = shared.iter_files(root=tmp_path, max_files=2)
    assert rows == files[:2]
