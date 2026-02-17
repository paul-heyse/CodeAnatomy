"""Tests for core path utility helpers."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.path_utils import safe_rel_path


def test_safe_rel_path_returns_relative_path_when_under_root(tmp_path: Path) -> None:
    """Return a relative path when target is within root."""
    root = tmp_path / "repo"
    nested = root / "tools" / "cq"
    nested.mkdir(parents=True)
    target = nested / "file.py"
    target.write_text("", encoding="utf-8")

    assert safe_rel_path(root=root, path=target) == "tools/cq/file.py"


def test_safe_rel_path_returns_absolute_path_when_outside_root(tmp_path: Path) -> None:
    """Return an absolute path when target is outside root."""
    root = tmp_path / "repo"
    root.mkdir()
    outside = tmp_path / "outside.py"
    outside.write_text("", encoding="utf-8")

    assert safe_rel_path(root=root, path=outside) == outside.resolve().as_posix()
