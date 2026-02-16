"""Tests for semantic path-filter helpers."""

from __future__ import annotations

from tools.cq.search.semantic.models import compile_globs, match_path


def test_compile_globs_and_match_path() -> None:
    """Test compile globs and match path."""
    spec = compile_globs(["**/*.py"])
    assert match_path(spec, "src/foo.py")
    assert not match_path(spec, "src/foo.rs")
