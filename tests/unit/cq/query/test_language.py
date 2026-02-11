"""Tests for language scope/glob helpers."""

from __future__ import annotations

from tools.cq.query.language import constrain_include_globs_for_language


def test_constrain_include_globs_directory_python() -> None:
    """Directory globs should retain slash separators for Python suffix expansion."""
    constrained = constrain_include_globs_for_language(["tools/cq/**"], "python")
    assert constrained == ["tools/cq/**/*.py", "tools/cq/**/*.pyi"]


def test_constrain_include_globs_directory_rust() -> None:
    """Directory globs should expand to Rust suffixes without malformed paths."""
    constrained = constrain_include_globs_for_language(["rust/**"], "rust")
    assert constrained == ["rust/**/*.rs"]


def test_constrain_include_globs_bare_directory_pattern() -> None:
    """Patterns ending in ** should expand to proper recursive globs."""
    constrained = constrain_include_globs_for_language(["tools/**"], "python")
    assert constrained == ["tools/**/*.py", "tools/**/*.pyi"]


def test_constrain_include_globs_file_suffix_filtering() -> None:
    """Explicit file paths should be kept only when extension matches language."""
    assert constrain_include_globs_for_language(["foo.py"], "python") == ["foo.py"]
    assert constrain_include_globs_for_language(["foo.py"], "rust") is None


def test_constrain_include_globs_preserves_negated_patterns() -> None:
    """Negated include globs should pass through unchanged."""
    constrained = constrain_include_globs_for_language(["!**/vendor/**"], "python")
    assert constrained == ["!**/vendor/**"]
