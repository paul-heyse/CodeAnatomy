"""Tests for AST-grep metavariable helper extraction."""

from __future__ import annotations

from tools.cq.astgrep.metavar import (
    extract_metavar_names,
    extract_variadic_metavar_names,
)


def test_extract_metavar_names_returns_unique_sorted_names() -> None:
    """Metavar extraction should de-duplicate and sort names."""
    text = "foo($X, $Y) and $$$ARGS and $X"
    assert extract_metavar_names(text) == ("ARGS", "X", "Y")


def test_extract_variadic_metavar_names_filters_to_variadic_tokens() -> None:
    """Variadic extraction should only keep $$$-prefixed metavars."""
    text = "foo($X, $$$ARGS, $$$REST)"
    assert extract_variadic_metavar_names(text) == ("ARGS", "REST")
