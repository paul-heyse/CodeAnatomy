"""Tests for pure AST parse/walk helper behavior."""

from __future__ import annotations

from extract.coordination.context import FileContext
from extract.extractors.ast.builders import _run_ast_parse_walk
from extract.extractors.ast.setup import AstExtractOptions


def test_run_ast_parse_walk_returns_walk_for_valid_python() -> None:
    """Valid Python should return a walk artifact with no parse errors."""
    file_ctx = FileContext(
        file_id="f1",
        path="a.py",
        abs_path=None,
        file_sha256="abc",
        text="x = 1\n",
    )

    walk, errors = _run_ast_parse_walk(file_ctx, options=AstExtractOptions())

    assert walk is not None
    assert errors == []
    assert len(walk.nodes) >= 1


def test_run_ast_parse_walk_returns_error_for_invalid_python() -> None:
    """Invalid Python should return parse errors and no walk artifact."""
    file_ctx = FileContext(
        file_id="f2",
        path="bad.py",
        abs_path=None,
        file_sha256="def",
        text="def bad(:\n",
    )

    walk, errors = _run_ast_parse_walk(file_ctx, options=AstExtractOptions())

    assert walk is None
    assert errors
