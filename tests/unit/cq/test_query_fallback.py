"""Tests for query fallback behavior."""

from __future__ import annotations

from pathlib import Path

from tools.cq.cli_app.commands.query import q
from tools.cq.cli_app.context import CliContext


def _make_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "sample.py").write_text("def build_graph():\n    return 1\n")
    return repo


def test_q_fallbacks_to_smart_search_on_plain_query(tmp_path: Path) -> None:
    """Plain query strings should fall back to smart search."""
    repo = _make_repo(tmp_path)
    ctx = CliContext.build(argv=["cq", "q", "build_graph"], root=repo)
    result = q("build_graph", ctx=ctx)
    assert result.is_cq_result
    cq_result = result.result
    assert cq_result.run.macro == "search"
    assert cq_result.summary["query"] == "build_graph"


def test_q_parse_error_does_not_fallback(tmp_path: Path) -> None:
    """Malformed token queries should surface parse errors."""
    repo = _make_repo(tmp_path)
    ctx = CliContext.build(argv=["cq", "q", "name=build_graph"], root=repo)
    result = q("name=build_graph", ctx=ctx)
    assert result.is_cq_result
    cq_result = result.result
    assert cq_result.run.macro == "q"
    assert "entity" in str(cq_result.summary.get("error", ""))
