"""Tests for incremental details expand macro."""

from __future__ import annotations

from pathlib import Path

from tools.cq.macros.expand import cmd_expand


def test_cmd_expand_known_kind(tmp_path: Path) -> None:
    """Expand a known details kind and return a successful result."""
    result = cmd_expand(
        root=tmp_path,
        argv=["cq", "expand"],
        tc=None,
        kind="sym.scope_graph",
        handle={"payload": {"tables_count": 3}},
    )
    assert result.summary.error is None
    assert result.sections
    assert result.sections[0].findings[0].details.kind == "sym.scope_graph"


def test_cmd_expand_unknown_kind_returns_error(tmp_path: Path) -> None:
    """Return an error result when expansion kind is unknown."""
    result = cmd_expand(
        root=tmp_path,
        argv=["cq", "expand"],
        tc=None,
        kind="unknown.kind",
        handle={},
    )
    assert result.summary.error is not None
