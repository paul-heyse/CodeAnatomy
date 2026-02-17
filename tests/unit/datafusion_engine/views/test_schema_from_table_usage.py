"""Tests for graph schema helper de-duplication."""

from __future__ import annotations

from pathlib import Path


def test_views_graph_does_not_define_local_schema_from_table() -> None:
    """Graph module avoids duplicated local _schema_from_table helper."""
    source = Path("src/datafusion_engine/views/graph.py").read_text(encoding="utf-8")
    assert "def _schema_from_table" not in source
