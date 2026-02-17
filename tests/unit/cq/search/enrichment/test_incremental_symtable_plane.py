"""Tests for incremental symtable plane."""

from __future__ import annotations

import symtable

from tools.cq.search.enrichment.incremental_symtable_plane import (
    build_incremental_symtable_plane,
    build_sym_scope_graph,
)

_MIN_EXPECTED_TABLES = 2


def test_build_sym_scope_graph_tables() -> None:
    """Build scope graph rows from parsed symtable data."""
    table = symtable.symtable("x = 1\ndef f(a):\n    return a + x\n", "sample.py", "exec")
    graph = build_sym_scope_graph(table)
    tables_count = graph.get("tables_count")
    assert isinstance(tables_count, int)
    assert tables_count >= _MIN_EXPECTED_TABLES


def test_incremental_symtable_plane_binding_resolution() -> None:
    """Resolve anchor binding identity in the incremental symtable payload."""
    source = "x = 1\ndef f(a):\n    return a + x\n"
    table = symtable.symtable(source, "sample.py", "exec")
    payload = build_incremental_symtable_plane(table, anchor_name="x", anchor_line=3)
    binding = payload.get("binding_resolution")
    assert isinstance(binding, dict)
    assert binding.get("symbol") == "x"
    assert "binding_id" in binding
    scope_graph = payload.get("scope_graph")
    assert isinstance(scope_graph, dict)
    tables_count = scope_graph.get("tables_count")
    assert isinstance(tables_count, int)
    assert tables_count >= _MIN_EXPECTED_TABLES
