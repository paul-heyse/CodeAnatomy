"""Tests for DataFusion query fragment helpers."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from datafusion_engine.query_fragments import (
    cst_callsites_attrs_sql,
    cst_defs_attrs_sql,
    cst_edges_attrs_sql,
    cst_imports_attrs_sql,
    cst_nodes_attrs_sql,
    cst_refs_attrs_sql,
)


@pytest.mark.parametrize(
    ("builder", "base"),
    [
        (cst_refs_attrs_sql, "cst_refs"),
        (cst_defs_attrs_sql, "cst_defs"),
        (cst_callsites_attrs_sql, "cst_callsites"),
        (cst_imports_attrs_sql, "cst_imports"),
        (cst_nodes_attrs_sql, "cst_nodes"),
        (cst_edges_attrs_sql, "cst_edges"),
    ],
)
def test_cst_attrs_views_expose_attr_columns(
    builder: Callable[[], str],
    base: str,
) -> None:
    """Ensure CST attrs views expose attr_key/attr_value columns."""
    sql = builder()
    assert "map_entries" in sql
    assert "attr_key" in sql
    assert "attr_value" in sql
    assert f"FROM {base}" in sql
