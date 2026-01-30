"""Tests for DataFusion registry view expression helpers."""

from __future__ import annotations

import pytest

from datafusion_engine.view_registry import VIEW_SELECT_REGISTRY


@pytest.mark.parametrize(
    "name",
    [
        "cst_refs_attrs",
        "cst_defs_attrs",
        "cst_callsites_attrs",
        "cst_imports_attrs",
        "cst_nodes_attrs",
        "cst_edges_attrs",
    ],
)
def test_cst_attrs_views_expose_attr_columns(
    name: str,
) -> None:
    """Ensure CST attrs views expose attr_key/attr_value columns."""
    exprs = VIEW_SELECT_REGISTRY.get(name)
    assert exprs is not None
    expr_names = {expr.schema_name() for expr in exprs}
    assert "attr_key" in expr_names
    assert "attr_value" in expr_names
