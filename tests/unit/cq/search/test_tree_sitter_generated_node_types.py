"""Tests for generated node-type snapshot modules."""

from __future__ import annotations

from tools.cq.search.generated.python_node_types_v1 import NODE_TYPES as PY_NODE_TYPES
from tools.cq.search.generated.rust_node_types_v1 import NODE_TYPES as RS_NODE_TYPES


def test_generated_python_node_types_snapshot_present() -> None:
    assert PY_NODE_TYPES
    assert any(row[0] == "function_definition" for row in PY_NODE_TYPES)


def test_generated_rust_node_types_snapshot_present() -> None:
    assert RS_NODE_TYPES
    assert any(row[0] == "function_item" for row in RS_NODE_TYPES)
