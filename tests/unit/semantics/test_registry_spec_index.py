# ruff: noqa: D103
"""Tests for semantic registry spec index contract."""

from __future__ import annotations

from semantics.registry import SemanticSpecIndex


def test_semantic_spec_index_struct_fields() -> None:
    index = SemanticSpecIndex(name="v", kind="normalize", inputs=("a",), outputs=("b",))
    assert index.name == "v"
    assert index.inputs == ("a",)
