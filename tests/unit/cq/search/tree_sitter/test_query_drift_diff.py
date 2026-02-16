"""Tests for grammar drift diff helpers."""

from __future__ import annotations

from types import SimpleNamespace

from tools.cq.search.tree_sitter.query.drift import diff_schema, has_breaking_changes


def test_diff_schema_reports_added_and_removed_members() -> None:
    """Test diff schema reports added and removed members."""
    old_index = SimpleNamespace(
        all_node_kinds={"function_item", "struct_item"},
        field_names={"name", "body"},
    )
    new_index = SimpleNamespace(
        all_node_kinds={"function_item", "enum_item"},
        field_names={"name", "variants"},
    )

    diff = diff_schema(old_index, new_index)

    assert diff.added_node_kinds == ("enum_item",)
    assert diff.removed_node_kinds == ("struct_item",)
    assert diff.added_fields == ("variants",)
    assert diff.removed_fields == ("body",)
    assert has_breaking_changes(diff) is True
