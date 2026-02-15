"""Tests for merged query drift module (diff types and grammar drift reporting)."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

from tools.cq.search.tree_sitter.query.drift import (
    GrammarDiffV1,
    build_grammar_drift_report,
    diff_schema,
    has_breaking_changes,
)

# -- Diff Types ---------------------------------------------------------------


def test_diff_schema_reports_added_and_removed_members() -> None:
    """Verify diff_schema captures node-kind and field changes."""
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


def test_has_breaking_changes_false_for_additions_only() -> None:
    """Verify additions-only diffs are not breaking."""
    diff = GrammarDiffV1(added_node_kinds=("new_kind",))
    assert has_breaking_changes(diff) is False


# -- Drift Report -------------------------------------------------------------


@dataclass(frozen=True)
class _Source:
    pack_name: str
    source: str


def test_build_grammar_drift_report_flags_empty_sources() -> None:
    """Verify drift report flags empty query sources."""
    report = build_grammar_drift_report(language="python", query_sources=())
    assert report.compatible is False
    assert "query_pack_sources_empty" in report.errors


def test_build_grammar_drift_report_accepts_scm_sources() -> None:
    """Verify drift report works with valid SCM sources."""
    report = build_grammar_drift_report(
        language="rust",
        query_sources=(_Source("10_refs.scm", "(identifier) @ref"),),
    )
    assert report.grammar_digest
    assert report.query_digest
