"""Tests for Python enrichment cross-source agreement helpers."""

from __future__ import annotations

from tools.cq.search.python.extractors_agreement import build_agreement_section


def test_build_agreement_section_reports_full_when_all_sources_match() -> None:
    """Verify full agreement status when all enrichment planes match."""
    agreement = build_agreement_section(
        ast_fields={"node_kind": "function_definition"},
        python_resolution_fields={"node_kind": "function_definition"},
        tree_sitter_fields={"node_kind": "function_definition"},
    )

    assert agreement["status"] == "full"
    assert agreement["conflicts"] == []


def test_build_agreement_section_reports_conflict_on_mismatch() -> None:
    """Verify conflict status when enrichment planes disagree."""
    agreement = build_agreement_section(
        ast_fields={"node_kind": "class_definition"},
        python_resolution_fields={"node_kind": "function_definition"},
        tree_sitter_fields={"node_kind": "function_definition"},
    )

    assert agreement["status"] == "conflict"
    assert agreement["conflicts"]


def test_build_agreement_section_reports_partial_when_sources_missing() -> None:
    """Verify partial status when one or more enrichment planes are missing."""
    agreement = build_agreement_section(
        ast_fields={"node_kind": "function_definition"},
        python_resolution_fields={},
        tree_sitter_fields={"node_kind": "function_definition"},
    )

    assert agreement["status"] == "partial"
