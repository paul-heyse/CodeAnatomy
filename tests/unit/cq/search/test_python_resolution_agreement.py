"""Tests for python_resolution agreement metadata in Python enrichment."""

from __future__ import annotations

from tools.cq.search.python.extractors import _build_agreement_section


def test_agreement_full_with_python_resolution_source() -> None:
    agreement = _build_agreement_section(
        ast_fields={"call_target": "pkg.fn"},
        python_resolution_fields={"symbol_role": "read"},
        tree_sitter_fields={"parse_quality": {"has_error": False}},
    )
    assert agreement["status"] == "full"
    sources = agreement["sources"]
    assert isinstance(sources, list)
    assert "python_resolution" in sources
    assert "libcst" not in sources


def test_agreement_conflict_uses_python_resolution_key() -> None:
    agreement = _build_agreement_section(
        ast_fields={"symbol_role": "read"},
        python_resolution_fields={"symbol_role": "write"},
        tree_sitter_fields={},
    )
    assert agreement["status"] == "conflict"
    conflicts = agreement["conflicts"]
    assert isinstance(conflicts, list)
    assert conflicts
    first = conflicts[0]
    assert isinstance(first, dict)
    assert "python_resolution" in first
    assert "libcst" not in first


def test_agreement_partial_when_resolution_missing() -> None:
    agreement = _build_agreement_section(
        ast_fields={"call_target": "pkg.fn"},
        python_resolution_fields={},
        tree_sitter_fields={},
    )
    assert agreement["status"] == "partial"
