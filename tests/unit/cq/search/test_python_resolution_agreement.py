"""Tests for python_resolution agreement metadata in Python enrichment."""

from __future__ import annotations

from tools.cq.search.python.extractors import _build_agreement_section, _build_stage_facts


def test_agreement_full_with_python_resolution_source() -> None:
    """Test agreement full with python resolution source."""
    agreement = _build_agreement_section(
        ast_stage=_build_stage_facts({"call_target": "pkg.fn"}),
        python_resolution_stage=_build_stage_facts({"symbol_role": "read"}),
        tree_sitter_stage=_build_stage_facts({"scope_kind": "function"}),
    )
    assert agreement["status"] == "full"
    sources = agreement["sources"]
    assert isinstance(sources, list)
    assert "python_resolution" in sources
    assert "libcst" not in sources


def test_agreement_conflict_uses_python_resolution_key() -> None:
    """Test agreement conflict uses python resolution key."""
    agreement = _build_agreement_section(
        ast_stage=_build_stage_facts({"enclosing_callable": "pkg.read"}),
        python_resolution_stage=_build_stage_facts({"enclosing_callable": "pkg.write"}),
        tree_sitter_stage=_build_stage_facts({}),
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
    """Test agreement partial when resolution missing."""
    agreement = _build_agreement_section(
        ast_stage=_build_stage_facts({"call_target": "pkg.fn"}),
        python_resolution_stage=_build_stage_facts({}),
        tree_sitter_stage=_build_stage_facts({}),
    )
    assert agreement["status"] == "partial"
