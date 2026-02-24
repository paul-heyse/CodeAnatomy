"""Tests for query result section fallback helpers."""

from __future__ import annotations

import msgspec
from tools.cq.core.schema import CqResult, Finding, Section, mk_result, mk_runmeta
from tools.cq.query.section_fallbacks import ensure_query_sections


def _base_result() -> CqResult:
    run = mk_runmeta(
        macro="q",
        argv=["cq", "q", "entity=function"],
        root=".",
        started_ms=0.0,
        toolchain={},
        run_id="run-1",
    )
    return mk_result(run)


def test_ensure_query_sections_adds_findings_section_when_missing() -> None:
    """A non-empty finding set should produce one fallback section."""
    result = _base_result()
    result_with_findings = msgspec.structs.replace(
        result,
        key_findings=(Finding(category="definition", message="found"),),
        sections=(),
    )

    updated = ensure_query_sections(result_with_findings, title="Findings")
    assert len(updated.sections) == 1
    assert updated.sections[0].title == "Findings"
    assert len(updated.sections[0].findings) == 1


def test_ensure_query_sections_preserves_existing_sections() -> None:
    """Existing sections should not be replaced by fallback."""
    result = _base_result()
    with_section = msgspec.structs.replace(
        result,
        key_findings=(Finding(category="definition", message="found"),),
        sections=(Section(title="Neighborhood Preview"),),
    )

    updated = ensure_query_sections(with_section, title="Findings")
    assert len(updated.sections) == 1
    assert updated.sections[0].title == "Neighborhood Preview"
