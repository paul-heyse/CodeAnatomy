"""Tests for grammar drift compatibility reporting."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.search.tree_sitter_grammar_drift import build_grammar_drift_report


@dataclass(frozen=True)
class _Source:
    pack_name: str
    source: str


def test_build_grammar_drift_report_flags_empty_sources() -> None:
    report = build_grammar_drift_report(language="python", query_sources=())
    assert report.compatible is False
    assert "query_pack_sources_empty" in report.errors


def test_build_grammar_drift_report_accepts_scm_sources() -> None:
    report = build_grammar_drift_report(
        language="rust",
        query_sources=(_Source("10_refs.scm", "(identifier) @ref"),),
    )
    assert report.grammar_digest
    assert report.query_digest
