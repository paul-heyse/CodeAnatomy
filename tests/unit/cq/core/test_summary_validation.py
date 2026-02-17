"""Unit tests for summary validation helpers."""

from __future__ import annotations

from tools.cq.core.summary_contract import summary_for_variant
from tools.cq.core.summary_validation import validate_summary_envelope, validate_summary_sections


def test_validate_summary_envelope_accepts_default_summary() -> None:
    """Default summary envelopes should pass structural validation."""
    summary = summary_for_variant("search")
    errors = validate_summary_envelope(summary)
    assert errors == []


def test_validate_summary_sections_reports_non_string() -> None:
    """Section validation should report non-string section entries."""
    errors = validate_summary_sections(("ok", 1))
    assert errors
