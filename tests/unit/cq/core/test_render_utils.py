"""Tests for shared render utility helpers."""

from __future__ import annotations

from tools.cq.core.render_utils import (
    clean_scalar,
    extract_symbol_hint,
    format_location,
    iter_result_findings,
    na,
    safe_int,
)
from tools.cq.core.schema import CqResult, DetailPayload, Finding, RunMeta, Section

SAMPLE_INT_VALUE = 3
SAMPLE_FLOAT_VALUE = 3.5


def _run_meta() -> RunMeta:
    """Build a minimal run metadata payload for rendering tests.

    Returns:
        RunMeta: Minimal CQ run metadata fixture.
    """
    return RunMeta(
        macro="q",
        argv=["cq", "q", "entity=function"],
        root=".",
        started_ms=0.0,
        elapsed_ms=1.0,
        toolchain={},
    )


def test_na_formats_reason_text() -> None:
    """`na` should normalize underscore-delimited reasons."""
    assert na("not_applicable") == "N/A - not applicable"


def test_clean_scalar_normalizes_scalars() -> None:
    """`clean_scalar` should normalize known scalar types."""
    assert clean_scalar("  ok  ") == "ok"
    assert clean_scalar(value=True) == "yes"
    assert clean_scalar(value=False) == "no"
    assert clean_scalar(SAMPLE_FLOAT_VALUE) == "3.5"
    assert clean_scalar([]) is None


def test_safe_int_rejects_bool() -> None:
    """`safe_int` should reject booleans while accepting true ints."""
    assert safe_int(SAMPLE_INT_VALUE) == SAMPLE_INT_VALUE
    assert safe_int(value=True) is None
    assert safe_int("3") is None


def test_format_location_handles_optional_inputs() -> None:
    """`format_location` should tolerate partially missing location fields."""
    assert format_location("src/a.py", 10, 2) == "src/a.py:10:2"
    assert format_location("src/a.py", None, None) == "src/a.py"
    assert format_location(None, None, None) is None


def test_extract_symbol_hint_prefers_detail_fields() -> None:
    """`extract_symbol_hint` should use explicit detail fields first."""
    finding = Finding(
        category="callsite",
        message="call: fallback",
        details=DetailPayload(data_items=(("name", "target"),)),
    )
    assert extract_symbol_hint(finding) == "target"


def test_iter_result_findings_preserves_section_order() -> None:
    """`iter_result_findings` should preserve key, section, then evidence order."""
    key = Finding(category="definition", message="function: key")
    section_finding = Finding(category="callsite", message="call: section")
    evidence = Finding(category="evidence", message="evidence: sample")
    result = CqResult(
        run=_run_meta(),
        key_findings=(key,),
        sections=(Section(title="Section", findings=(section_finding,)),),
        evidence=(evidence,),
    )
    assert iter_result_findings(result) == [key, section_finding, evidence]
