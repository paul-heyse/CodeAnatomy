"""Tests for frozen Section contracts."""

from __future__ import annotations

import pytest
from tools.cq.core.schema import Finding, Section, append_section_finding


def test_section_is_frozen_and_tuple_backed() -> None:
    """Section defaults to immutable tuple-backed findings and frozen fields."""
    section = Section(title="Example")

    assert section.findings == ()
    assert isinstance(section.findings, tuple)

    expected_errors: tuple[type[BaseException], ...] = (AttributeError, TypeError)
    field_name = "title"
    with pytest.raises(expected_errors):
        setattr(section, field_name, "Updated")


def test_append_section_finding_returns_new_section() -> None:
    """Appending a finding returns a new section instance."""
    section = Section(title="Example")
    finding = Finding(category="test", message="message")

    updated = append_section_finding(section, finding)

    assert section.findings == ()
    assert updated.findings == (finding,)
