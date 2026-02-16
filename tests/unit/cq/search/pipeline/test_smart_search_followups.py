"""Tests for smart-search follow-up wrapper module."""

from __future__ import annotations

import pytest
import tools.cq.search.pipeline.smart_search as smart_search_module
from tools.cq.core.schema import Finding
from tools.cq.search._shared.types import QueryMode
from tools.cq.search.pipeline.smart_search_followups import generate_followup_suggestions
from tools.cq.search.pipeline.smart_search_types import EnrichedMatch


def test_generate_followup_suggestions_delegates_to_smart_search(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Follow-up wrapper should delegate to smart_search.build_followups."""
    expected = [Finding(category="followup", message="try calls")]
    monkeypatch.setattr(
        smart_search_module,
        "build_followups",
        lambda *_args, **_kwargs: expected,
    )

    matches: list[EnrichedMatch] = []
    result = generate_followup_suggestions(matches, "target", QueryMode.IDENTIFIER)
    assert result == expected
