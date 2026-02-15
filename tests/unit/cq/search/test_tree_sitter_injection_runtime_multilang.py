"""Tests for injection runtime with combined/multilang plan metadata."""

from __future__ import annotations

from typing import Any

import pytest
from tools.cq.search.tree_sitter_injection_runtime import parse_injected_ranges
from tools.cq.search.tree_sitter_injections import InjectionPlanV1


class _Language:
    name = "sql"


def test_parse_injected_ranges_reports_combined_count_when_bindings_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tools.cq.search.tree_sitter_injection_runtime as runtime

    monkeypatch.setattr(runtime, "_TreeSitterParser", None)
    monkeypatch.setattr(runtime, "_TreeSitterPoint", None)
    monkeypatch.setattr(runtime, "_TreeSitterRange", None)
    language: Any = _Language()
    result = parse_injected_ranges(
        source_bytes=b'sql!("select 1")',
        language=language,
        plans=(
            InjectionPlanV1(
                language="sql",
                start_byte=0,
                end_byte=16,
                profile_name="sql",
                combined=True,
            ),
        ),
    )
    assert result.plan_count == 1
    assert result.combined_count == 1
    assert result.parsed is False
