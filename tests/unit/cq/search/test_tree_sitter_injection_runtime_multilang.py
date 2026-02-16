"""Tests for injection runtime with combined/multilang plan metadata."""

from __future__ import annotations

from typing import Any

import pytest
from tools.cq.search.tree_sitter.rust_lane.injection_runtime import parse_injected_ranges
from tools.cq.search.tree_sitter.rust_lane.injections import InjectionPlanV1


class _Language:
    name = "sql"


def test_parse_injected_ranges_reports_combined_count_when_bindings_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test parse injected ranges reports combined count when bindings missing."""
    import tools.cq.search.tree_sitter.rust_lane.injection_runtime as runtime

    class _Parser:
        def __init__(self, _language: object | None = None) -> None:
            self.included_ranges: list[object] = []

        @staticmethod
        def parse(_source_bytes: bytes) -> object:
            msg = "boom"
            raise RuntimeError(msg)

    monkeypatch.setattr(runtime, "_TreeSitterParser", _Parser)
    monkeypatch.setattr(runtime, "_TreeSitterPoint", lambda row, col: (row, col))
    monkeypatch.setattr(runtime, "_TreeSitterRange", lambda *_args: object())
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
    assert result.errors == ("RuntimeError",)
