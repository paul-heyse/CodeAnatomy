# ruff: noqa: PLR6301
"""Tests for Rust match-centric injection planning."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.search.tree_sitter.rust_lane.injections import (
    build_injection_plan_from_matches,
)


@dataclass(frozen=True)
class _FakeNode:
    type: str
    start_byte: int
    end_byte: int
    start_point: tuple[int, int]
    end_point: tuple[int, int]

    def child_by_field_name(self, name: str, /) -> _FakeNode | None:
        _ = name
        return None


class _FakeQuery:
    def __init__(self, rows: dict[int, dict[str, object]]) -> None:
        self._rows = rows

    def pattern_settings(self, pattern_idx: int) -> dict[str, object]:
        return self._rows.get(pattern_idx, {})


def test_build_injection_plan_from_matches_uses_pattern_settings_and_macro_profile() -> None:
    """Test build injection plan from matches uses pattern settings and macro profile."""
    query = _FakeQuery(
        {
            0: {
                "injection.language": "sql",
                "injection.combined": None,
                "injection.include-children": None,
            }
        }
    )
    source = b'sql!("select 1")'
    matches = [
        (
            0,
            {
                "injection.content": [_FakeNode("string_literal", 5, 16, (0, 5), (0, 16))],
                "injection.macro.name": [_FakeNode("macro_identifier", 0, 3, (0, 0), (0, 3))],
            },
        )
    ]
    plans = build_injection_plan_from_matches(
        query=query,
        matches=matches,
        source_bytes=source,
        default_language="rust",
    )
    assert len(plans) == 1
    assert plans[0].language == "sql"
    assert plans[0].combined is True
    assert plans[0].include_children is True
    assert plans[0].profile_name == "sql"


def test_build_injection_plan_from_matches_reads_captured_language() -> None:
    """Test build injection plan from matches reads captured language."""
    query = _FakeQuery({0: {}})
    source = b"lang!(python, code)"
    matches = [
        (
            0,
            {
                "injection.content": [_FakeNode("string_literal", 12, 16, (0, 12), (0, 16))],
                "injection.language": [_FakeNode("identifier", 6, 12, (0, 6), (0, 12))],
            },
        )
    ]
    plans = build_injection_plan_from_matches(
        query=query,
        matches=matches,
        source_bytes=source,
        default_language="rust",
    )
    assert len(plans) == 1
    assert plans[0].language == "python"
