"""Tests for Rust tags runtime helpers."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.search.tree_sitter.tags import build_tag_events

EXPECTED_TAG_ROWS = 2


@dataclass(frozen=True)
class _FakeNode:
    start_byte: int
    end_byte: int
    type: str = "identifier"
    start_point: tuple[int, int] = (0, 0)
    end_point: tuple[int, int] = (0, 0)

    @staticmethod
    def child_by_field_name(name: str, /) -> _FakeNode | None:
        _ = name
        return None


def test_build_tag_events_emits_definition_and_reference_rows() -> None:
    """Test build tag events emits definition and reference rows."""
    source_bytes = b"fn demo() { call(); }"
    matches = [
        (
            0,
            {
                "role.definition": [_FakeNode(3, 7)],
                "name": [_FakeNode(3, 7)],
            },
        ),
        (
            1,
            {
                "role.reference": [_FakeNode(12, 16)],
                "name": [_FakeNode(12, 16)],
            },
        ),
    ]
    rows = build_tag_events(matches=matches, source_bytes=source_bytes)
    assert len(rows) == EXPECTED_TAG_ROWS
    assert rows[0].role == "definition"
    assert rows[0].name == "demo"
    assert rows[1].role == "reference"
