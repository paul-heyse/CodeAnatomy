"""Tests for flattened tags module (contracts and runtime)."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.search.tree_sitter.tags import RustTagEventV1, build_tag_events

EXPECTED_TAG_ROWS = 2


@dataclass(frozen=True)
class _FakeNode:
    """Minimal node stub for tag event building."""

    start_byte: int
    end_byte: int
    type: str = "identifier"
    start_point: tuple[int, int] = (0, 0)
    end_point: tuple[int, int] = (0, 0)

    @staticmethod
    def child_by_field_name(name: str, /) -> _FakeNode | None:
        _ = name
        return None


def test_rust_tag_event_v1_is_importable() -> None:
    """Verify the contract type is available from the flattened module."""
    event = RustTagEventV1(role="definition", kind="symbol", name="demo", start_byte=0, end_byte=4)
    assert event.role == "definition"


def test_build_tag_events_emits_definition_and_reference_rows() -> None:
    """Verify build_tag_events produces rows from match data."""
    source_bytes = b"fn demo() { call(); }"
    matches = [
        (0, {"role.definition": [_FakeNode(3, 7)], "name": [_FakeNode(3, 7)]}),
        (1, {"role.reference": [_FakeNode(12, 16)], "name": [_FakeNode(12, 16)]}),
    ]
    rows = build_tag_events(matches=matches, source_bytes=source_bytes)
    assert len(rows) == EXPECTED_TAG_ROWS
    assert rows[0].role == "definition"
    assert rows[0].name == "demo"
    assert rows[1].role == "reference"


def test_build_tag_events_skips_matches_without_role() -> None:
    """Verify matches without role.definition/role.reference are skipped."""
    source_bytes = b"fn demo() {}"
    matches = [(0, {"name": [_FakeNode(3, 7)]})]
    rows = build_tag_events(matches=matches, source_bytes=source_bytes)
    assert len(rows) == 0


def test_build_tag_events_supports_distribution_taxonomy_and_name_fallback() -> None:
    """Test build tag events supports distribution taxonomy and name fallback."""
    source_bytes = b"fn demo() { demo(); }"
    matches = [
        (
            0,
            {
                "definition.function": [_FakeNode(3, 7)],
                "definition.function.name": [_FakeNode(3, 7)],
            },
        ),
        (
            1,
            {
                "reference.call": [_FakeNode(12, 16)],
                "reference.call.name": [_FakeNode(12, 16)],
            },
        ),
    ]
    rows = build_tag_events(matches=matches, source_bytes=source_bytes)
    assert len(rows) == EXPECTED_TAG_ROWS
    assert rows[0].role == "definition"
    assert rows[0].name == "demo"
    assert rows[1].role == "reference"
    assert rows[1].name == "demo"
