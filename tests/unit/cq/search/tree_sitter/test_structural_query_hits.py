"""Tests for typed structural query-hit export helpers."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.search.tree_sitter.structural.exports import export_query_hits


@dataclass(frozen=True)
class _FakeNode:
    type: str
    start_byte: int
    end_byte: int


def test_export_query_hits_emits_typed_rows() -> None:
    """Test export query hits emits typed rows."""
    matches = [
        (
            1,
            {
                "name": [_FakeNode(type="identifier", start_byte=4, end_byte=7)],
            },
        )
    ]

    rows = export_query_hits(file_path="sample.rs", matches=matches)

    assert len(rows) == 1
    assert rows[0].query_name == "sample.rs"
    assert rows[0].pattern_index == 1
    assert rows[0].capture_name == "name"
    assert rows[0].node_id == "sample.rs:4:7:identifier"
