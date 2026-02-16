"""Tests for shared normalization helpers."""

from __future__ import annotations

import pytest
from datafusion import SessionContext, lit

from semantics.normalization_helpers import LineIndexJoinOptions, byte_offset_expr, line_index_join

EXPECTED_END_LINE_START_BYTE = 10


def test_line_index_join_by_file_id() -> None:
    """line_index_join resolves start/end line metadata by file_id."""
    ctx = SessionContext()
    ctx.from_pydict(
        {
            "file_id": ["f1"],
            "start_line": [1],
            "end_line": [2],
        },
        name="events",
    )
    ctx.from_pydict(
        {
            "file_id": ["f1", "f1"],
            "path": ["a.py", "a.py"],
            "line_no": [1, 2],
            "line_start_byte": [0, 10],
            "line_text": ["abc", "def"],
        },
        name="line_index",
    )

    joined = line_index_join(
        ctx.table("events"),
        "line_index",
        options=LineIndexJoinOptions(
            start_line_col="start_line",
            end_line_col="end_line",
            ctx=ctx,
        ),
    )

    row = joined.to_arrow_table().to_pylist()[0]
    assert row["start_line_start_byte"] == 0
    assert row["end_line_start_byte"] == EXPECTED_END_LINE_START_BYTE


def test_line_index_join_requires_file_or_path() -> None:
    """line_index_join raises when neither file_id nor path exists."""
    ctx = SessionContext()
    ctx.from_pydict({"start_line": [1], "end_line": [1]}, name="events")
    ctx.from_pydict(
        {
            "file_id": ["f1"],
            "path": ["a.py"],
            "line_no": [1],
            "line_start_byte": [0],
            "line_text": ["abc"],
        },
        name="line_index",
    )

    with pytest.raises(ValueError, match="file_id or path"):
        _ = line_index_join(
            ctx.table("events"),
            "line_index",
            options=LineIndexJoinOptions(
                start_line_col="start_line",
                end_line_col="end_line",
                ctx=ctx,
            ),
        )


def test_byte_offset_expr_uses_col_to_byte_udf() -> None:
    """byte_offset_expr builds a UDF-based offset expression."""
    expr = byte_offset_expr(
        "line_start",
        "line_text",
        "char_no",
        lit("utf16"),
    )

    assert "col_to_byte" in str(expr)
