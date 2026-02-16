# ruff: noqa: D103
"""Tests for shared line-offset extraction helper."""

from __future__ import annotations

from extract.coordination.context import FileContext
from extract.coordination.line_offsets import line_offsets_from_file_ctx


def test_line_offsets_from_file_context() -> None:
    code = "a\n b\n"
    file_ctx = FileContext(
        file_id="f",
        path="x.py",
        abs_path=None,
        file_sha256=None,
        encoding="utf-8",
        text=code,
        data=code.encode("utf-8"),
    )
    offsets = line_offsets_from_file_ctx(file_ctx)
    assert offsets is not None
