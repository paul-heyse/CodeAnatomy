"""Tests for normalize span expression helpers."""

from __future__ import annotations

import ibis

from datafusion_engine.span_utils import ENC_UTF8, ENC_UTF16, ENC_UTF32
from normalize.span_logic import normalize_col_unit_expr


def test_normalize_col_unit_expr_with_position_encoding() -> None:
    """Normalize col-unit expressions with encoding fallback."""
    backend = ibis.datafusion.connect()
    table = ibis.memtable(
        {
            "col_unit": [None, "UTF8", "byte", "2", "utf16", "garbage", None],
            "position_encoding": [
                ENC_UTF16,
                None,
                None,
                ENC_UTF8,
                None,
                None,
                ENC_UTF32,
            ],
        }
    )
    expr = table.select(
        norm=normalize_col_unit_expr(
            table.col_unit,
            position_encoding=table.position_encoding,
        )
    )
    result = backend.execute(expr)
    assert result["norm"].tolist() == [
        "utf16",
        "utf8",
        "byte",
        "utf16",
        "utf16",
        "utf32",
        "utf32",
    ]
