"""Tests for Ibis macro helpers."""

from __future__ import annotations

from typing import cast

import ibis
from ibis import selectors as s
from ibis.expr.types import Table, Value

from ibis_engine.macros import IbisMacroRewrite, apply_macros, bind_columns


def test_apply_macros_with_selectors() -> None:
    """Apply selector-driven macros across wide schemas."""
    table = ibis.memtable({"a": [1, 2], "b": [3.0, 4.0], "label": ["x", "y"]})

    def center_numeric(expr: Table) -> Table:
        placeholder = ibis._
        across = cast(
            "Value",
            s.across(
                s.numeric(),
                {"centered": placeholder - placeholder.mean()},
                names="{fn}_{col}",
            ),
        )
        return expr.mutate(
            across
        )

    result = apply_macros(table, macros=[center_numeric])
    assert "centered_a" in result.columns
    assert "centered_b" in result.columns


def test_apply_macro_rewrite_substitute() -> None:
    """Apply rewrite macros via substitution mappings."""
    table = ibis.memtable({"a": [1, 2], "b": [3, 4]})
    (bound_a,) = bind_columns(table, "a")
    rewrite = IbisMacroRewrite({bound_a: cast("Value", table.a.cast("int64"))})
    result = apply_macros(table, macros=[rewrite])
    assert isinstance(result, Table)
