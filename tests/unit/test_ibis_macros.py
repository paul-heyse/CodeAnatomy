"""Unit tests for Ibis macro helpers."""

from __future__ import annotations

import ibis
from ibis import selectors as s
from ibis.expr.types import Table

from ibis_engine.macros import IbisMacroRewrite, apply_macros, bind_columns


def test_bind_columns_with_selectors() -> None:
    """Bind selectors and deferred expressions to a table."""
    table = ibis.memtable({"a": [1], "b": [2], "status": ["ok"]})
    bound = bind_columns(table, s.numeric(), ibis._.status)
    names = [expr.get_name() for expr in bound]
    assert names == ["a", "b", "status"]


def test_apply_macros_with_selectors() -> None:
    """Apply macros that use selectors for derived fields."""
    table = ibis.memtable({"a": [1], "b": [2], "status": ["ok"]})

    def _macro(input_table: Table) -> Table:
        first_numeric = bind_columns(input_table, s.numeric())[0]
        return input_table.mutate(first_numeric=first_numeric)

    result = apply_macros(table, macros=(_macro,))
    assert "first_numeric" in result.columns


def test_apply_macro_rewrite_substitute() -> None:
    """Apply rewrite macros via substitution mappings."""
    table = ibis.memtable({"a": [1, 2], "b": [3, 4]})
    (bound_a,) = bind_columns(table, "a")
    rewrite = IbisMacroRewrite({bound_a: table.a.cast("int64")})
    result = apply_macros(table, macros=[rewrite])
    assert isinstance(result, Table)
