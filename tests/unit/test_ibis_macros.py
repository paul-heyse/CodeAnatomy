"""Unit tests for Ibis macro helpers."""

from __future__ import annotations

from datetime import UTC, date, datetime, time

import ibis
from ibis import selectors as s
from ibis.expr.types import Table

from ibis_engine.macros import IbisMacroRewrite, apply_macros, bind_columns
from ibis_engine.selector_utils import SelectorPattern, selector_from_pattern


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


def test_selector_from_pattern_variants() -> None:
    """Select columns with numeric/string/temporal/regex patterns."""
    table = ibis.memtable(
        {
            "num": [1],
            "txt": ["a"],
            "when": [datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)],
            "day": [date(2024, 1, 1)],
            "clock": [time(12, 0, 0)],
            "flag": [True],
        }
    )
    numeric = table.bind(
        selector_from_pattern(SelectorPattern(name="numeric", column_type="numeric"))
    )
    assert [col.get_name() for col in numeric] == ["num"]
    strings = table.bind(
        selector_from_pattern(SelectorPattern(name="string", column_type="string"))
    )
    assert [col.get_name() for col in strings] == ["txt"]
    temporal = table.bind(
        selector_from_pattern(SelectorPattern(name="temporal", column_type="temporal"))
    )
    assert [col.get_name() for col in temporal] == ["when", "day", "clock"]
    regex = table.bind(selector_from_pattern(SelectorPattern(name="regex", regex_pattern="^t")))
    assert [col.get_name() for col in regex] == ["txt"]
