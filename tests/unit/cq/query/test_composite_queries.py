"""Composite query parser regression tests."""

from __future__ import annotations

from tools.cq.query.parser import parse_query


def test_parse_top_level_any_composite_query() -> None:
    """Test parse top level any composite query."""
    query = parse_query("any='logger.$M($$$),print($$$)'")
    assert query.is_pattern_query
    assert query.composite is not None
    assert query.composite.operator == "any"
    assert query.composite.patterns == ("logger.$M($$$)", "print($$$)")


def test_parse_top_level_all_composite_with_scope() -> None:
    """Test parse top level all composite with scope."""
    query = parse_query("all='await $X,return $Y' in=src lang=auto")
    assert query.is_pattern_query
    assert query.composite is not None
    assert query.composite.operator == "all"
    assert query.scope.in_dir == "src"
    assert query.lang_scope == "auto"


def test_parse_entity_query_with_composite_still_entity_mode() -> None:
    """Test parse entity query with composite still entity mode."""
    query = parse_query("entity=function any='await $X,return $Y'")
    assert not query.is_pattern_query
    assert query.entity == "function"
    assert query.composite is not None
    assert query.composite.operator == "any"
