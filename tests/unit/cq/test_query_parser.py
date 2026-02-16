"""Test query parser for cq tool.

Verifies that:
1. Query strings are parsed correctly
2. Various query formats are supported
3. Error handling works for invalid queries
"""

from __future__ import annotations

from typing import Any, cast

import pytest
from tools.cq.query.ir import Expander, Query, Scope
from tools.cq.query.parser import QueryParseError, parse_query

DEPTH_THREE = 3
TWO_EXPANDERS = 2
LIMIT_TEN = 10
LIMIT_TWENTY = 20
SECOND_EXPANDER_INDEX = 1
SECOND_DEPTH = 2


class TestParseQuery:
    """Tests for parse_query function."""

    @staticmethod
    def test_basic_entity_query() -> None:
        """Parse minimal entity query."""
        query = parse_query("entity=function")
        assert query.entity == "function"
        assert query.name is None
        assert query.expand == ()
        assert query.fields == ("def",)

    @staticmethod
    def test_entity_with_name() -> None:
        """Parse entity query with name."""
        query = parse_query("entity=function name=foo")
        assert query.entity == "function"
        assert query.name == "foo"

    @staticmethod
    def test_entity_with_regex_name() -> None:
        """Parse entity query with regex name pattern."""
        query = parse_query("entity=function name=~test_.*")
        assert query.name == "~test_.*"

    @staticmethod
    def test_entity_with_scope() -> None:
        """Parse entity query with scope constraints."""
        query = parse_query("entity=function in=src/cli/")
        assert query.scope.in_dir == "src/cli/"

    @staticmethod
    def test_entity_with_exclude() -> None:
        """Parse entity query with exclusions."""
        query = parse_query("entity=function exclude=tests,build")
        assert query.scope.exclude == ("tests", "build")

    @staticmethod
    def test_entity_with_expander() -> None:
        """Parse entity query with single expander."""
        query = parse_query("entity=function expand=callers")
        assert len(query.expand) == 1
        assert query.expand[0].kind == "callers"
        assert query.expand[0].depth == 1

    @staticmethod
    def test_entity_with_expander_depth() -> None:
        """Parse entity query with expander and custom depth."""
        query = parse_query("entity=function expand=callers(depth=3)")
        assert query.expand[0].depth == DEPTH_THREE

    @staticmethod
    def test_entity_with_multiple_expanders() -> None:
        """Parse entity query with multiple expanders."""
        query = parse_query("entity=function expand=callers(depth=2),callees(depth=1)")
        assert len(query.expand) == TWO_EXPANDERS
        assert query.expand[0].kind == "callers"
        assert query.expand[0].depth == SECOND_DEPTH
        assert query.expand[SECOND_EXPANDER_INDEX].kind == "callees"
        assert query.expand[1].depth == 1

    @staticmethod
    def test_entity_with_fields() -> None:
        """Parse entity query with output fields."""
        query = parse_query("entity=function fields=def,callers,imports")
        assert query.fields == ("def", "callers", "imports")

    @staticmethod
    def test_entity_with_limit() -> None:
        """Parse entity query with result limit."""
        query = parse_query("entity=function limit=10")
        assert query.limit == LIMIT_TEN

    @staticmethod
    def test_entity_with_explain() -> None:
        """Parse entity query with explain flag."""
        query = parse_query("entity=function explain=true")
        assert query.explain is True

    @staticmethod
    def test_full_query() -> None:
        """Parse full query with all options."""
        query = parse_query(
            "entity=function name=build expand=callers(depth=2) "
            "in=src/ exclude=tests fields=def,imports limit=20"
        )
        assert query.entity == "function"
        assert query.name == "build"
        assert query.expand[0].kind == "callers"
        assert query.expand[0].depth == SECOND_DEPTH
        assert query.scope.in_dir == "src/"
        assert query.scope.exclude == ("tests",)
        assert query.fields == ("def", "imports")
        assert query.limit == LIMIT_TWENTY

    @staticmethod
    def test_class_entity() -> None:
        """Parse class entity query."""
        query = parse_query("entity=class name=MyClass")
        assert query.entity == "class"
        assert query.name == "MyClass"

    @staticmethod
    def test_import_entity() -> None:
        """Parse import entity query."""
        query = parse_query("entity=import in=src/")
        assert query.entity == "import"

    @staticmethod
    def test_pattern_query_with_nth_child() -> None:
        """Parse pattern query with nthChild options."""
        query = parse_query(
            "pattern='print($A)' nthChild=2 nthChild.reverse=true nthChild.ofRule='kind=argument'"
        )
        assert query.nth_child is not None
        assert query.nth_child.position == TWO_EXPANDERS
        assert query.nth_child.reverse is True
        assert query.nth_child.of_rule == "kind=argument"


class TestParseQueryErrors:
    """Tests for parse_query error handling."""

    @staticmethod
    def test_missing_entity() -> None:
        """Missing entity should raise error."""
        with pytest.raises(QueryParseError, match="must specify 'entity'"):
            parse_query("name=foo")

    @staticmethod
    def test_invalid_entity() -> None:
        """Invalid entity type should raise error."""
        with pytest.raises(QueryParseError, match="Invalid entity type"):
            parse_query("entity=foobar")

    @staticmethod
    def test_invalid_expander() -> None:
        """Invalid expander kind should raise error."""
        with pytest.raises(QueryParseError, match="Invalid expander kind"):
            parse_query("entity=function expand=badexpander")

    @staticmethod
    def test_invalid_field() -> None:
        """Invalid field should raise error."""
        with pytest.raises(QueryParseError, match="Invalid field"):
            parse_query("entity=function fields=badfield")


class TestQueryIR:
    """Tests for Query IR dataclasses."""

    @staticmethod
    def test_query_with_scope() -> None:
        """Test Query.with_scope() method."""
        query = Query(entity="function")
        new_scope = Scope(in_dir="src/")
        new_query = query.with_scope(new_scope)

        assert new_query.scope.in_dir == "src/"
        assert query.scope.in_dir is None  # Original unchanged

    @staticmethod
    def test_query_with_expand() -> None:
        """Test Query.with_expand() method."""
        query = Query(entity="function")
        new_query = query.with_expand(Expander(kind="callers", depth=2))

        assert len(new_query.expand) == 1
        assert new_query.expand[0].kind == "callers"
        assert query.expand == ()  # Original unchanged

    @staticmethod
    def test_query_with_fields() -> None:
        """Test Query.with_fields() method."""
        query = Query(entity="function")
        new_query = query.with_fields("def", "imports", "callers")

        assert new_query.fields == ("def", "imports", "callers")
        assert query.fields == ("def",)  # Original unchanged

    @staticmethod
    def test_expander_frozen() -> None:
        """Expander should be immutable."""
        expander = Expander(kind="callers", depth=2)
        with pytest.raises(AttributeError):
            cast("Any", expander).depth = 3

    @staticmethod
    def test_scope_frozen() -> None:
        """Scope should be immutable."""
        scope = Scope(in_dir="src/")
        with pytest.raises(AttributeError):
            cast("Any", scope).in_dir = "other/"
