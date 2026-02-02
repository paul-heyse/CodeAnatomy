"""Test query parser for cq tool.

Verifies that:
1. Query strings are parsed correctly
2. Various query formats are supported
3. Error handling works for invalid queries
"""

from __future__ import annotations

import pytest
from tools.cq.query.ir import Expander, Query, Scope
from tools.cq.query.parser import QueryParseError, parse_query


class TestParseQuery:
    """Tests for parse_query function."""

    def test_basic_entity_query(self) -> None:
        """Parse minimal entity query."""
        query = parse_query("entity=function")
        assert query.entity == "function"
        assert query.name is None
        assert query.expand == ()
        assert query.fields == ("def",)

    def test_entity_with_name(self) -> None:
        """Parse entity query with name."""
        query = parse_query("entity=function name=foo")
        assert query.entity == "function"
        assert query.name == "foo"

    def test_entity_with_regex_name(self) -> None:
        """Parse entity query with regex name pattern."""
        query = parse_query("entity=function name=~test_.*")
        assert query.name == "~test_.*"

    def test_entity_with_scope(self) -> None:
        """Parse entity query with scope constraints."""
        query = parse_query("entity=function in=src/cli/")
        assert query.scope.in_dir == "src/cli/"

    def test_entity_with_exclude(self) -> None:
        """Parse entity query with exclusions."""
        query = parse_query("entity=function exclude=tests,build")
        assert query.scope.exclude == ("tests", "build")

    def test_entity_with_expander(self) -> None:
        """Parse entity query with single expander."""
        query = parse_query("entity=function expand=callers")
        assert len(query.expand) == 1
        assert query.expand[0].kind == "callers"
        assert query.expand[0].depth == 1

    def test_entity_with_expander_depth(self) -> None:
        """Parse entity query with expander and custom depth."""
        query = parse_query("entity=function expand=callers(depth=3)")
        assert query.expand[0].depth == 3

    def test_entity_with_multiple_expanders(self) -> None:
        """Parse entity query with multiple expanders."""
        query = parse_query("entity=function expand=callers(depth=2),callees(depth=1)")
        assert len(query.expand) == 2
        assert query.expand[0].kind == "callers"
        assert query.expand[0].depth == 2
        assert query.expand[1].kind == "callees"
        assert query.expand[1].depth == 1

    def test_entity_with_fields(self) -> None:
        """Parse entity query with output fields."""
        query = parse_query("entity=function fields=def,callers,hazards")
        assert query.fields == ("def", "callers", "hazards")

    def test_entity_with_limit(self) -> None:
        """Parse entity query with result limit."""
        query = parse_query("entity=function limit=10")
        assert query.limit == 10

    def test_entity_with_explain(self) -> None:
        """Parse entity query with explain flag."""
        query = parse_query("entity=function explain=true")
        assert query.explain is True

    def test_full_query(self) -> None:
        """Parse full query with all options."""
        query = parse_query(
            "entity=function name=build expand=callers(depth=2) "
            "in=src/ exclude=tests fields=def,hazards limit=20"
        )
        assert query.entity == "function"
        assert query.name == "build"
        assert query.expand[0].kind == "callers"
        assert query.expand[0].depth == 2
        assert query.scope.in_dir == "src/"
        assert query.scope.exclude == ("tests",)
        assert query.fields == ("def", "hazards")
        assert query.limit == 20

    def test_class_entity(self) -> None:
        """Parse class entity query."""
        query = parse_query("entity=class name=MyClass")
        assert query.entity == "class"
        assert query.name == "MyClass"

    def test_import_entity(self) -> None:
        """Parse import entity query."""
        query = parse_query("entity=import in=src/")
        assert query.entity == "import"


class TestParseQueryErrors:
    """Tests for parse_query error handling."""

    def test_missing_entity(self) -> None:
        """Missing entity should raise error."""
        with pytest.raises(QueryParseError, match="must specify 'entity'"):
            parse_query("name=foo")

    def test_invalid_entity(self) -> None:
        """Invalid entity type should raise error."""
        with pytest.raises(QueryParseError, match="Invalid entity type"):
            parse_query("entity=foobar")

    def test_invalid_expander(self) -> None:
        """Invalid expander kind should raise error."""
        with pytest.raises(QueryParseError, match="Invalid expander kind"):
            parse_query("entity=function expand=badexpander")

    def test_invalid_field(self) -> None:
        """Invalid field should raise error."""
        with pytest.raises(QueryParseError, match="Invalid field"):
            parse_query("entity=function fields=badfield")


class TestQueryIR:
    """Tests for Query IR dataclasses."""

    def test_query_with_scope(self) -> None:
        """Test Query.with_scope() method."""
        query = Query(entity="function")
        new_scope = Scope(in_dir="src/")
        new_query = query.with_scope(new_scope)

        assert new_query.scope.in_dir == "src/"
        assert query.scope.in_dir is None  # Original unchanged

    def test_query_with_expand(self) -> None:
        """Test Query.with_expand() method."""
        query = Query(entity="function")
        new_query = query.with_expand(Expander(kind="callers", depth=2))

        assert len(new_query.expand) == 1
        assert new_query.expand[0].kind == "callers"
        assert query.expand == ()  # Original unchanged

    def test_query_with_fields(self) -> None:
        """Test Query.with_fields() method."""
        query = Query(entity="function")
        new_query = query.with_fields("def", "hazards", "callers")

        assert new_query.fields == ("def", "hazards", "callers")
        assert query.fields == ("def",)  # Original unchanged

    def test_expander_frozen(self) -> None:
        """Expander should be immutable."""
        expander = Expander(kind="callers", depth=2)
        with pytest.raises(AttributeError):
            expander.depth = 3  # type: ignore[misc]

    def test_scope_frozen(self) -> None:
        """Scope should be immutable."""
        scope = Scope(in_dir="src/")
        with pytest.raises(AttributeError):
            scope.in_dir = "other/"  # type: ignore[misc]
