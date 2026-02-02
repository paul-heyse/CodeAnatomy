"""Tests for join-based queries.

Verifies:
1. Join target parsing
2. Join constraint construction
3. Join query parsing
"""

from __future__ import annotations

from tools.cq.query.ir import JoinConstraint, JoinTarget, Query
from tools.cq.query.parser import parse_query


class TestJoinTarget:
    """Tests for JoinTarget dataclass."""

    def test_simple_join_target(self) -> None:
        """Create simple join target."""
        target = JoinTarget(entity="function", name=None)
        assert target.entity == "function"
        assert target.name is None

    def test_join_target_with_name(self) -> None:
        """Create join target with name filter."""
        target = JoinTarget(entity="function", name="foo")
        assert target.entity == "function"
        assert target.name == "foo"

    def test_parse_simple_target(self) -> None:
        """Parse simple target specification."""
        target = JoinTarget.parse("function")
        assert target.entity == "function"
        assert target.name is None

    def test_parse_target_with_name(self) -> None:
        """Parse target specification with name."""
        target = JoinTarget.parse("function:foo")
        assert target.entity == "function"
        assert target.name == "foo"


class TestJoinConstraint:
    """Tests for JoinConstraint dataclass."""

    def test_used_by_constraint(self) -> None:
        """Create used_by join constraint."""
        constraint = JoinConstraint(
            join_type="used_by",
            target=JoinTarget(entity="function", name=None),
        )
        assert constraint.join_type == "used_by"
        assert constraint.target.entity == "function"

    def test_defines_constraint(self) -> None:
        """Create defines join constraint."""
        constraint = JoinConstraint(
            join_type="defines",
            target=JoinTarget(entity="class", name="MyClass"),
        )
        assert constraint.join_type == "defines"
        assert constraint.target.name == "MyClass"


class TestJoinQueryParsing:
    """Tests for parsing join queries."""

    def test_used_by_parsing(self) -> None:
        """Parse query with used_by constraint."""
        query = parse_query("entity=function used_by=function:main")
        assert len(query.joins) == 1
        assert query.joins[0].join_type == "used_by"
        assert query.joins[0].target.entity == "function"
        assert query.joins[0].target.name == "main"

    def test_defines_parsing(self) -> None:
        """Parse query with defines constraint."""
        query = parse_query("entity=module defines=class:Config")
        assert len(query.joins) == 1
        assert query.joins[0].join_type == "defines"
        assert query.joins[0].target.name == "Config"

    def test_raises_parsing(self) -> None:
        """Parse query with raises constraint."""
        query = parse_query("entity=function raises=class:ValueError")
        assert len(query.joins) == 1
        assert query.joins[0].join_type == "raises"

    def test_exports_parsing(self) -> None:
        """Parse query with exports constraint."""
        query = parse_query("entity=module exports=function:main")
        assert len(query.joins) == 1
        assert query.joins[0].join_type == "exports"

    def test_multiple_joins(self) -> None:
        """Parse query with multiple join constraints."""
        query = parse_query("entity=function used_by=function:main raises=class:Error")
        assert len(query.joins) == 2

        types = {j.join_type for j in query.joins}
        assert types == {"used_by", "raises"}


class TestJoinQueryIR:
    """Tests for join query IR construction."""

    def test_query_with_joins(self) -> None:
        """Create query with join constraints."""
        query = Query(
            entity="function",
            joins=(
                JoinConstraint(
                    join_type="used_by",
                    target=JoinTarget(entity="function", name="main"),
                ),
            ),
        )
        assert len(query.joins) == 1

    def test_query_default_empty_joins(self) -> None:
        """Query has empty joins by default."""
        query = Query(entity="function")
        assert query.joins == ()
