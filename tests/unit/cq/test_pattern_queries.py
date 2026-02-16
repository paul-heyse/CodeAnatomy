"""Tests for pattern-based queries.

Verifies:
1. Pattern query IR construction
2. Pattern query parsing
3. Pattern query planning
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

import pytest
from tools.cq.query.ir import PatternSpec, Query
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import AstGrepRule, compile_query


class TestPatternSpec:
    """Tests for PatternSpec dataclass."""

    def test_basic_pattern(self) -> None:
        """Create basic pattern spec."""
        spec = PatternSpec(pattern="def $F($$$)")
        assert spec.pattern == "def $F($$$)"
        assert spec.context is None
        assert spec.selector is None
        assert spec.strictness == "smart"

    def test_pattern_with_context(self) -> None:
        """Create pattern spec with context."""
        spec = PatternSpec(
            pattern="def $F($$$)",
            context="class $C",
        )
        assert spec.context == "class $C"

    def test_pattern_with_strictness(self) -> None:
        """Create pattern spec with custom strictness."""
        spec = PatternSpec(
            pattern="def $F($$$)",
            strictness="ast",
        )
        assert spec.strictness == "ast"


class TestPatternQueryParsing:
    """Tests for parsing pattern queries."""

    def test_simple_pattern(self) -> None:
        """Parse simple pattern query."""
        query = parse_query("pattern='def $F($$$)'")
        assert query.is_pattern_query
        assert query.pattern_spec is not None
        assert query.pattern_spec.pattern == "def $F($$$)"
        assert query.entity is None

    def test_pattern_with_strictness(self) -> None:
        """Parse pattern query with strictness."""
        query = parse_query("pattern='getattr($X, $Y)' strictness=ast")
        assert query.pattern_spec is not None
        assert query.pattern_spec.strictness == "ast"

    def test_pattern_with_context(self) -> None:
        """Parse pattern query with context."""
        query = parse_query("pattern='def $F($$$)' context='class $C'")
        assert query.pattern_spec is not None
        assert query.pattern_spec.context == "class $C"

    def test_pattern_with_scope(self) -> None:
        """Parse pattern query with scope constraints."""
        query = parse_query("pattern='def $F($$$)' in=src/")
        assert query.pattern_spec is not None
        assert query.scope.in_dir == "src/"

    def test_pattern_with_limit(self) -> None:
        """Parse pattern query with limit."""
        query = parse_query("pattern='$X = getattr($Y, $Z)' limit=10")
        assert query.pattern_spec is not None
        assert query.limit == 10

    def test_pattern_with_double_quotes(self) -> None:
        """Parse pattern query with double-quoted value."""
        query = parse_query('pattern="def $F($$$)"')
        assert query.pattern_spec is not None
        assert query.pattern_spec.pattern == "def $F($$$)"


class TestPatternQueryIR:
    """Tests for pattern query IR construction."""

    def test_pattern_query_is_pattern(self) -> None:
        """Pattern query should report is_pattern_query=True."""
        query = Query(
            pattern_spec=PatternSpec(pattern="def $F($$$)"),
        )
        assert query.is_pattern_query

    def test_entity_query_not_pattern(self) -> None:
        """Entity query should report is_pattern_query=False."""
        query = Query(entity="function")
        assert not query.is_pattern_query

    def test_cannot_have_both_entity_and_pattern(self) -> None:
        """Query cannot specify both entity and pattern_spec."""
        with pytest.raises(ValueError, match="cannot specify both"):
            Query(
                entity="function",
                pattern_spec=PatternSpec(pattern="def $F($$$)"),
            )

    def test_must_have_entity_or_pattern(self) -> None:
        """Query must specify either entity or pattern_spec."""
        with pytest.raises(ValueError, match="must specify either"):
            Query()


class TestPatternQueryPlanning:
    """Tests for pattern query compilation."""

    def test_compile_pattern_query(self) -> None:
        """Compile pattern query produces ast-grep rules."""
        query = Query(
            pattern_spec=PatternSpec(pattern="def $F($$$)"),
        )
        plan = compile_query(query)
        assert plan.is_pattern_query
        assert len(plan.sg_rules) == 1
        assert plan.sg_rules[0].pattern == "def $F($$$)"

    def test_pattern_with_context_in_rule(self) -> None:
        """Pattern with context produces rule with context."""
        query = Query(
            pattern_spec=PatternSpec(
                pattern="def $F($$$)",
                context="class $C",
            ),
        )
        plan = compile_query(query)
        assert plan.sg_rules[0].context == "class $C"

    def test_compile_pattern_query_threads_composite_and_nth_child(self) -> None:
        """Pattern-plan compilation should preserve composite + nthChild."""
        query = parse_query(
            "pattern='print($A)' any='print($A),print($B)' "
            "nthChild=2 nthChild.reverse=true nthChild.ofRule='kind=argument'"
        )
        plan = compile_query(query)
        assert plan.sg_rules[0].composite is not None
        assert plan.sg_rules[0].composite.operator == "any"
        assert plan.sg_rules[0].nth_child is not None
        assert plan.sg_rules[0].nth_child.position == 2
        assert plan.sg_rules[0].nth_child.reverse is True
        assert plan.sg_rules[0].nth_child.of_rule == "kind=argument"


class TestAstGrepRule:
    """Tests for AstGrepRule dataclass."""

    def test_basic_rule(self) -> None:
        """Create basic ast-grep rule."""
        rule = AstGrepRule(pattern="def $F($$$)")
        assert rule.pattern == "def $F($$$)"

    def test_rule_to_yaml_dict(self) -> None:
        """Convert rule to YAML dict."""
        rule = AstGrepRule(pattern="def $F($$$)")
        yaml_dict = rule.to_yaml_dict()
        assert yaml_dict["pattern"] == "def $F($$$)"

    def test_rule_with_inside(self) -> None:
        """Rule with inside constraint."""
        rule = AstGrepRule(
            pattern="def $F($$$)",
            inside="class $C",
        )
        yaml_dict = cast("Mapping[str, object]", rule.to_yaml_dict())
        assert "inside" in yaml_dict
        inside = cast("Mapping[str, object]", yaml_dict["inside"])
        assert inside["pattern"] == "class $C"

    def test_rule_with_stop_by(self) -> None:
        """Rule with stop-by mode."""
        rule = AstGrepRule(
            pattern="def $F($$$)",
            inside="class $C",
            inside_stop_by="end",
        )
        yaml_dict = cast("Mapping[str, object]", rule.to_yaml_dict())
        inside = cast("Mapping[str, object]", yaml_dict["inside"])
        assert inside["stopBy"] == "end"

    def test_rule_with_strictness(self) -> None:
        """Rule with custom strictness."""
        rule = AstGrepRule(
            pattern="def $F($$$)",
            strictness="ast",
        )
        yaml_dict = rule.to_yaml_dict()
        assert yaml_dict["strictness"] == "ast"

    def test_rule_with_has(self) -> None:
        """Rule with has constraint."""
        rule = AstGrepRule(
            pattern="class $C($$$)",
            has="def __init__($$$)",
        )
        yaml_dict = cast("Mapping[str, object]", rule.to_yaml_dict())
        assert "has" in yaml_dict
        has = cast("Mapping[str, object]", yaml_dict["has"])
        assert has["pattern"] == "def __init__($$$)"
