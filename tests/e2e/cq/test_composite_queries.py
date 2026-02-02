"""E2E tests for composite query features.

Tests the enhanced composite query support including all/any/not operators
and rule registry integration.
"""

from __future__ import annotations

from tools.cq.query import parse_query
from tools.cq.query.ir import CompositeRule

_ = CompositeRule  # Re-export for type checking


class TestCompositeRuleDataclass:
    """Tests for CompositeRule dataclass."""

    def test_composite_rule_all_operator(self) -> None:
        """CompositeRule with 'all' operator."""
        rule = CompositeRule(
            operator="all",
            patterns=("pattern1", "pattern2"),
        )
        assert rule.operator == "all"
        assert len(rule.patterns) == 2

    def test_composite_rule_any_operator(self) -> None:
        """CompositeRule with 'any' operator."""
        rule = CompositeRule(
            operator="any",
            patterns=("logger.$M", "print($$$)"),
        )
        assert rule.operator == "any"
        assert len(rule.patterns) == 2

    def test_composite_rule_not_operator(self) -> None:
        """CompositeRule with 'not' operator."""
        rule = CompositeRule(
            operator="not",
            patterns=("debug",),
        )
        assert rule.operator == "not"
        assert len(rule.patterns) == 1

    def test_composite_rule_with_metavar_order(self) -> None:
        """CompositeRule preserves metavar order for ordered captures."""
        rule = CompositeRule(
            operator="all",
            patterns=("$A", "$B"),
            metavar_order=("A", "B"),
        )
        assert rule.metavar_order == ("A", "B")


class TestCompositeRuleToAstGrep:
    """Tests for CompositeRule.to_ast_grep_dict method."""

    def test_all_rule_to_yaml(self) -> None:
        """Convert 'all' rule to ast-grep dict."""
        rule = CompositeRule(
            operator="all",
            patterns=("$X", "await $Y"),
        )
        result = rule.to_ast_grep_dict()
        assert "all" in result
        assert len(result["all"]) == 2

    def test_any_rule_to_yaml(self) -> None:
        """Convert 'any' rule to ast-grep dict."""
        rule = CompositeRule(
            operator="any",
            patterns=("logger.$M($$$)", "print($$$)"),
        )
        result = rule.to_ast_grep_dict()
        assert "any" in result
        assert len(result["any"]) == 2

    def test_not_rule_to_yaml(self) -> None:
        """Convert 'not' rule to ast-grep dict."""
        rule = CompositeRule(
            operator="not",
            patterns=("debug",),
        )
        result = rule.to_ast_grep_dict()
        assert "not" in result


class TestCompositeQueryParsing:
    """Tests for composite query parsing from query strings."""

    def test_parse_all_rule(self) -> None:
        """Parse 'all' composite rule from query string."""
        query = parse_query("pattern='$X' all='p1,p2'")
        assert query.composite is not None
        assert query.composite.operator == "all"
        assert len(query.composite.patterns) == 2

    def test_parse_any_rule(self) -> None:
        """Parse 'any' composite rule from query string."""
        query = parse_query("pattern='$X' any='logger.$M,print($$$)'")
        assert query.composite is not None
        assert query.composite.operator == "any"
        assert len(query.composite.patterns) == 2

    def test_parse_not_rule(self) -> None:
        """Parse 'not' composite rule from query string."""
        query = parse_query("pattern='$X' not='debug'")
        assert query.composite is not None
        assert query.composite.operator == "not"
        assert len(query.composite.patterns) == 1

    def test_no_composite_when_not_specified(self) -> None:
        """Query without composite rule has None."""
        query = parse_query("pattern='def $F($$$)'")
        assert query.composite is None


class TestAllCompositeRule:
    """Tests for 'all' (AND) composite rule semantics."""

    def test_all_requires_all_patterns(self) -> None:
        """All patterns must match for 'all' rule."""
        rule = CompositeRule(
            operator="all",
            patterns=("def $F", "return $X"),
        )
        yaml_dict = rule.to_ast_grep_dict()
        # ast-grep 'all' requires all sub-patterns to match
        assert "all" in yaml_dict
        assert yaml_dict["all"][0] == {"pattern": "def $F"}
        assert yaml_dict["all"][1] == {"pattern": "return $X"}


class TestAnyCompositeRule:
    """Tests for 'any' (OR) composite rule semantics."""

    def test_any_accepts_any_pattern(self) -> None:
        """Any pattern match satisfies 'any' rule."""
        rule = CompositeRule(
            operator="any",
            patterns=("logger.info", "logger.debug", "logger.error"),
        )
        yaml_dict = rule.to_ast_grep_dict()
        assert "any" in yaml_dict
        assert len(yaml_dict["any"]) == 3


class TestNotCompositeRule:
    """Tests for 'not' (negation) composite rule semantics."""

    def test_not_excludes_pattern(self) -> None:
        """'not' rule excludes matching results."""
        rule = CompositeRule(
            operator="not",
            patterns=("debug",),
        )
        yaml_dict = rule.to_ast_grep_dict()
        assert "not" in yaml_dict


class TestCompositeWithRelational:
    """Tests for combining composite rules with relational constraints."""

    def test_composite_with_inside(self) -> None:
        """Composite rule combined with inside constraint."""
        query = parse_query("pattern='$X' all='p1,p2' inside='class Config'")
        assert query.composite is not None
        constraints = query.get_all_relational_constraints()
        assert len(constraints) == 1
        assert constraints[0].operator == "inside"

    def test_composite_with_has(self) -> None:
        """Composite rule combined with has constraint."""
        query = parse_query("pattern='def $F($$$)' any='async,await' has='return'")
        assert query.composite is not None
        constraints = query.get_all_relational_constraints()
        assert len(constraints) == 1
        assert constraints[0].operator == "has"


class TestNestedComposites:
    """Tests for nested composite rule handling."""

    def test_single_composite_parsing(self) -> None:
        """Single composite is correctly parsed."""
        query = parse_query("pattern='$X' all='pattern1,pattern2'")
        assert query.composite is not None
        # Only one composite operator per query currently
        assert query.composite.operator == "all"

    def test_composite_patterns_preserved(self) -> None:
        """Composite pattern list is preserved."""
        patterns = "a,b,c,d"
        query = parse_query(f"pattern='$X' any='{patterns}'")
        assert query.composite is not None
        assert len(query.composite.patterns) == 4
        assert query.composite.patterns == ("a", "b", "c", "d")
