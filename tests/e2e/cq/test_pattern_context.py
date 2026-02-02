"""E2E tests for pattern context and selector features.

Tests the enhanced pattern object support including context disambiguation
and selector extraction.
"""

from __future__ import annotations

import pytest
from tools.cq.query import parse_query
from tools.cq.query.ir import PatternSpec


class TestPatternContextParsing:
    """Tests for pattern.context syntax parsing."""

    def test_parse_pattern_with_context(self) -> None:
        """Parse pattern with context for disambiguation."""
        query = parse_query("pattern.context='{ \"a\": 123 }' pattern.selector=pair")
        assert query.is_pattern_query
        assert query.pattern_spec is not None
        assert query.pattern_spec.context == '{ "a": 123 }'
        assert query.pattern_spec.selector == "pair"

    def test_parse_pattern_with_context_no_selector(self) -> None:
        """Parse pattern with context but no selector."""
        query = parse_query("pattern.context='class A { $F = $V }'")
        assert query.pattern_spec is not None
        assert query.pattern_spec.context == "class A { $F = $V }"
        assert query.pattern_spec.selector is None

    def test_parse_simple_pattern_unchanged(self) -> None:
        """Simple patterns without context remain unchanged."""
        query = parse_query("pattern='def $F($$$)'")
        assert query.pattern_spec is not None
        assert query.pattern_spec.pattern == "def $F($$$)"
        assert query.pattern_spec.context is None
        assert query.pattern_spec.selector is None

    def test_parse_pattern_with_dot_strictness(self) -> None:
        """Parse pattern.strictness dot notation."""
        query = parse_query("pattern='def $F()' pattern.strictness=cst")
        assert query.pattern_spec is not None
        assert query.pattern_spec.strictness == "cst"


class TestPatternSpecMethods:
    """Tests for PatternSpec methods."""

    def test_requires_yaml_rule_simple_pattern(self) -> None:
        """Simple pattern does not require YAML rule."""
        spec = PatternSpec(pattern="def $F($$$)")
        assert not spec.requires_yaml_rule()

    def test_requires_yaml_rule_with_context(self) -> None:
        """Pattern with context requires YAML rule."""
        spec = PatternSpec(pattern="$X", context="{ $X }")
        assert spec.requires_yaml_rule()

    def test_requires_yaml_rule_with_selector(self) -> None:
        """Pattern with selector requires YAML rule."""
        spec = PatternSpec(pattern="$X", selector="identifier")
        assert spec.requires_yaml_rule()

    def test_requires_yaml_rule_with_non_smart_strictness(self) -> None:
        """Pattern with non-smart strictness requires YAML rule."""
        spec = PatternSpec(pattern="def $F()", strictness="cst")
        assert spec.requires_yaml_rule()

    def test_to_yaml_dict_simple(self) -> None:
        """Simple pattern generates simple YAML dict."""
        spec = PatternSpec(pattern="def $F($$$)")
        yaml_dict = spec.to_yaml_dict()
        assert yaml_dict == {"pattern": "def $F($$$)"}

    def test_to_yaml_dict_with_context(self) -> None:
        """Pattern with context generates pattern object YAML."""
        spec = PatternSpec(
            pattern="$X",
            context='{ "a": 123 }',
            selector="pair",
        )
        yaml_dict = spec.to_yaml_dict()
        assert yaml_dict == {
            "pattern": {
                "context": '{ "a": 123 }',
                "selector": "pair",
            }
        }

    def test_to_yaml_dict_with_strictness(self) -> None:
        """Pattern with strictness includes it in YAML."""
        spec = PatternSpec(pattern="def $F()", strictness="ast")
        yaml_dict = spec.to_yaml_dict()
        assert yaml_dict == {
            "pattern": "def $F()",
            "strictness": "ast",
        }


class TestStrictnessModes:
    """Tests for strictness mode parsing and usage."""

    @pytest.mark.parametrize("strictness", ["cst", "smart", "ast", "relaxed", "signature"])
    def test_all_strictness_modes_valid(self, strictness: str) -> None:
        """All defined strictness modes are parseable."""
        query = parse_query(f"pattern='def $F()' strictness={strictness}")
        assert query.pattern_spec is not None
        assert query.pattern_spec.strictness == strictness

    def test_invalid_strictness_raises(self) -> None:
        """Invalid strictness mode raises parse error."""
        from tools.cq.query.parser import QueryParseError

        with pytest.raises(QueryParseError, match="Invalid strictness"):
            parse_query("pattern='def $F()' strictness=invalid")

    def test_default_strictness_is_smart(self) -> None:
        """Default strictness is 'smart'."""
        query = parse_query("pattern='def $F()'")
        assert query.pattern_spec is not None
        assert query.pattern_spec.strictness == "smart"


class TestMetavarFilterParsing:
    """Tests for metavariable filter parsing."""

    def test_parse_metavar_filter_regex(self) -> None:
        """Parse metavar filter with regex pattern."""
        query = parse_query("pattern='$L $$OP $R' $$OP=~'^[<>=]'")
        assert len(query.metavar_filters) == 1
        assert query.metavar_filters[0].name == "OP"
        assert query.metavar_filters[0].pattern == "^[<>=]"
        assert not query.metavar_filters[0].negate

    def test_parse_metavar_filter_negated(self) -> None:
        """Parse negated metavar filter."""
        # Use !~pattern format (exclamation before tilde)
        query = parse_query("pattern='$X' $X=!~debug")
        assert len(query.metavar_filters) == 1
        assert query.metavar_filters[0].name == "X"
        assert query.metavar_filters[0].pattern == "debug"
        assert query.metavar_filters[0].negate

    def test_no_metavar_filters_by_default(self) -> None:
        """No metavar filters when not specified."""
        query = parse_query("pattern='def $F($$$)'")
        assert len(query.metavar_filters) == 0


class TestCompositeRuleParsing:
    """Tests for composite rule parsing."""

    def test_parse_all_rule(self) -> None:
        """Parse 'all' composite rule."""
        query = parse_query("pattern='$X' all='p1,p2'")
        assert query.composite is not None
        assert query.composite.operator == "all"
        assert len(query.composite.patterns) == 2

    def test_parse_any_rule(self) -> None:
        """Parse 'any' composite rule."""
        query = parse_query("pattern='$X' any='logger.$M,print($$$)'")
        assert query.composite is not None
        assert query.composite.operator == "any"
        assert len(query.composite.patterns) == 2

    def test_parse_not_rule(self) -> None:
        """Parse 'not' composite rule."""
        query = parse_query("pattern='$X' not='debug'")
        assert query.composite is not None
        assert query.composite.operator == "not"
        assert len(query.composite.patterns) == 1


class TestNthChildParsing:
    """Tests for nthChild positional matching parsing."""

    def test_parse_exact_position(self) -> None:
        """Parse nthChild with exact position."""
        query = parse_query("pattern='$X' nthChild=3")
        assert query.nth_child is not None
        assert query.nth_child.position == 3
        assert not query.nth_child.reverse

    def test_parse_formula_position(self) -> None:
        """Parse nthChild with formula position."""
        query = parse_query("pattern='$X' nthChild='2n+1'")
        assert query.nth_child is not None
        assert query.nth_child.position == "2n+1"

    def test_parse_reverse_flag(self) -> None:
        """Parse nthChild with reverse flag."""
        query = parse_query("pattern='$X' nthChild=1 nthChild.reverse=true")
        assert query.nth_child is not None
        assert query.nth_child.reverse is True

    def test_parse_of_rule(self) -> None:
        """Parse nthChild with ofRule."""
        query = parse_query("pattern='$X' nthChild=2 nthChild.ofRule='identifier'")
        assert query.nth_child is not None
        assert query.nth_child.of_rule == "identifier"
