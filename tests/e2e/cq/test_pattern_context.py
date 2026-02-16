"""E2E tests for pattern context and selector features.

Tests the enhanced pattern object support including context disambiguation
and selector extraction.
"""

from __future__ import annotations

import pytest
from tools.cq.query import parse_query
from tools.cq.query.ir import PatternSpec

COMPOSITE_PATTERN_COUNT = 2
NTH_CHILD_EXACT_POSITION = 3


class TestPatternContextParsing:
    """Tests for pattern.context syntax parsing."""

    @staticmethod
    def test_parse_pattern_with_context() -> None:
        """Parse pattern with context for disambiguation."""
        query = parse_query("pattern.context='{ \"a\": 123 }' pattern.selector=pair")
        assert query.is_pattern_query
        assert query.pattern_spec is not None
        assert query.pattern_spec.context == '{ "a": 123 }'
        assert query.pattern_spec.selector == "pair"

    @staticmethod
    def test_parse_pattern_with_context_no_selector() -> None:
        """Parse pattern with context but no selector."""
        query = parse_query("pattern.context='class A { $F = $V }'")
        assert query.pattern_spec is not None
        assert query.pattern_spec.context == "class A { $F = $V }"
        assert query.pattern_spec.selector is None

    @staticmethod
    def test_parse_simple_pattern_unchanged() -> None:
        """Simple patterns without context remain unchanged."""
        query = parse_query("pattern='def $F($$$)'")
        assert query.pattern_spec is not None
        assert query.pattern_spec.pattern == "def $F($$$)"
        assert query.pattern_spec.context is None
        assert query.pattern_spec.selector is None

    @staticmethod
    def test_parse_pattern_with_dot_strictness() -> None:
        """Parse pattern.strictness dot notation."""
        query = parse_query("pattern='def $F()' pattern.strictness=cst")
        assert query.pattern_spec is not None
        assert query.pattern_spec.strictness == "cst"


class TestPatternSpecMethods:
    """Tests for PatternSpec methods."""

    @staticmethod
    def test_requires_yaml_rule_simple_pattern() -> None:
        """Simple pattern does not require YAML rule."""
        spec = PatternSpec(pattern="def $F($$$)")
        assert not spec.requires_yaml_rule()

    @staticmethod
    def test_requires_yaml_rule_with_context() -> None:
        """Pattern with context requires YAML rule."""
        spec = PatternSpec(pattern="$X", context="{ $X }")
        assert spec.requires_yaml_rule()

    @staticmethod
    def test_requires_yaml_rule_with_selector() -> None:
        """Pattern with selector requires YAML rule."""
        spec = PatternSpec(pattern="$X", selector="identifier")
        assert spec.requires_yaml_rule()

    @staticmethod
    def test_requires_yaml_rule_with_non_smart_strictness() -> None:
        """Pattern with non-smart strictness requires YAML rule."""
        spec = PatternSpec(pattern="def $F()", strictness="cst")
        assert spec.requires_yaml_rule()

    @staticmethod
    def test_to_yaml_dict_simple() -> None:
        """Simple pattern generates simple YAML dict."""
        spec = PatternSpec(pattern="def $F($$$)")
        yaml_dict = spec.to_yaml_dict()
        assert yaml_dict == {"pattern": "def $F($$$)"}

    @staticmethod
    def test_to_yaml_dict_with_context() -> None:
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

    @staticmethod
    def test_to_yaml_dict_with_strictness() -> None:
        """Pattern with strictness includes it in YAML."""
        spec = PatternSpec(pattern="def $F()", strictness="ast")
        yaml_dict = spec.to_yaml_dict()
        assert yaml_dict == {
            "pattern": "def $F()",
            "strictness": "ast",
        }


class TestStrictnessModes:
    """Tests for strictness mode parsing and usage."""

    @staticmethod
    @pytest.mark.parametrize("strictness", ["cst", "smart", "ast", "relaxed", "signature"])
    def test_all_strictness_modes_valid(strictness: str) -> None:
        """All defined strictness modes are parseable."""
        query = parse_query(f"pattern='def $F()' strictness={strictness}")
        assert query.pattern_spec is not None
        assert query.pattern_spec.strictness == strictness

    @staticmethod
    def test_invalid_strictness_raises() -> None:
        """Invalid strictness mode raises parse error."""
        from tools.cq.query.parser import QueryParseError

        with pytest.raises(QueryParseError, match="Invalid strictness"):
            parse_query("pattern='def $F()' strictness=invalid")

    @staticmethod
    def test_default_strictness_is_smart() -> None:
        """Default strictness is 'smart'."""
        query = parse_query("pattern='def $F()'")
        assert query.pattern_spec is not None
        assert query.pattern_spec.strictness == "smart"


class TestMetavarFilterParsing:
    """Tests for metavariable filter parsing."""

    @staticmethod
    def test_parse_metavar_filter_regex() -> None:
        """Parse metavar filter with regex pattern."""
        query = parse_query("pattern='$L $$OP $R' $$OP=~'^[<>=]'")
        assert len(query.metavar_filters) == 1
        assert query.metavar_filters[0].name == "OP"
        assert query.metavar_filters[0].pattern == "^[<>=]"
        assert not query.metavar_filters[0].negate

    @staticmethod
    def test_parse_metavar_filter_negated() -> None:
        """Parse negated metavar filter."""
        # Use !~pattern format (exclamation before tilde)
        query = parse_query("pattern='$X' $X=!~debug")
        assert len(query.metavar_filters) == 1
        assert query.metavar_filters[0].name == "X"
        assert query.metavar_filters[0].pattern == "debug"
        assert query.metavar_filters[0].negate

    @staticmethod
    def test_no_metavar_filters_by_default() -> None:
        """No metavar filters when not specified."""
        query = parse_query("pattern='def $F($$$)'")
        assert len(query.metavar_filters) == 0


class TestCompositeRuleParsing:
    """Tests for composite rule parsing."""

    @staticmethod
    def test_parse_all_rule() -> None:
        """Parse 'all' composite rule."""
        query = parse_query("pattern='$X' all='p1,p2'")
        assert query.composite is not None
        assert query.composite.operator == "all"
        assert len(query.composite.patterns) == COMPOSITE_PATTERN_COUNT

    @staticmethod
    def test_parse_any_rule() -> None:
        """Parse 'any' composite rule."""
        query = parse_query("pattern='$X' any='logger.$M,print($$$)'")
        assert query.composite is not None
        assert query.composite.operator == "any"
        assert len(query.composite.patterns) == COMPOSITE_PATTERN_COUNT

    @staticmethod
    def test_parse_not_rule() -> None:
        """Parse 'not' composite rule."""
        query = parse_query("pattern='$X' not='debug'")
        assert query.composite is not None
        assert query.composite.operator == "not"
        assert len(query.composite.patterns) == 1


class TestNthChildParsing:
    """Tests for nthChild positional matching parsing."""

    @staticmethod
    def test_parse_exact_position() -> None:
        """Parse nthChild with exact position."""
        query = parse_query(f"pattern='$X' nthChild={NTH_CHILD_EXACT_POSITION}")
        assert query.nth_child is not None
        assert query.nth_child.position == NTH_CHILD_EXACT_POSITION
        assert not query.nth_child.reverse

    @staticmethod
    def test_parse_formula_position() -> None:
        """Parse nthChild with formula position."""
        query = parse_query("pattern='$X' nthChild='2n+1'")
        assert query.nth_child is not None
        assert query.nth_child.position == "2n+1"

    @staticmethod
    def test_parse_reverse_flag() -> None:
        """Parse nthChild with reverse flag."""
        query = parse_query("pattern='$X' nthChild=1 nthChild.reverse=true")
        assert query.nth_child is not None
        assert query.nth_child.reverse is True

    @staticmethod
    def test_parse_of_rule() -> None:
        """Parse nthChild with ofRule."""
        query = parse_query("pattern='$X' nthChild=2 nthChild.ofRule='identifier'")
        assert query.nth_child is not None
        assert query.nth_child.of_rule == "identifier"
