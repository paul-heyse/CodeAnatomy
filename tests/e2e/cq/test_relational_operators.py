"""E2E tests for relational operator features.

Tests the enhanced relational query support including inside, has, precedes,
follows operators with stopBy and field constraints.
"""

from __future__ import annotations

import pytest
from tools.cq.query import parse_query
from tools.cq.query.ir import RelationalConstraint


class TestRelationalOperatorParsing:
    """Tests for relational operator parsing."""

    def test_parse_inside_constraint(self) -> None:
        """Parse inside relational constraint."""
        query = parse_query("pattern='$X' inside='class Config'")
        assert query.pattern_spec is not None
        constraints = query.get_all_relational_constraints()
        assert len(constraints) == 1
        assert constraints[0].operator == "inside"
        assert constraints[0].pattern == "class Config"

    def test_parse_has_constraint(self) -> None:
        """Parse has relational constraint."""
        query = parse_query("pattern='def $F($$$)' has='await $X'")
        constraints = query.get_all_relational_constraints()
        assert len(constraints) == 1
        assert constraints[0].operator == "has"
        assert constraints[0].pattern == "await $X"

    def test_parse_precedes_constraint(self) -> None:
        """Parse precedes relational constraint."""
        query = parse_query("pattern='return $X' precedes='finally:'")
        constraints = query.get_all_relational_constraints()
        assert len(constraints) == 1
        assert constraints[0].operator == "precedes"
        assert constraints[0].pattern == "finally:"

    def test_parse_follows_constraint(self) -> None:
        """Parse follows relational constraint."""
        query = parse_query("pattern='console.log($_)' follows='await $X'")
        constraints = query.get_all_relational_constraints()
        assert len(constraints) == 1
        assert constraints[0].operator == "follows"
        assert constraints[0].pattern == "await $X"

    def test_parse_multiple_relational_constraints(self) -> None:
        """Parse multiple relational constraints."""
        query = parse_query("pattern='$X()' inside='def handle' has='await'")
        constraints = query.get_all_relational_constraints()
        assert len(constraints) == 2
        operators = {c.operator for c in constraints}
        assert operators == {"inside", "has"}


class TestStopByControl:
    """Tests for stopBy control parsing."""

    def test_stopby_default_is_neighbor(self) -> None:
        """Default stopBy is 'neighbor'."""
        query = parse_query("pattern='$X' inside='class $C'")
        constraints = query.get_all_relational_constraints()
        assert len(constraints) == 1
        assert constraints[0].stop_by == "neighbor"

    def test_stopby_end_full_traversal(self) -> None:
        """Parse stopBy=end for full traversal."""
        query = parse_query("pattern='$X' inside='class $C' inside.stopBy=end")
        constraints = query.get_all_relational_constraints()
        assert len(constraints) == 1
        assert constraints[0].stop_by == "end"

    def test_stopby_custom_pattern(self) -> None:
        """Parse custom stopBy pattern."""
        query = parse_query("pattern='$X' inside='class $C' inside.stopBy='def $F'")
        constraints = query.get_all_relational_constraints()
        assert len(constraints) == 1
        assert constraints[0].stop_by == "def $F"

    def test_stopby_global_applies_to_all(self) -> None:
        """Global stopBy applies to all constraints without specific stopBy."""
        query = parse_query("pattern='$X' inside='class $C' stopBy=end")
        constraints = query.get_all_relational_constraints()
        assert len(constraints) == 1
        assert constraints[0].stop_by == "end"


class TestFieldScopedSearches:
    """Tests for field-scoped search parsing."""

    def test_field_inside_constraint(self) -> None:
        """Parse field constraint for inside operator."""
        query = parse_query("pattern='$X' inside='def $F($$$)' inside.field=body")
        constraints = query.get_all_relational_constraints()
        assert len(constraints) == 1
        assert constraints[0].field_name == "body"

    def test_field_has_constraint(self) -> None:
        """Parse field constraint for has operator."""
        query = parse_query("pattern='def $F($$$)' has='$X' has.field=parameters")
        constraints = query.get_all_relational_constraints()
        assert len(constraints) == 1
        assert constraints[0].field_name == "parameters"

    def test_field_global_constraint(self) -> None:
        """Global field constraint applies to relevant operators."""
        query = parse_query("pattern='$X' inside='class $C' field=body")
        constraints = query.get_all_relational_constraints()
        assert len(constraints) == 1
        assert constraints[0].field_name == "body"

    def test_precedes_rejects_field(self) -> None:
        """Precedes operator does not support field constraint."""
        with pytest.raises(ValueError, match=r"does not support.*field"):
            # Build constraint directly to test validation
            RelationalConstraint(
                operator="precedes",
                pattern="$X",
                field_name="body",
            )

    def test_follows_rejects_field(self) -> None:
        """Follows operator does not support field constraint."""
        with pytest.raises(ValueError, match=r"does not support.*field"):
            RelationalConstraint(
                operator="follows",
                pattern="$X",
                field_name="body",
            )


class TestRelationalConstraintToAstGrep:
    """Tests for RelationalConstraint.to_ast_grep_dict method."""

    def test_simple_inside_constraint(self) -> None:
        """Simple inside constraint to ast-grep dict."""
        constraint = RelationalConstraint(
            operator="inside",
            pattern="class Config",
        )
        result = constraint.to_ast_grep_dict()
        assert result == {"inside": {"pattern": "class Config"}}

    def test_constraint_with_stopby(self) -> None:
        """Constraint with stopBy to ast-grep dict."""
        constraint = RelationalConstraint(
            operator="has",
            pattern="await $X",
            stop_by="end",
        )
        result = constraint.to_ast_grep_dict()
        assert result == {"has": {"pattern": "await $X", "stopBy": "end"}}

    def test_constraint_with_field(self) -> None:
        """Constraint with field to ast-grep dict."""
        constraint = RelationalConstraint(
            operator="inside",
            pattern="def $F($$$)",
            field_name="body",
        )
        result = constraint.to_ast_grep_dict()
        assert result == {"inside": {"pattern": "def $F($$$)", "field": "body"}}

    def test_constraint_with_all_options(self) -> None:
        """Constraint with all options to ast-grep dict."""
        constraint = RelationalConstraint(
            operator="inside",
            pattern="class $C",
            stop_by="def $F",
            field_name="body",
        )
        result = constraint.to_ast_grep_dict()
        assert result == {
            "inside": {
                "pattern": "class $C",
                "stopBy": "def $F",
                "field": "body",
            }
        }


class TestQueryRelationalMethods:
    """Tests for Query.get_all_relational_constraints method."""

    def test_get_all_constraints_empty(self) -> None:
        """Query with no constraints returns empty list."""
        query = parse_query("pattern='def $F()'")
        constraints = query.get_all_relational_constraints()
        assert len(constraints) == 0

    def test_get_all_constraints_single(self) -> None:
        """Query with single constraint returns it."""
        query = parse_query("pattern='$X' inside='class $C'")
        constraints = query.get_all_relational_constraints()
        assert len(constraints) == 1
        assert constraints[0].operator == "inside"

    def test_get_all_constraints_multiple(self) -> None:
        """Query with multiple constraints returns all."""
        query = parse_query("pattern='$X' inside='class $C' has='$Y' follows='$Z'")
        constraints = query.get_all_relational_constraints()
        assert len(constraints) == 3
        operators = {c.operator for c in constraints}
        assert operators == {"inside", "has", "follows"}
