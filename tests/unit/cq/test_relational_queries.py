"""Tests for relational constraint queries.

Verifies:
1. Relational constraint IR construction
2. Relational constraint parsing
3. Relational constraint application
"""

from __future__ import annotations

from tools.cq.query.ir import Query, RelationalConstraint
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query


class TestRelationalConstraint:
    """Tests for RelationalConstraint dataclass."""

    def test_inside_constraint(self) -> None:
        """Create inside relational constraint."""
        constraint = RelationalConstraint(
            operator="inside",
            pattern="class Config",
        )
        assert constraint.operator == "inside"
        assert constraint.pattern == "class Config"
        assert constraint.stop_by == "neighbor"

    def test_has_constraint(self) -> None:
        """Create has relational constraint."""
        constraint = RelationalConstraint(
            operator="has",
            pattern="return $X",
        )
        assert constraint.operator == "has"
        assert constraint.pattern == "return $X"

    def test_precedes_constraint(self) -> None:
        """Create precedes relational constraint."""
        constraint = RelationalConstraint(
            operator="precedes",
            pattern="except $E:",
        )
        assert constraint.operator == "precedes"

    def test_follows_constraint(self) -> None:
        """Create follows relational constraint."""
        constraint = RelationalConstraint(
            operator="follows",
            pattern="@decorator",
        )
        assert constraint.operator == "follows"

    def test_stop_by_end(self) -> None:
        """Constraint with stop_by=end."""
        constraint = RelationalConstraint(
            operator="inside",
            pattern="class $C",
            stop_by="end",
        )
        assert constraint.stop_by == "end"

    def test_to_ast_grep_dict(self) -> None:
        """Convert constraint to ast-grep dict."""
        constraint = RelationalConstraint(
            operator="inside",
            pattern="class Config",
        )
        d = constraint.to_ast_grep_dict()
        assert "inside" in d
        assert d["inside"]["pattern"] == "class Config"

    def test_to_ast_grep_dict_with_stop_by(self) -> None:
        """Convert constraint with stop_by to ast-grep dict."""
        constraint = RelationalConstraint(
            operator="inside",
            pattern="class Config",
            stop_by="end",
        )
        d = constraint.to_ast_grep_dict()
        assert d["inside"]["stopBy"] == "end"


class TestRelationalQueryParsing:
    """Tests for parsing relational constraints in queries."""

    def test_inside_constraint_parsing(self) -> None:
        """Parse query with inside constraint."""
        query = parse_query("entity=function inside='class Config'")
        assert len(query.relational) == 1
        assert query.relational[0].operator == "inside"
        assert query.relational[0].pattern == "class Config"

    def test_has_constraint_parsing(self) -> None:
        """Parse query with has constraint."""
        query = parse_query("entity=function has='return $X'")
        assert len(query.relational) == 1
        assert query.relational[0].operator == "has"
        assert query.relational[0].pattern == "return $X"

    def test_multiple_constraints_parsing(self) -> None:
        """Parse query with multiple relational constraints."""
        query = parse_query("entity=function inside='class $C' has='await $X'")
        assert len(query.relational) == 2

        ops = {c.operator for c in query.relational}
        assert ops == {"inside", "has"}

    def test_inside_with_stop_by(self) -> None:
        """Parse query with inside constraint and stop_by."""
        query = parse_query("entity=function inside='class $C' inside_stop_by=end")
        assert len(query.relational) == 1
        assert query.relational[0].stop_by == "end"

    def test_pattern_query_with_relational(self) -> None:
        """Parse pattern query with relational constraint."""
        query = parse_query("pattern='def $F($$$)' inside='class Config'")
        assert query.is_pattern_query
        assert len(query.relational) == 1
        assert query.relational[0].operator == "inside"


class TestRelationalQueryIR:
    """Tests for relational constraint IR construction."""

    def test_query_with_relational(self) -> None:
        """Create query with relational constraints."""
        query = Query(
            entity="function",
            relational=(RelationalConstraint(operator="inside", pattern="class Config"),),
        )
        assert len(query.relational) == 1

    def test_with_relational_method(self) -> None:
        """Use with_relational to add constraints."""
        query = Query(entity="function")
        query = query.with_relational(
            RelationalConstraint(operator="inside", pattern="class Config")
        )
        assert len(query.relational) == 1

    def test_with_relational_accumulates(self) -> None:
        """with_relational accumulates constraints."""
        query = Query(entity="function")
        query = query.with_relational(RelationalConstraint(operator="inside", pattern="class A"))
        query = query.with_relational(RelationalConstraint(operator="has", pattern="return $X"))
        assert len(query.relational) == 2


class TestRelationalQueryPlanning:
    """Tests for compiling relational constraints."""

    def test_entity_with_relational_produces_rules(self) -> None:
        """Entity query with relational constraints produces ast-grep rules."""
        query = Query(
            entity="function",
            relational=(RelationalConstraint(operator="inside", pattern="class Config"),),
        )
        plan = compile_query(query)
        # Entity queries with relational constraints produce ast-grep rules
        assert len(plan.sg_rules) == 1
        assert plan.sg_rules[0].inside == "class Config"

    def test_pattern_with_relational_combined(self) -> None:
        """Pattern query with relational constraints combines them in rule."""
        query = Query(
            pattern_spec=__import__("tools.cq.query.ir", fromlist=["PatternSpec"]).PatternSpec(
                pattern="def $F($$$)"
            ),
            relational=(RelationalConstraint(operator="inside", pattern="class $C"),),
        )
        plan = compile_query(query)
        assert plan.sg_rules[0].inside == "class $C"

    def test_multiple_relational_all_applied(self) -> None:
        """Multiple relational constraints all applied to rule."""
        query = Query(
            entity="function",
            relational=(
                RelationalConstraint(operator="inside", pattern="class $C"),
                RelationalConstraint(operator="has", pattern="return $X"),
            ),
        )
        plan = compile_query(query)
        rule = plan.sg_rules[0]
        assert rule.inside == "class $C"
        assert rule.has == "return $X"
