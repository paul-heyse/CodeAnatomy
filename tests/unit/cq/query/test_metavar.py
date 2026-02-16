"""Tests for query metavariable helpers."""

from __future__ import annotations

from tools.cq.query.ir import CompositeRule, MetaVarFilter, NthChildSpec
from tools.cq.query.metavar import (
    extract_metavar_names,
    extract_rule_metavars,
    extract_rule_variadic_metavars,
    extract_variadic_metavar_names,
    partition_metavar_filters,
)
from tools.cq.query.planner import AstGrepRule


def test_extract_metavar_names_variants() -> None:
    names = extract_metavar_names("foo($A, $$OP, $$$ARGS, $_IGNORED)")
    assert set(names) == {"A", "OP", "ARGS"}


def test_extract_variadic_metavar_names() -> None:
    names = extract_variadic_metavar_names("foo($A, $$$ARGS, $$$MORE)")
    assert names == ("ARGS", "MORE")


def test_extract_rule_metavars_includes_composite_and_nth() -> None:
    rule = AstGrepRule(
        pattern="foo($A, $$$ARGS)",
        inside="class $C",
        composite=CompositeRule(operator="any", patterns=("bar($B)",)),
        nth_child=NthChildSpec(position=2, of_rule="kind=$K"),
    )
    names = set(extract_rule_metavars(rule))
    assert names >= {"A", "ARGS", "C", "B", "K"}
    assert extract_rule_variadic_metavars(rule) == frozenset({"ARGS"})


def test_partition_metavar_filters_pushdown_and_residual() -> None:
    filters = (
        MetaVarFilter(name="A", pattern="^foo$", negate=False),
        MetaVarFilter(name="A", pattern="^bar$", negate=False),
        MetaVarFilter(name="B", pattern="^baz$", negate=True),
    )
    constraints, residual = partition_metavar_filters(filters)
    assert constraints == {"A": {"regex": "^foo$"}}
    assert residual == (filters[1], filters[2])
