"""Tests for inferred dependencies module."""

from __future__ import annotations

import pytest

from relspec.inferred_deps import (
    InferredDeps,
    compare_deps,
    infer_deps_from_sqlglot_expr,
    summarize_inferred_deps,
)

EXPECTED_TWO: int = 2
EXPECTED_ONE: int = 1


def test_inferred_deps_creation() -> None:
    """Create InferredDeps with all fields."""
    deps = InferredDeps(
        rule_name="test_rule",
        output="test_output",
        inputs=("table_a", "table_b"),
        required_columns={"table_a": ("col1", "col2"), "table_b": ("col3",)},
        plan_fingerprint="abc123",
        declared_inputs=("table_a", "table_b"),
        inputs_match=True,
    )
    assert deps.rule_name == "test_rule"
    assert deps.output == "test_output"
    assert deps.inputs == ("table_a", "table_b")
    assert deps.inputs_match is True
    assert deps.extra_inferred == ()
    assert deps.missing_declared == ()


def test_inferred_deps_mismatch() -> None:
    """Detect mismatches between declared and inferred inputs."""
    deps = InferredDeps(
        rule_name="test_rule",
        output="test_output",
        inputs=("table_a", "table_c"),  # inferred
        declared_inputs=("table_a", "table_b"),  # declared
        inputs_match=False,
        extra_inferred=("table_c",),
        missing_declared=("table_b",),
    )
    assert deps.inputs_match is False
    assert deps.extra_inferred == ("table_c",)
    assert deps.missing_declared == ("table_b",)


def test_compare_deps_matching() -> None:
    """Compare matching declared and inferred inputs."""
    inferred = InferredDeps(
        rule_name="test_rule",
        output="test_output",
        inputs=("table_a", "table_b"),
    )
    result = compare_deps(("table_a", "table_b"), inferred)
    assert result.inputs_match is True
    assert result.extra_inferred == ()
    assert result.missing_declared == ()


def test_compare_deps_extra_inferred() -> None:
    """Detect tables inferred but not declared."""
    inferred = InferredDeps(
        rule_name="test_rule",
        output="test_output",
        inputs=("table_a", "table_b", "table_c"),
    )
    result = compare_deps(("table_a", "table_b"), inferred)
    assert result.inputs_match is False
    assert result.extra_inferred == ("table_c",)
    assert result.missing_declared == ()


def test_compare_deps_missing_declared() -> None:
    """Detect tables declared but not inferred."""
    inferred = InferredDeps(
        rule_name="test_rule",
        output="test_output",
        inputs=("table_a",),
    )
    result = compare_deps(("table_a", "table_b"), inferred)
    assert result.inputs_match is False
    assert result.extra_inferred == ()
    assert result.missing_declared == ("table_b",)


def test_compare_deps_both_differences() -> None:
    """Detect both extra and missing tables."""
    inferred = InferredDeps(
        rule_name="test_rule",
        output="test_output",
        inputs=("table_a", "table_c"),
    )
    result = compare_deps(("table_a", "table_b"), inferred)
    assert result.inputs_match is False
    assert result.extra_inferred == ("table_c",)
    assert result.missing_declared == ("table_b",)


def test_summarize_inferred_deps_all_match() -> None:
    """Summarize when all rules match."""
    deps = [
        InferredDeps(
            rule_name="rule1",
            output="out1",
            inputs=("a",),
            declared_inputs=("a",),
            inputs_match=True,
        ),
        InferredDeps(
            rule_name="rule2",
            output="out2",
            inputs=("b",),
            declared_inputs=("b",),
            inputs_match=True,
        ),
    ]
    summary = summarize_inferred_deps(deps)
    assert summary.total_rules == EXPECTED_TWO
    assert summary.matched_rules == EXPECTED_TWO
    assert summary.mismatched_rules == 0
    assert summary.mismatches == ()


def test_summarize_inferred_deps_with_mismatches() -> None:
    """Summarize when some rules have mismatches."""
    deps = [
        InferredDeps(
            rule_name="rule1",
            output="out1",
            inputs=("a",),
            declared_inputs=("a",),
            inputs_match=True,
        ),
        InferredDeps(
            rule_name="rule2",
            output="out2",
            inputs=("b", "c"),
            declared_inputs=("b",),
            inputs_match=False,
            extra_inferred=("c",),
        ),
    ]
    summary = summarize_inferred_deps(deps)
    assert summary.total_rules == EXPECTED_TWO
    assert summary.matched_rules == EXPECTED_ONE
    assert summary.mismatched_rules == EXPECTED_ONE
    assert summary.extra_inferred_total == EXPECTED_ONE
    assert summary.missing_declared_total == 0
    assert len(summary.mismatches) == EXPECTED_ONE
    assert summary.mismatches[0].rule_name == "rule2"


def test_infer_deps_from_sqlglot_expr() -> None:
    """Infer dependencies from SQLGlot expression."""
    from sqlglot_tools.compat import parse_one

    expr = parse_one("SELECT a.x, b.y FROM table_a a JOIN table_b b ON a.id = b.id")
    deps = infer_deps_from_sqlglot_expr(
        expr,
        rule_name="join_rule",
        output="joined",
        declared_inputs=("table_a", "table_b"),
    )
    assert deps.rule_name == "join_rule"
    assert deps.output == "joined"
    assert "table_a" in deps.inputs or "a" in deps.inputs
    assert deps.plan_fingerprint


def test_infer_deps_from_sqlglot_expr_invalid_type() -> None:
    """Raise when expression is not SQLGlot Expression."""
    with pytest.raises(TypeError, match="Expected SQLGlot Expression"):
        infer_deps_from_sqlglot_expr(
            "not an expression",  # type: ignore[arg-type]
            rule_name="test",
            output="out",
        )
