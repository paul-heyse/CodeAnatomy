"""Tests for inferred dependencies module."""

from __future__ import annotations

import pytest

from relspec.inferred_deps import InferredDeps, infer_deps_from_sqlglot_expr


def test_inferred_deps_creation() -> None:
    """Create InferredDeps with all fields."""
    deps = InferredDeps(
        task_name="test_task",
        output="test_output",
        inputs=("table_a", "table_b"),
        required_columns={"table_a": ("col1", "col2"), "table_b": ("col3",)},
        plan_fingerprint="abc123",
    )
    assert deps.task_name == "test_task"
    assert deps.output == "test_output"
    assert deps.inputs == ("table_a", "table_b")


def test_infer_deps_from_sqlglot_expr() -> None:
    """Infer dependencies from SQLGlot expression."""
    from sqlglot_tools.compat import parse_one

    expr = parse_one("SELECT a.x, b.y FROM table_a a JOIN table_b b ON a.id = b.id")
    deps = infer_deps_from_sqlglot_expr(
        expr,
        task_name="join_task",
        output="joined",
    )
    assert deps.task_name == "join_task"
    assert deps.output == "joined"
    assert "table_a" in deps.inputs or "a" in deps.inputs
    assert deps.plan_fingerprint


def test_infer_deps_from_sqlglot_expr_invalid_type() -> None:
    """Raise when expression is not SQLGlot Expression."""
    with pytest.raises(TypeError, match="Expected SQLGlot Expression"):
        infer_deps_from_sqlglot_expr(
            "not an expression",  # type: ignore[arg-type]
            task_name="test",
            output="out",
        )
