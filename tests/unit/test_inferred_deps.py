"""Tests for inferred dependencies module."""

from __future__ import annotations

from typing import cast

import pytest

from relspec.inferred_deps import (
    InferredDeps,
    InferredDepsInputs,
    infer_deps_from_plan_bundle,
    infer_deps_from_sqlglot_expr,
)


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
        InferredDepsInputs(
            expr=expr,
            task_name="join_task",
            output="joined",
        )
    )
    assert deps.task_name == "join_task"
    assert deps.output == "joined"
    assert "table_a" in deps.inputs or "a" in deps.inputs
    assert deps.plan_fingerprint


def test_infer_deps_from_sqlglot_expr_invalid_type() -> None:
    """Raise when expression is not SQLGlot Expression."""
    from sqlglot_tools.compat import Expression

    with pytest.raises(TypeError, match="Expected SQLGlot Expression"):
        infer_deps_from_sqlglot_expr(
            InferredDepsInputs(
                expr=cast("Expression", "not an expression"),
                task_name="test",
                output="out",
            )
        )


def test_infer_deps_from_plan_bundle() -> None:
    """Infer dependencies from DataFusion plan bundle."""
    from datafusion import SessionContext

    from datafusion_engine.plan_bundle import build_plan_bundle

    # Create a test DataFusion plan
    ctx = SessionContext()
    ctx.register_csv("table_a", "tests/fixtures/test.csv", has_header=True)
    ctx.register_csv("table_b", "tests/fixtures/test2.csv", has_header=True)

    # Build a simple query
    df = ctx.sql("SELECT a.x, b.y FROM table_a a JOIN table_b b ON a.id = b.id")
    plan_bundle = build_plan_bundle(ctx, df)

    # Infer dependencies using the plan bundle
    deps = infer_deps_from_plan_bundle(
        InferredDepsInputs(
            expr=None,  # Not used when plan_bundle is provided
            task_name="datafusion_join_task",
            output="datafusion_joined",
            plan_bundle=plan_bundle,
        )
    )

    # Verify the inferred dependencies
    assert deps.task_name == "datafusion_join_task"
    assert deps.output == "datafusion_joined"
    assert "table_a" in deps.inputs or "a" in deps.inputs
    assert "table_b" in deps.inputs or "b" in deps.inputs
    assert deps.plan_fingerprint == plan_bundle.plan_fingerprint


def test_infer_deps_from_plan_bundle_missing_bundle() -> None:
    """Raise when plan_bundle is None."""
    with pytest.raises(ValueError, match="plan_bundle must be provided"):
        infer_deps_from_plan_bundle(
            InferredDepsInputs(
                expr=None,
                task_name="test",
                output="out",
                plan_bundle=None,
            )
        )


def test_sqlglot_expr_delegates_to_plan_bundle() -> None:
    """Verify infer_deps_from_sqlglot_expr delegates to plan bundle when available."""
    from datafusion import SessionContext

    from datafusion_engine.plan_bundle import build_plan_bundle

    # Create a test DataFusion plan
    ctx = SessionContext()
    ctx.register_csv("test_table", "tests/fixtures/test.csv", has_header=True)
    df = ctx.sql("SELECT x FROM test_table")
    plan_bundle = build_plan_bundle(ctx, df)

    # Call the SQLGlot function with a plan_bundle
    deps = infer_deps_from_sqlglot_expr(
        InferredDepsInputs(
            expr=None,  # Should be ignored when plan_bundle is present
            task_name="delegation_test",
            output="delegated",
            plan_bundle=plan_bundle,
        )
    )

    # Verify it used the plan bundle path
    assert deps.task_name == "delegation_test"
    assert deps.plan_fingerprint == plan_bundle.plan_fingerprint
