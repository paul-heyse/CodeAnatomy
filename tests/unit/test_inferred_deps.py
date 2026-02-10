"""Tests for inferred dependencies module."""

from __future__ import annotations

from relspec.inferred_deps import (
    InferredDeps,
    InferredDepsInputs,
    infer_deps_from_plan_bundle,
)
from tests.test_helpers.semantic_registry_runtime import semantic_registry_runtime


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


def test_infer_deps_from_plan_bundle() -> None:
    """Infer dependencies from DataFusion plan bundle."""
    import pyarrow as pa

    from datafusion_engine.plan.bundle_artifact import PlanBundleOptions, build_plan_artifact
    from tests.test_helpers.arrow_seed import register_arrow_table

    # Create a test DataFusion plan
    ctx, session_runtime = semantic_registry_runtime()
    table_a = pa.table({"id": [1, 2], "x": ["a", "b"]})
    table_b = pa.table({"id": [1, 2], "y": ["c", "d"]})
    register_arrow_table(ctx, name="table_a", value=table_a)
    register_arrow_table(ctx, name="table_b", value=table_b)

    # Build a simple query
    df = ctx.sql("SELECT a.x, b.y FROM table_a a JOIN table_b b ON a.id = b.id")
    plan_bundle = build_plan_artifact(
        ctx,
        df,
        options=PlanBundleOptions(session_runtime=session_runtime),
    )

    # Infer dependencies using the plan bundle
    deps = infer_deps_from_plan_bundle(
        InferredDepsInputs(
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
