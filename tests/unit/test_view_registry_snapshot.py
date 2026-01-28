"""Tests for DataFusion view registry snapshots."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.plan_bundle import PlanBundleOptions, build_plan_bundle
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.view_artifacts import (
    ViewArtifactLineage,
    ViewArtifactRequest,
    build_view_artifact_from_bundle,
)


def _arrow_schema_from_df(df: object) -> pa.Schema:
    """Resolve a DataFusion DataFrame schema into PyArrow.

    Returns
    -------
    pa.Schema
        Resolved PyArrow schema for the DataFrame.

    Raises
    ------
    TypeError
        Raised when the schema cannot be resolved to PyArrow.
    """
    schema = getattr(df, "schema", None)
    if callable(schema):
        schema = schema()
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Failed to resolve DataFusion schema."
    raise TypeError(msg)


def test_view_registry_snapshot_stable_for_repeated_registration() -> None:
    """Keep view registry snapshots stable across repeated registrations."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    session_runtime = profile.session_runtime()
    registry = profile.view_registry
    assert registry is not None
    alpha_df = ctx.sql("SELECT 1 AS value")
    beta_df = ctx.sql("SELECT 2 AS value")
    alpha_bundle = build_plan_bundle(
        ctx,
        alpha_df,
        options=PlanBundleOptions(session_runtime=session_runtime),
    )
    beta_bundle = build_plan_bundle(
        ctx,
        beta_df,
        options=PlanBundleOptions(session_runtime=session_runtime),
    )
    alpha_schema = _arrow_schema_from_df(alpha_df)
    beta_schema = _arrow_schema_from_df(beta_df)
    alpha = build_view_artifact_from_bundle(
        alpha_bundle,
        request=ViewArtifactRequest(
            name="alpha_view",
            schema=alpha_schema,
            lineage=ViewArtifactLineage(required_udfs=(), referenced_tables=()),
            runtime_hash=None,
        ),
    )
    beta = build_view_artifact_from_bundle(
        beta_bundle,
        request=ViewArtifactRequest(
            name="beta_view",
            schema=beta_schema,
            lineage=ViewArtifactLineage(required_udfs=(), referenced_tables=()),
            runtime_hash=None,
        ),
    )
    registry.record(name="alpha_view", artifact=alpha)
    registry.record(name="beta_view", artifact=beta)
    first_snapshot = registry.snapshot()
    registry.record(name="alpha_view", artifact=alpha)
    second_snapshot = registry.snapshot()
    assert first_snapshot == second_snapshot
