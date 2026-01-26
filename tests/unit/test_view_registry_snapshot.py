"""Tests for DataFusion view registry snapshots."""

from __future__ import annotations

import pyarrow as pa
import sqlglot.expressions as exp

from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.view_artifacts import ViewArtifactInputs, build_view_artifact


def test_view_registry_snapshot_stable_for_repeated_registration() -> None:
    """Keep view registry snapshots stable across repeated registrations."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    registry = profile.view_registry
    assert registry is not None
    schema = pa.schema([])
    alpha = build_view_artifact(
        ViewArtifactInputs(
            ctx=ctx,
            name="alpha_view",
            ast=exp.select(exp.Literal.number(1)),
            schema=schema,
            sql="SELECT 1",
        )
    )
    beta = build_view_artifact(
        ViewArtifactInputs(
            ctx=ctx,
            name="beta_view",
            ast=exp.select(exp.Literal.number(2)),
            schema=schema,
            sql="SELECT 2",
        )
    )
    registry.record(name="alpha_view", artifact=alpha)
    registry.record(name="beta_view", artifact=beta)
    first_snapshot = registry.snapshot()
    registry.record(name="alpha_view", artifact=alpha)
    second_snapshot = registry.snapshot()
    assert first_snapshot == second_snapshot
