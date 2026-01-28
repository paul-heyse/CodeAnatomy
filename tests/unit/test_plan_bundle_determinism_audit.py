"""Tests for determinism audit bundles in plan details."""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine.plan_bundle import PlanBundleOptions, build_plan_bundle
from datafusion_engine.runtime import DataFusionRuntimeProfile

datafusion = pytest.importorskip("datafusion")

_SHA256_HEX_LENGTH = 64


def test_plan_bundle_determinism_audit_bundle() -> None:
    """Ensure determinism audit bundle captures core hash inputs."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    session_runtime = profile.session_runtime()
    ctx.register_record_batches(
        "events",
        [pa.table({"id": [1, 2], "label": ["a", "b"]}).to_batches()],
    )
    df = ctx.sql("SELECT id FROM events")
    bundle = build_plan_bundle(
        ctx,
        df,
        options=PlanBundleOptions(session_runtime=session_runtime),
    )
    audit = bundle.plan_details.get("determinism_audit")
    assert isinstance(audit, dict)
    assert audit.get("plan_fingerprint") == bundle.plan_fingerprint
    assert audit.get("planning_env_hash") == bundle.artifacts.planning_env_hash
    assert audit.get("rulepack_hash") == bundle.artifacts.rulepack_hash
    assert audit.get("information_schema_hash") == bundle.artifacts.information_schema_hash
    assert audit.get("udf_snapshot_hash") == bundle.artifacts.udf_snapshot_hash
    assert audit.get("function_registry_hash") == bundle.artifacts.function_registry_hash
    df_settings_hash = audit.get("df_settings_hash")
    assert isinstance(df_settings_hash, str)
    assert len(df_settings_hash) == _SHA256_HEX_LENGTH
