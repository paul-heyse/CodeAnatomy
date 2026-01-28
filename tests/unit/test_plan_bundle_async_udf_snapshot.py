"""Tests for async UDF planning settings in plan bundles."""

from __future__ import annotations

import pytest


def test_plan_bundle_captures_async_udf_settings() -> None:
    """Ensure async UDF runtime settings are captured in plan artifacts."""
    pytest.importorskip("datafusion")
    try:
        import datafusion.substrait  # noqa: F401
    except ImportError:
        pytest.skip("datafusion.substrait is required for plan bundle construction.")
    import pyarrow as pa

    try:
        import datafusion_ext  # noqa: F401
    except ImportError:
        pytest.skip("datafusion_ext is required for plan bundle construction.")

    from datafusion_engine.plan_bundle import PlanBundleOptions, build_plan_bundle
    from datafusion_engine.runtime import DataFusionRuntimeProfile

    async_timeout_ms = 2500
    async_batch_size = 128
    profile = DataFusionRuntimeProfile(
        enable_async_udfs=True,
        async_udf_timeout_ms=async_timeout_ms,
        async_udf_batch_size=async_batch_size,
    )
    runtime = profile.session_runtime()
    ctx = runtime.ctx
    ctx.register_record_batches(
        "events",
        [pa.table({"id": [1, 2]}).to_batches()[0]],
    )
    df = ctx.sql("SELECT id FROM events")
    bundle = build_plan_bundle(
        ctx,
        df,
        options=PlanBundleOptions(
            compute_execution_plan=False,
            session_runtime=runtime,
        ),
    )
    async_payload = bundle.artifacts.planning_env_snapshot.get("async_udf")
    assert isinstance(async_payload, dict)
    assert async_payload["enable_async_udfs"] is True
    assert async_payload["async_udf_timeout_ms"] == async_timeout_ms
    assert async_payload["async_udf_batch_size"] == async_batch_size
