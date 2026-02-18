"""Integration checks for runtime session-pool isolation."""

from __future__ import annotations

from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def test_session_pool_isolation() -> None:
    """Equivalent runtime profiles should not share pooled SessionContext objects."""
    profile_a = DataFusionRuntimeProfile()
    profile_b = DataFusionRuntimeProfile()

    pool_a = profile_a.context_pool()
    pool_b = profile_b.context_pool()

    with pool_a.checkout() as ctx_a:
        rows_a = ctx_a.sql("SELECT 1 AS v").to_arrow_table().to_pylist()
    with pool_b.checkout() as ctx_b:
        rows_b = ctx_b.sql("SELECT 1 AS v").to_arrow_table().to_pylist()

    assert rows_a == [{"v": 1}]
    assert rows_b == [{"v": 1}]
    assert ctx_a is not ctx_b
