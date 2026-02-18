"""Context-pool policy immutability and isolation tests."""

from __future__ import annotations

from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def test_context_cache_is_not_process_global_across_profiles() -> None:
    """Equivalent profiles keep independent pooled SessionContext instances."""
    profile_a = DataFusionRuntimeProfile()
    profile_b = DataFusionRuntimeProfile()

    ctx_a_1 = profile_a.session_context()
    ctx_a_2 = profile_a.session_context()
    ctx_b_1 = profile_b.session_context()
    ctx_b_2 = profile_b.session_context()

    assert ctx_a_1 is ctx_a_2
    assert ctx_b_1 is ctx_b_2
    assert ctx_a_1 is not ctx_b_1
