"""Tests for view cache registration helper exports."""

from __future__ import annotations

from datafusion_engine.views import cache_registration


def test_cache_registration_exports() -> None:
    """Cache registration module should expose expected helper callables."""
    assert callable(cache_registration.register_view_with_cache)
    assert callable(cache_registration.register_delta_staging_cache)
    assert callable(cache_registration.register_delta_output_cache)
    assert callable(cache_registration.register_uncached_view)
