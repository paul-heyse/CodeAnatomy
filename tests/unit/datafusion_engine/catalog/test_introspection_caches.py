"""Tests for introspection cache scoping behavior."""

from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.catalog.introspection import (
    IntrospectionCaches,
    introspection_cache_for_ctx,
)


def test_introspection_cache_is_scoped_by_injected_cache_container() -> None:
    """Injected cache containers isolate per-context introspection caches."""
    ctx = SessionContext()
    first_container = IntrospectionCaches()
    second_container = IntrospectionCaches()

    first_cache = introspection_cache_for_ctx(ctx, caches=first_container)
    first_cache_again = introspection_cache_for_ctx(ctx, caches=first_container)
    second_cache = introspection_cache_for_ctx(ctx, caches=second_container)

    assert first_cache is first_cache_again
    assert first_cache is not second_cache
