"""Cache registration helpers extracted from views.graph."""

from __future__ import annotations

from datafusion import DataFrame

from datafusion_engine.views.graph import (
    CacheRegistrationContext,
    _register_delta_output_cache,
    _register_delta_staging_cache,
    _register_uncached_view,
    _register_view_with_cache,
)


def register_view_with_cache(registration: CacheRegistrationContext) -> DataFrame:
    """Register a view according to cache policy.

    Returns:
        DataFrame: Registered DataFrame view handle.
    """
    return _register_view_with_cache(
        registration.ctx,
        node=registration.node,
        df=registration.df,
        cache=registration.cache,
        adapter=registration.adapter,
    )


def register_delta_staging_cache(registration: CacheRegistrationContext) -> DataFrame:
    """Register a view using delta_staging cache policy.

    Returns:
        DataFrame: Registered DataFrame view handle.
    """
    return _register_delta_staging_cache(registration)


def register_delta_output_cache(registration: CacheRegistrationContext) -> DataFrame:
    """Register a view using delta_output cache policy.

    Returns:
        DataFrame: Registered DataFrame view handle.
    """
    return _register_delta_output_cache(registration)


def register_uncached_view(registration: CacheRegistrationContext) -> DataFrame:
    """Register a view without caching.

    Returns:
        DataFrame: Registered DataFrame view handle.
    """
    return _register_uncached_view(registration)


__all__ = [
    "register_delta_output_cache",
    "register_delta_staging_cache",
    "register_uncached_view",
    "register_view_with_cache",
]
