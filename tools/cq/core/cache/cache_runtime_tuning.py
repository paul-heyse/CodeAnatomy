"""Diskcache runtime tuning helpers."""

from __future__ import annotations

import os
from contextlib import suppress

from tools.cq.core.cache.cache_runtime_tuning_contracts import CacheRuntimeTuningV1
from tools.cq.core.cache.policy import CqCachePolicyV1


def resolve_cache_runtime_tuning(policy: CqCachePolicyV1) -> CacheRuntimeTuningV1:
    """Resolve runtime tuning settings from policy and optional env overrides.

    Returns:
        CacheRuntimeTuningV1: Effective runtime tuning configuration.
    """
    mmap_size = int(os.getenv("CQ_CACHE_SQLITE_MMAP_SIZE", str(policy.sqlite_mmap_size)) or 0)
    cache_size = int(os.getenv("CQ_CACHE_SQLITE_CACHE_SIZE", str(policy.sqlite_cache_size)) or 0)
    cull_limit = int(os.getenv("CQ_CACHE_CULL_LIMIT", str(policy.cull_limit)) or policy.cull_limit)
    batch_size = int(
        os.getenv("CQ_CACHE_TRANSACTION_BATCH_SIZE", str(policy.transaction_batch_size))
        or policy.transaction_batch_size
    )
    eviction_policy = (
        os.getenv("CQ_CACHE_EVICTION_POLICY", policy.eviction_policy) or policy.eviction_policy
    ).strip()
    stats_enabled_raw = os.getenv("CQ_CACHE_STATISTICS_ENABLED")
    stats_enabled = policy.statistics_enabled
    if isinstance(stats_enabled_raw, str) and stats_enabled_raw.strip():
        stats_enabled = stats_enabled_raw.strip().lower() in {"1", "true", "yes", "on"}
    tag_index_raw = os.getenv("CQ_CACHE_TAG_INDEX_ENABLED")
    create_tag_index = True
    if isinstance(tag_index_raw, str) and tag_index_raw.strip():
        create_tag_index = tag_index_raw.strip().lower() in {"1", "true", "yes", "on"}
    return CacheRuntimeTuningV1(
        cull_limit=max(0, cull_limit),
        eviction_policy=eviction_policy or policy.eviction_policy,
        statistics_enabled=bool(stats_enabled),
        create_tag_index=bool(create_tag_index),
        sqlite_mmap_size=max(0, mmap_size),
        sqlite_cache_size=max(0, cache_size),
        transaction_batch_size=max(1, batch_size),
    )


def apply_cache_runtime_tuning(cache: object, tuning: CacheRuntimeTuningV1) -> None:
    """Best-effort apply tuning using diskcache reset API when available."""
    reset_fn = getattr(cache, "reset", None)
    if callable(reset_fn):
        with suppress(RuntimeError, TypeError, ValueError, AttributeError):
            reset_fn("cull_limit", tuning.cull_limit)
        with suppress(RuntimeError, TypeError, ValueError, AttributeError):
            reset_fn("eviction_policy", tuning.eviction_policy)
        with suppress(RuntimeError, TypeError, ValueError, AttributeError):
            reset_fn("statistics", 1 if tuning.statistics_enabled else 0)
        if tuning.sqlite_mmap_size > 0:
            with suppress(RuntimeError, TypeError, ValueError, AttributeError):
                reset_fn("sqlite_mmap_size", tuning.sqlite_mmap_size)
        if tuning.sqlite_cache_size > 0:
            with suppress(RuntimeError, TypeError, ValueError, AttributeError):
                reset_fn("sqlite_cache_size", tuning.sqlite_cache_size)
    if tuning.create_tag_index:
        create_tag_index_fn = getattr(cache, "create_tag_index", None)
        if callable(create_tag_index_fn):
            with suppress(RuntimeError, TypeError, ValueError, AttributeError):
                create_tag_index_fn()


__all__ = ["apply_cache_runtime_tuning", "resolve_cache_runtime_tuning"]
