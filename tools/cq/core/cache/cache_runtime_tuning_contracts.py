"""Contracts for diskcache runtime tuning configuration."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class CacheRuntimeTuningV1(CqStruct, frozen=True):
    """Resolved runtime tuning parameters for cache backend operations."""

    cull_limit: int = 16
    eviction_policy: str = "least-recently-stored"
    statistics_enabled: bool = False
    create_tag_index: bool = True
    sqlite_mmap_size: int = 0
    sqlite_cache_size: int = 0
    transaction_batch_size: int = 128


__all__ = ["CacheRuntimeTuningV1"]
