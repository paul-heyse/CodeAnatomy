from __future__ import annotations

import os

from tools.cq.core.cache.cache_runtime_tuning import (
    apply_cache_runtime_tuning,
    resolve_cache_runtime_tuning,
)
from tools.cq.core.cache.policy import CqCachePolicyV1


class _FakeCache:
    def __init__(self) -> None:
        self.reset_calls: list[tuple[str, object]] = []
        self.tag_index_created = False

    def reset(self, key: str, value: object) -> None:
        self.reset_calls.append((key, value))

    def create_tag_index(self) -> None:
        self.tag_index_created = True


def test_resolve_cache_runtime_tuning_uses_policy_defaults() -> None:
    policy = CqCachePolicyV1(
        sqlite_mmap_size=4096,
        sqlite_cache_size=2048,
        cull_limit=8,
        eviction_policy="least-recently-used",
        statistics_enabled=True,
        transaction_batch_size=64,
    )
    tuning = resolve_cache_runtime_tuning(policy)
    assert tuning.sqlite_mmap_size == 4096
    assert tuning.sqlite_cache_size == 2048
    assert tuning.cull_limit == 8
    assert tuning.eviction_policy == "least-recently-used"
    assert tuning.statistics_enabled is True
    assert tuning.transaction_batch_size == 64


def test_apply_cache_runtime_tuning_resets_cache_runtime_knobs() -> None:
    cache = _FakeCache()
    policy = CqCachePolicyV1(
        sqlite_mmap_size=8192,
        sqlite_cache_size=4096,
        cull_limit=4,
        eviction_policy="least-recently-used",
        statistics_enabled=True,
    )
    tuning = resolve_cache_runtime_tuning(policy)
    apply_cache_runtime_tuning(cache, tuning)
    assert ("cull_limit", 4) in cache.reset_calls
    assert ("eviction_policy", "least-recently-used") in cache.reset_calls
    assert ("statistics", 1) in cache.reset_calls
    assert ("sqlite_mmap_size", 8192) in cache.reset_calls
    assert ("sqlite_cache_size", 4096) in cache.reset_calls
    assert cache.tag_index_created is True


def test_resolve_cache_runtime_tuning_env_overrides() -> None:
    os.environ["CQ_CACHE_CULL_LIMIT"] = "11"
    os.environ["CQ_CACHE_EVICTION_POLICY"] = "least-frequently-used"
    os.environ["CQ_CACHE_STATISTICS_ENABLED"] = "0"
    os.environ["CQ_CACHE_TRANSACTION_BATCH_SIZE"] = "222"
    policy = CqCachePolicyV1()
    tuning = resolve_cache_runtime_tuning(policy)
    assert tuning.cull_limit == 11
    assert tuning.eviction_policy == "least-frequently-used"
    assert tuning.statistics_enabled is False
    assert tuning.transaction_batch_size == 222
    os.environ.pop("CQ_CACHE_CULL_LIMIT", None)
    os.environ.pop("CQ_CACHE_EVICTION_POLICY", None)
    os.environ.pop("CQ_CACHE_STATISTICS_ENABLED", None)
    os.environ.pop("CQ_CACHE_TRANSACTION_BATCH_SIZE", None)
