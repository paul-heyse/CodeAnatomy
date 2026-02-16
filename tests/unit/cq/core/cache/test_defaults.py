"""Tests for canonical cache defaults."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.core.cache.defaults import (
    DEFAULT_CACHE_CULL_LIMIT,
    DEFAULT_CACHE_EVICTION_POLICY,
    DEFAULT_CACHE_SHARDS,
    DEFAULT_CACHE_TIMEOUT_SECONDS,
    DEFAULT_CACHE_TTL_SECONDS,
)
from tools.cq.core.cache.policy import default_cache_policy
from tools.cq.core.runtime.execution_policy import default_runtime_execution_policy


def test_default_cache_policy_uses_shared_defaults(tmp_path: Path) -> None:
    """Default cache policy mirrors shared cache defaults."""
    policy = default_cache_policy(root=tmp_path)

    assert policy.ttl_seconds == DEFAULT_CACHE_TTL_SECONDS
    assert policy.shards == DEFAULT_CACHE_SHARDS
    assert float(policy.timeout_seconds) == pytest.approx(DEFAULT_CACHE_TIMEOUT_SECONDS)
    assert policy.cull_limit == DEFAULT_CACHE_CULL_LIMIT
    assert policy.eviction_policy == DEFAULT_CACHE_EVICTION_POLICY


def test_runtime_execution_policy_uses_shared_cache_defaults() -> None:
    """Runtime execution policy inherits shared cache defaults."""
    runtime = default_runtime_execution_policy().cache

    assert runtime.ttl_seconds == DEFAULT_CACHE_TTL_SECONDS
    assert runtime.shards == DEFAULT_CACHE_SHARDS
    assert runtime.cull_limit == DEFAULT_CACHE_CULL_LIMIT
    assert runtime.eviction_policy == DEFAULT_CACHE_EVICTION_POLICY
