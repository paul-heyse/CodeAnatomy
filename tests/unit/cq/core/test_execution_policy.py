"""Tests for test_execution_policy."""

from __future__ import annotations

import pytest
from tools.cq.core.runtime.execution_policy import default_runtime_execution_policy

CACHE_SIZE_LIMIT_BYTES = 2048
SEARCH_CANDIDATES_TTL_SECONDS = 21


def test_runtime_execution_policy_parses_extended_cache_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure extended cache environment variables override runtime defaults."""
    monkeypatch.setenv("CQ_RUNTIME_CACHE_SIZE_LIMIT_BYTES", str(CACHE_SIZE_LIMIT_BYTES))
    monkeypatch.setenv("CQ_RUNTIME_CACHE_CULL_LIMIT", "0")
    monkeypatch.setenv("CQ_RUNTIME_CACHE_EVICTION_POLICY", "least-recently-used")
    monkeypatch.setenv("CQ_RUNTIME_CACHE_STATS_ENABLED", "1")
    monkeypatch.setenv("CQ_RUNTIME_CACHE_EVICT_RUN_TAG_ON_EXIT", "1")
    monkeypatch.setenv(
        "CQ_RUNTIME_CACHE_TTL_SEARCH_CANDIDATES_SECONDS",
        str(SEARCH_CANDIDATES_TTL_SECONDS),
    )
    monkeypatch.setenv("CQ_RUNTIME_CACHE_ENABLE_SEARCH_CANDIDATES", "0")
    monkeypatch.setenv("CQ_RUNTIME_CACHE_EPHEMERAL_SEARCH_CANDIDATES", "1")

    policy = default_runtime_execution_policy()

    assert policy.cache.size_limit_bytes == CACHE_SIZE_LIMIT_BYTES
    assert policy.cache.cull_limit == 0
    assert policy.cache.eviction_policy == "least-recently-used"
    assert policy.cache.statistics_enabled is True
    assert policy.cache.evict_run_tag_on_exit is True
    assert policy.cache.namespace_ttl_seconds["search_candidates"] == SEARCH_CANDIDATES_TTL_SECONDS
    assert policy.cache.namespace_enabled["search_candidates"] is False
    assert policy.cache.namespace_ephemeral["search_candidates"] is True
