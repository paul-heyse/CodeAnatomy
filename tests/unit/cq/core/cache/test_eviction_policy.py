"""Unit tests for cache eviction-policy helpers."""

from __future__ import annotations

from tools.cq.core.cache.eviction_policy import EvictionPolicy, apply_eviction


def test_apply_eviction_when_over_limit() -> None:
    """Eviction should trigger when size exceeds policy limit."""
    policy = EvictionPolicy(max_size_bytes=10)
    assert apply_eviction(policy=policy, current_size_bytes=11)


def test_apply_eviction_when_within_limit() -> None:
    """Eviction should not trigger when size is at or below the limit."""
    policy = EvictionPolicy(max_size_bytes=10)
    assert not apply_eviction(policy=policy, current_size_bytes=10)
