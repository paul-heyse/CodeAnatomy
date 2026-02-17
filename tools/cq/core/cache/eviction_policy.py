"""Cache eviction policy helpers."""

from __future__ import annotations

import msgspec


class EvictionPolicy(msgspec.Struct, frozen=True, kw_only=True):
    """Cache eviction configuration."""

    max_size_bytes: int = 100 * 1024 * 1024
    ttl_seconds: int = 3600


def apply_eviction(*, policy: EvictionPolicy, current_size_bytes: int) -> bool:
    """Return whether eviction should run for the current cache size."""
    return int(current_size_bytes) > int(policy.max_size_bytes)


__all__ = ["EvictionPolicy", "apply_eviction"]
