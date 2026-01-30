"""Cache configuration types for the Hamilton pipeline."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CacheRuntimeContext:
    """Snapshot of Hamilton cache configuration for run manifests."""

    cache_path: str | None
    cache_log_dir: str | None
    cache_log_glob: str | None
    cache_policy_profile: str | None
    cache_log_enabled: bool = True


__all__ = ["CacheRuntimeContext"]
