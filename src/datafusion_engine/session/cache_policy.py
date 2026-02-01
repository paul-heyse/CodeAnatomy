"""Cache policy configuration for DataFusion runtime sessions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class CachePolicyConfig:
    """Explicit cache policy settings for DataFusion.

    Attributes
    ----------
    listing_cache_size
        Cache size for listing table files.
    metadata_cache_size
        Cache size for metadata entries.
    stats_cache_size
        Cache size for predicate/statistics caches when supported.
    """

    listing_cache_size: int
    metadata_cache_size: int
    stats_cache_size: int | None = None


DEFAULT_CACHE_POLICY: Final[CachePolicyConfig] = CachePolicyConfig(
    listing_cache_size=128 * 1024 * 1024,
    metadata_cache_size=256 * 1024 * 1024,
    stats_cache_size=64 * 1024 * 1024,
)


def cache_policy_settings(policy: CachePolicyConfig) -> dict[str, str]:
    """Return DataFusion settings derived from a cache policy.

    Parameters
    ----------
    policy
        Cache policy configuration.

    Returns
    -------
    dict[str, str]
        DataFusion config settings for cache limits.
    """
    settings = {
        "datafusion.runtime.list_files_cache_limit": str(policy.listing_cache_size),
        "datafusion.runtime.metadata_cache_limit": str(policy.metadata_cache_size),
    }
    if policy.stats_cache_size is not None:
        settings["datafusion.execution.parquet.max_predicate_cache_size"] = str(
            policy.stats_cache_size
        )
    return settings


__all__ = [
    "DEFAULT_CACHE_POLICY",
    "CachePolicyConfig",
    "cache_policy_settings",
]
