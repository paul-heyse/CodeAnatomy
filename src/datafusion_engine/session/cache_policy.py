"""Cache policy configuration for DataFusion runtime sessions."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from core.config_base import config_fingerprint
from serde_msgspec import StructBaseStrict


class CachePolicyConfig(StructBaseStrict, frozen=True):
    """Explicit cache policy settings for DataFusion.

    Attributes:
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

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for cache policy settings.

        Returns:
        -------
        Mapping[str, object]
            Payload describing cache policy settings.
        """
        return {
            "listing_cache_size": self.listing_cache_size,
            "metadata_cache_size": self.metadata_cache_size,
            "stats_cache_size": self.stats_cache_size,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for cache policy settings.

        Returns:
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


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

    Returns:
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
