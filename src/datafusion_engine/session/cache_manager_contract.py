"""Typed cache-manager policy contract for runtime construction."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion_engine.session.delta_session_builder import (
    parse_runtime_duration_seconds,
    parse_runtime_size,
)
from serde_msgspec import StructBaseStrict


class CacheManagerContractV1(StructBaseStrict, frozen=True):
    """Canonical cache-manager policy payload."""

    enabled: bool = False
    metadata_cache_limit_bytes: int | None = None
    list_files_cache_limit_bytes: int | None = None
    list_files_cache_ttl_seconds: int | None = None

    def payload(self) -> Mapping[str, object]:
        """Return canonical mapping payload for bridge/runtime serialization."""
        return {
            "enabled": self.enabled,
            "metadata_cache_limit_bytes": self.metadata_cache_limit_bytes,
            "list_files_cache_limit_bytes": self.list_files_cache_limit_bytes,
            "list_files_cache_ttl_seconds": self.list_files_cache_ttl_seconds,
        }


def cache_manager_contract_from_settings(
    *,
    enabled: bool,
    settings: Mapping[str, str],
) -> CacheManagerContractV1:
    """Compile cache-manager contract from runtime settings payload.

    Returns:
        CacheManagerContractV1: Typed contract with parsed byte/TTL limits.
    """
    metadata_limit = parse_runtime_size(settings.get("datafusion.runtime.metadata_cache_limit"))
    list_files_limit = parse_runtime_size(settings.get("datafusion.runtime.list_files_cache_limit"))
    ttl_seconds = parse_runtime_duration_seconds(
        settings.get("datafusion.runtime.list_files_cache_ttl")
    )
    return CacheManagerContractV1(
        enabled=enabled,
        metadata_cache_limit_bytes=metadata_limit,
        list_files_cache_limit_bytes=list_files_limit,
        list_files_cache_ttl_seconds=ttl_seconds,
    )


__all__ = [
    "CacheManagerContractV1",
    "cache_manager_contract_from_settings",
]
