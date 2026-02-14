"""Cache namespace defaults and TTL resolution utilities."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.core.cache.policy import CqCachePolicyV1

_DEFAULT_NAMESPACE_TTLS_SECONDS: dict[str, int] = {
    "search_candidates": 120,
    "search_enrichment": 900,
    "query_entity_scan": 900,
    "query_entity_fragment": 900,
    "pattern_scan": 600,
    "pattern_fragment": 600,
    "calls_target_metadata": 900,
    "semantic_front_door": 300,
    "neighborhood_snapshot": 600,
    "neighborhood_fragment": 600,
    "file_inventory": 180,
    "scope_snapshot": 180,
}


def cache_namespace_env_suffix(namespace: str) -> str:
    """Return uppercase env suffix for a cache namespace name."""
    normalized = []
    for char in namespace:
        if char.isalnum():
            normalized.append(char.upper())
        else:
            normalized.append("_")
    return "".join(normalized)


def resolve_namespace_ttl_seconds(*, policy: CqCachePolicyV1, namespace: str) -> int:
    """Resolve namespace TTL from policy overrides and defaults.

    Returns:
        Effective TTL in seconds for the namespace.
    """
    override = policy.namespace_ttl_seconds.get(namespace)
    if isinstance(override, int) and override > 0:
        return override
    default = _DEFAULT_NAMESPACE_TTLS_SECONDS.get(namespace)
    if isinstance(default, int) and default > 0:
        return default
    return int(policy.ttl_seconds)


def is_namespace_cache_enabled(*, policy: CqCachePolicyV1, namespace: str) -> bool:
    """Return whether caching is enabled for a namespace."""
    override = policy.namespace_enabled.get(namespace)
    if override is None:
        return True
    return bool(override)


def is_namespace_ephemeral(*, policy: CqCachePolicyV1, namespace: str) -> bool:
    """Return whether namespace cache entries should be tagged as run-ephemeral."""
    override = policy.namespace_ephemeral.get(namespace)
    return bool(override)


def namespace_defaults() -> dict[str, int]:
    """Return a copy of built-in namespace TTL defaults."""
    return dict(_DEFAULT_NAMESPACE_TTLS_SECONDS)


__all__ = [
    "cache_namespace_env_suffix",
    "is_namespace_cache_enabled",
    "is_namespace_ephemeral",
    "namespace_defaults",
    "resolve_namespace_ttl_seconds",
]
