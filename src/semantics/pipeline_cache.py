"""Semantic pipeline cache-policy helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from datafusion_engine.views.artifacts import CachePolicy


def _cache_policy_for(
    name: str,
    policy: Mapping[str, CachePolicy] | None,
) -> CachePolicy:
    """Resolve per-view cache policy value.

    Returns:
    -------
    CachePolicy
        Resolved cache policy for the view name.
    """
    if policy is None:
        return "none"
    return policy.get(name, "none")


def _normalize_cache_policy(policy: str) -> CachePolicy:
    """Validate cache policy value and normalize to literal.

    Returns:
    -------
    CachePolicy
        Normalized cache-policy literal.

    Raises:
        ValueError: If ``policy`` is not a supported cache policy.
    """
    valid = {"none", "memory", "delta_staging", "delta_output"}
    if policy in valid:
        return cast("CachePolicy", policy)
    msg = f"Unsupported cache policy: {policy!r}."
    raise ValueError(msg)


def _resolve_cache_policy_hierarchy(
    *,
    explicit_policy: Mapping[str, CachePolicy] | None,
    compiled_policy: Mapping[str, CachePolicy] | None,
) -> Mapping[str, CachePolicy]:
    """Resolve cache policy through explicit and compiled layers.

    Returns:
    -------
    Mapping[str, CachePolicy]
        Explicit policy when present, else compiled policy, else empty mapping.
    """
    if explicit_policy is not None:
        return explicit_policy
    if compiled_policy is not None:
        return compiled_policy
    return {}


def cache_policy_for(name: str, policy: Mapping[str, CachePolicy] | None) -> CachePolicy:
    """Public wrapper for per-view cache policy resolution.

    Returns:
    -------
    CachePolicy
        Resolved cache policy.
    """
    return _cache_policy_for(name, policy)


def normalize_cache_policy(policy: str) -> CachePolicy:
    """Public wrapper for cache-policy normalization.

    Returns:
    -------
    CachePolicy
        Normalized cache policy.
    """
    return _normalize_cache_policy(policy)


def resolve_cache_policy_hierarchy(
    *,
    explicit_policy: Mapping[str, CachePolicy] | None,
    compiled_policy: Mapping[str, CachePolicy] | None,
) -> Mapping[str, CachePolicy]:
    """Public wrapper for cache-policy hierarchy resolution.

    Returns:
    -------
    Mapping[str, CachePolicy]
        Resolved cache-policy mapping.
    """
    return _resolve_cache_policy_hierarchy(
        explicit_policy=explicit_policy,
        compiled_policy=compiled_policy,
    )


__all__ = [
    "cache_policy_for",
    "normalize_cache_policy",
    "resolve_cache_policy_hierarchy",
]
