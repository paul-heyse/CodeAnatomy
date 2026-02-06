"""Shared commit metadata helpers for Delta-backed cache writes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from obs.otel.run_context import get_run_id


@dataclass(frozen=True)
class CacheCommitMetadataRequest:
    """Inputs for cache commit metadata payloads."""

    operation: str
    cache_policy: str
    cache_scope: str
    schema_hash: str | None = None
    plan_hash: str | None = None
    cache_key: str | None = None
    result: str | None = None
    extra: Mapping[str, object] | None = None


def cache_commit_metadata(request: CacheCommitMetadataRequest) -> dict[str, str]:
    """Return commit metadata for cache-related Delta writes.

    Parameters
    ----------
    request
        Commit metadata inputs for cache operations.

    Returns:
    -------
    dict[str, str]
        Stringified commit metadata entries.
    """
    payload: dict[str, object] = {
        # Delta commit info already writes a reserved "operation" key.
        # Use a cache-specific key to avoid duplicate JSON fields in commit logs.
        "cache_operation": request.operation,
        "cache_policy": request.cache_policy,
        "cache_scope": request.cache_scope,
    }
    run_id = get_run_id()
    if run_id:
        payload["run_id"] = run_id
    if request.schema_hash:
        payload["schema_hash"] = request.schema_hash
    if request.plan_hash:
        payload["plan_hash"] = request.plan_hash
    if request.cache_key:
        payload["cache_key"] = request.cache_key
    if request.result:
        payload["cache_result"] = request.result
    if request.extra:
        for key, value in request.extra.items():
            if value is None:
                continue
            payload[str(key)] = value
    return {str(key): str(value) for key, value in payload.items() if value is not None}


__all__ = ["CacheCommitMetadataRequest", "cache_commit_metadata"]
