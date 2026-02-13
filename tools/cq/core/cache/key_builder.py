"""Deterministic cache key builders for CQ runtime artifacts."""

from __future__ import annotations

import hashlib

import msgspec


def build_cache_key(
    namespace: str,
    *,
    version: str,
    workspace: str,
    language: str,
    target: str,
    extras: dict[str, object] | None = None,
) -> str:
    """Build a deterministic cache key string for CQ runtime data.

    Returns:
        Namespaced cache key that includes a stable payload digest.
    """
    payload = {
        "namespace": namespace,
        "version": version,
        "workspace": workspace,
        "language": language,
        "target": target,
        "extras": extras or {},
    }
    digest = hashlib.sha256(msgspec.json.encode(payload)).hexdigest()
    return f"cq:{namespace}:{version}:{digest}"


def build_cache_tag(*, workspace: str, language: str) -> str:
    """Build tag used for bulk invalidation.

    Returns:
        Workspace-language cache tag.
    """
    return f"{workspace}:{language}"


def build_run_cache_tag(*, workspace: str, language: str, run_id: str) -> str:
    """Build run-scoped cache invalidation tag.

    Returns:
        Run-scoped cache tag.
    """
    return f"{workspace}:{language}:run:{run_id}"


__all__ = ["build_cache_key", "build_cache_tag", "build_run_cache_tag"]
