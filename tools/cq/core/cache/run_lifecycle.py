"""Run-scoped cache tag helpers and optional run-end eviction."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend
from tools.cq.core.cache.key_builder import (
    build_namespace_cache_tag,
    build_run_cache_tag,
)
from tools.cq.core.cache.namespaces import is_namespace_ephemeral
from tools.cq.core.cache.policy import CqCachePolicyV1, default_cache_policy
from tools.cq.core.cache.telemetry import record_cache_evict


def resolve_write_cache_tag(
    *,
    policy: CqCachePolicyV1,
    workspace: str,
    language: str,
    namespace: str,
    scope_hash: str | None = None,
    snapshot: str | None = None,
    run_id: str | None = None,
) -> str:
    """Resolve namespace write tag, optionally using run-scoped ephemerality."""
    if is_namespace_ephemeral(policy=policy, namespace=namespace) and run_id:
        return build_run_cache_tag(
            workspace=workspace,
            language=language,
            run_id=run_id,
        )
    return build_namespace_cache_tag(
        workspace=workspace,
        language=language,
        namespace=namespace,
        scope_hash=scope_hash,
        snapshot=snapshot,
        run_id=run_id if is_namespace_ephemeral(policy=policy, namespace=namespace) else None,
    )


def maybe_evict_run_cache_tag(
    *,
    root: Path,
    language: str,
    run_id: str | None,
) -> bool:
    """Evict run-scoped cache products when configured."""
    if not run_id:
        return False
    policy = default_cache_policy(root=root)
    if not policy.evict_run_tag_on_exit:
        return False
    backend = get_cq_cache_backend(root=root)
    tag = build_run_cache_tag(
        workspace=str(root.resolve()),
        language=language,
        run_id=run_id,
    )
    ok = backend.evict_tag(tag)
    record_cache_evict(namespace="run_lifecycle", ok=ok, tag=tag)
    return ok


__all__ = [
    "maybe_evict_run_cache_tag",
    "resolve_write_cache_tag",
]
