"""Run-scoped cache tag helpers and optional run-end eviction."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.cache.backend_core import get_cq_cache_backend
from tools.cq.core.cache.key_builder import (
    build_namespace_cache_tag,
    build_run_cache_tag,
)
from tools.cq.core.cache.namespaces import is_namespace_ephemeral
from tools.cq.core.cache.policy import CqCachePolicyV1, default_cache_policy
from tools.cq.core.cache.telemetry import record_cache_evict
from tools.cq.core.structs import CqStruct


class CacheWriteTagRequestV1(CqStruct, frozen=True):
    """Request envelope for namespace write-tag resolution."""

    policy: CqCachePolicyV1
    workspace: str
    language: str
    namespace: str
    scope_hash: str | None = None
    snapshot: str | None = None
    run_id: str | None = None


def resolve_write_cache_tag(
    request: CacheWriteTagRequestV1,
) -> str:
    """Resolve namespace write tag, optionally using run-scoped ephemerality.

    Returns:
        str: Write tag for the selected cache namespace.
    """
    if (
        is_namespace_ephemeral(policy=request.policy, namespace=request.namespace)
        and request.run_id
    ):
        return build_run_cache_tag(
            workspace=request.workspace,
            language=request.language,
            run_id=request.run_id,
        )
    return build_namespace_cache_tag(
        workspace=request.workspace,
        language=request.language,
        namespace=request.namespace,
        scope_hash=request.scope_hash,
        snapshot=request.snapshot,
        run_id=(
            request.run_id
            if is_namespace_ephemeral(policy=request.policy, namespace=request.namespace)
            else None
        ),
    )


def maybe_evict_run_cache_tag(
    *,
    root: Path,
    language: str,
    run_id: str | None,
) -> bool:
    """Evict run-scoped cache products when configured.

    Returns:
        bool: `True` when eviction runs and succeeds, `False` otherwise.
    """
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
    "CacheWriteTagRequestV1",
    "maybe_evict_run_cache_tag",
    "resolve_write_cache_tag",
]
