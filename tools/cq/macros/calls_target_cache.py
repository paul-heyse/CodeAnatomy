"""Cache orchestration helpers for calls target metadata."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from tools.cq.core.cache.contracts import CallsTargetCacheV1
from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend
from tools.cq.core.cache.interface import CqCacheBackend
from tools.cq.core.cache.key_builder import build_cache_key, build_scope_hash
from tools.cq.core.cache.namespaces import (
    is_namespace_cache_enabled,
    resolve_namespace_ttl_seconds,
)
from tools.cq.core.cache.policy import CqCachePolicyV1, default_cache_policy
from tools.cq.core.cache.run_lifecycle import CacheWriteTagRequestV1, resolve_write_cache_tag
from tools.cq.core.cache.snapshot_fingerprint import build_scope_snapshot_fingerprint
from tools.cq.core.cache.telemetry import (
    record_cache_decode_failure,
    record_cache_get,
    record_cache_set,
)
from tools.cq.core.contracts import contract_to_builtins

if TYPE_CHECKING:
    from collections.abc import Callable

    from tools.cq.query.language import QueryLanguage


@dataclass(frozen=True, slots=True)
class TargetMetadataCacheContext:
    """Cache context values used for target metadata lookup and persistence."""

    namespace: str
    root: Path
    cache: CqCacheBackend
    policy: CqCachePolicyV1
    cache_enabled: bool
    ttl_seconds: int
    cache_key: str
    scope_hash: str | None
    resolved_language: QueryLanguage | None


@dataclass(frozen=True, slots=True)
class TargetPayloadState:
    """Resolved target payload plus cache-write decision state."""

    target_location: tuple[str, int] | None
    target_callees: Counter[str]
    snapshot_digest: str | None
    should_write_cache: bool


def target_scope_snapshot_digest(
    *,
    root: Path,
    backend: CqCacheBackend,
    target_location: tuple[str, int] | None,
    language: QueryLanguage | None,
) -> str | None:
    """Build snapshot digest for one resolved target location.

    Returns:
        str | None: Snapshot digest for the target scope when available.
    """
    if target_location is None:
        return None
    file_path = root / target_location[0]
    if not file_path.exists():
        return None
    return build_scope_snapshot_fingerprint(
        root=root,
        backend=backend,
        files=[file_path],
        language=language or "python",
        scope_globs=[],
        scope_roots=[file_path.parent],
    ).digest


def resolve_target_payload(
    *,
    root: Path,
    backend: CqCacheBackend,
    function_name: str,
    resolved_language: QueryLanguage | None,
    resolve_target_definition: Callable[..., tuple[str, int] | None],
    scan_target_callees: Callable[..., Counter[str]],
) -> tuple[tuple[str, int] | None, Counter[str], str | None]:
    """Resolve target location/callees and compute scope digest.

    Returns:
        tuple[tuple[str, int] | None, Counter[str], str | None]: Target payload tuple.
    """
    resolved_target = resolve_target_definition(
        root,
        function_name,
        target_language=resolved_language,
    )
    resolved_callees = scan_target_callees(
        root,
        function_name,
        resolved_target,
        target_language=resolved_language,
    )
    resolved_snapshot = target_scope_snapshot_digest(
        root=root,
        backend=backend,
        target_location=resolved_target,
        language=resolved_language,
    )
    return resolved_target, resolved_callees, resolved_snapshot


def build_target_metadata_cache_context(
    *,
    root: Path,
    function_name: str,
    preview_limit: int,
    resolved_language: QueryLanguage | None,
) -> TargetMetadataCacheContext:
    """Build cache context for calls target metadata persistence.

    Returns:
        TargetMetadataCacheContext: Resolved cache context for target metadata.
    """
    namespace = "calls_target_metadata"
    resolved_root = root.resolve()
    policy = default_cache_policy(root=resolved_root)
    cache = get_cq_cache_backend(root=resolved_root)
    cache_enabled = is_namespace_cache_enabled(policy=policy, namespace=namespace)
    ttl_seconds = resolve_namespace_ttl_seconds(policy=policy, namespace=namespace)
    scope_hash = build_scope_hash(
        {
            "function_name": function_name,
            "lang": resolved_language or "auto",
            "preview_limit": preview_limit,
        }
    )
    cache_key = build_cache_key(
        namespace,
        version="v2",
        workspace=str(resolved_root),
        language=(resolved_language or "auto"),
        target=function_name,
        extras={
            "preview_limit": preview_limit,
            "scope_hash": scope_hash,
        },
    )
    return TargetMetadataCacheContext(
        namespace=namespace,
        root=resolved_root,
        cache=cache,
        policy=policy,
        cache_enabled=cache_enabled,
        ttl_seconds=ttl_seconds,
        cache_key=cache_key,
        scope_hash=scope_hash,
        resolved_language=resolved_language,
    )


def resolve_target_payload_state(
    *,
    context: TargetMetadataCacheContext,
    resolve_fn: Callable[[], tuple[tuple[str, int] | None, Counter[str], str | None]],
) -> TargetPayloadState:
    """Load target payload from cache, refreshing when invalid or stale.

    Returns:
        TargetPayloadState: Resolved payload state and cache-write decision.
    """
    cached = context.cache.get(context.cache_key) if context.cache_enabled else None
    if context.cache_enabled:
        record_cache_get(
            namespace=context.namespace,
            hit=isinstance(cached, dict),
            key=context.cache_key,
        )
    if not isinstance(cached, dict):
        target_location, target_callees, snapshot_digest = resolve_fn()
        return TargetPayloadState(
            target_location=target_location,
            target_callees=target_callees,
            snapshot_digest=snapshot_digest,
            should_write_cache=True,
        )
    try:
        cached_payload = msgspec.convert(cached, type=CallsTargetCacheV1)
    except (RuntimeError, TypeError, ValueError):
        if context.cache_enabled:
            record_cache_decode_failure(namespace=context.namespace)
        target_location, target_callees, snapshot_digest = resolve_fn()
        return TargetPayloadState(
            target_location=target_location,
            target_callees=target_callees,
            snapshot_digest=snapshot_digest,
            should_write_cache=True,
        )
    target_location = cached_payload.target_location
    target_callees = Counter(cached_payload.target_callees)
    snapshot_digest = cached_payload.snapshot_digest
    current_snapshot = target_scope_snapshot_digest(
        root=context.root,
        backend=context.cache,
        target_location=target_location,
        language=context.resolved_language,
    )
    if snapshot_digest != current_snapshot:
        target_location, target_callees, snapshot_digest = resolve_fn()
        return TargetPayloadState(
            target_location=target_location,
            target_callees=target_callees,
            snapshot_digest=snapshot_digest,
            should_write_cache=True,
        )
    if snapshot_digest is None and target_location is not None:
        return TargetPayloadState(
            target_location=target_location,
            target_callees=target_callees,
            snapshot_digest=current_snapshot,
            should_write_cache=True,
        )
    return TargetPayloadState(
        target_location=target_location,
        target_callees=target_callees,
        snapshot_digest=snapshot_digest,
        should_write_cache=False,
    )


def persist_target_metadata_cache(
    *,
    context: TargetMetadataCacheContext,
    payload_state: TargetPayloadState,
    run_id: str | None,
) -> None:
    """Persist calls target metadata payload to cache backend."""
    if not context.cache_enabled:
        return
    cache_payload = CallsTargetCacheV1(
        target_location=payload_state.target_location,
        target_callees=dict(payload_state.target_callees),
        snapshot_digest=payload_state.snapshot_digest,
    )
    tag = resolve_write_cache_tag(
        CacheWriteTagRequestV1(
            policy=context.policy,
            workspace=str(context.root),
            language=(context.resolved_language or "python"),
            namespace=context.namespace,
            scope_hash=context.scope_hash,
            snapshot=payload_state.snapshot_digest,
            run_id=run_id,
        )
    )
    ok = context.cache.set(
        context.cache_key,
        contract_to_builtins(cache_payload),
        expire=context.ttl_seconds,
        tag=tag,
    )
    record_cache_set(namespace=context.namespace, ok=ok, key=context.cache_key)


__all__ = [
    "TargetMetadataCacheContext",
    "TargetPayloadState",
    "build_target_metadata_cache_context",
    "persist_target_metadata_cache",
    "resolve_target_payload",
    "resolve_target_payload_state",
    "target_scope_snapshot_digest",
]
