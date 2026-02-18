"""Shared cache helpers for query execution workflows."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import msgspec

from tools.cq.core.cache.backend_core import get_cq_cache_backend
from tools.cq.core.cache.cache_decode import decode_cached_payload
from tools.cq.core.cache.content_hash import file_content_hash
from tools.cq.core.cache.fragment_contracts import (
    FragmentEntryV1,
    FragmentMissV1,
    FragmentRequestV1,
    FragmentWriteV1,
)
from tools.cq.core.cache.fragment_engine import FragmentPersistRuntimeV1, FragmentProbeRuntimeV1
from tools.cq.core.cache.fragment_runtime import FragmentScanResult, run_fragment_scan
from tools.cq.core.cache.interface import CqCacheBackend
from tools.cq.core.cache.key_builder import build_cache_key, build_scope_hash
from tools.cq.core.cache.namespaces import (
    is_namespace_cache_enabled,
    resolve_namespace_ttl_seconds,
)
from tools.cq.core.cache.policy import default_cache_policy
from tools.cq.core.cache.run_lifecycle import CacheWriteTagRequestV1, resolve_write_cache_tag
from tools.cq.core.cache.snapshot_fingerprint import (
    ScopeSnapshotBuildRequestV1,
    build_scope_snapshot_fingerprint,
)
from tools.cq.core.cache.telemetry import (
    record_cache_decode_failure,
    record_cache_get,
    record_cache_set,
)
from tools.cq.core.contracts import contract_to_builtins
from tools.cq.core.pathing import normalize_repo_relative_path
from tools.cq.core.types import QueryLanguage


@dataclass(frozen=True)
class QueryFragmentCacheContext:
    """Shared fragment-cache lifecycle context."""

    namespace: str
    root: Path
    language: QueryLanguage
    files: list[Path]
    cache: CqCacheBackend
    cache_enabled: bool
    ttl_seconds: int
    tag: str


@dataclass(frozen=True)
class CachedScanRequest:
    """Request envelope for cached read-through scan execution."""

    root: Path
    cache_key: str
    backend: CqCacheBackend | None
    expire: int | None = None
    tag: str | None = None


@dataclass(frozen=True)
class QueryFragmentContextBuildRequest:
    """Input envelope for fragment cache context construction."""

    root: Path
    files: Sequence[Path]
    scope_roots: Sequence[Path]
    scope_globs: Sequence[str] | None
    namespace: str
    language: QueryLanguage
    scope_hash_extras: Mapping[str, object] | None = None
    run_id: str | None = None


def cached_scan[T](
    *,
    request: CachedScanRequest,
    scan_fn: Callable[[], T],
    type_: type[T],
) -> T:
    """Execute scan with cache read-through/write-back semantics.

    Returns:
        T: Cached or freshly scanned payload.
    """
    if request.backend is not None:
        cached = request.backend.get(request.cache_key)
        decoded, attempted = decode_cached_payload(
            root=request.root,
            backend=request.backend,
            payload=cached,
            type_=type_,
        )
        if attempted and decoded is not None:
            return decoded

    result = scan_fn()

    if request.backend is None:
        return result

    try:
        request.backend.set(
            request.cache_key,
            msgspec.msgpack.encode(result),
            expire=request.expire,
            tag=request.tag,
        )
    except (TypeError, ValueError, RuntimeError):
        # Cache write failures are non-fatal for query execution.
        return result

    return result


def build_query_fragment_cache_context(
    request: QueryFragmentContextBuildRequest,
) -> QueryFragmentCacheContext:
    """Build shared fragment-cache lifecycle context for query scans.

    Returns:
        QueryFragmentCacheContext: Resolved context with backend policy decisions.
    """
    resolved_root = request.root.resolve()
    ordered_files = sorted(
        (path.resolve() for path in request.files), key=lambda item: item.as_posix()
    )
    scope_hash_payload: dict[str, object] = {
        "paths": tuple(sorted(str(path.resolve()) for path in request.scope_roots)),
        "scope_globs": tuple(request.scope_globs or ()),
        "lang": request.language,
    }
    if request.scope_hash_extras is not None:
        for key in sorted(request.scope_hash_extras):
            scope_hash_payload[key] = request.scope_hash_extras[key]
    scope_hash = build_scope_hash(scope_hash_payload)

    cache = get_cq_cache_backend(root=resolved_root)
    snapshot = build_scope_snapshot_fingerprint(
        request=ScopeSnapshotBuildRequestV1(
            root=resolved_root,
            files=tuple(ordered_files),
            language=request.language,
            scope_globs=tuple(request.scope_globs or ()),
            scope_roots=tuple(request.scope_roots),
        ),
        backend=cache,
    )
    policy = default_cache_policy(root=resolved_root)
    return QueryFragmentCacheContext(
        namespace=request.namespace,
        root=resolved_root,
        language=request.language,
        files=ordered_files,
        cache=cache,
        cache_enabled=is_namespace_cache_enabled(policy=policy, namespace=request.namespace),
        ttl_seconds=resolve_namespace_ttl_seconds(policy=policy, namespace=request.namespace),
        tag=resolve_write_cache_tag(
            CacheWriteTagRequestV1(
                policy=policy,
                workspace=str(resolved_root),
                language=request.language,
                namespace=request.namespace,
                scope_hash=scope_hash,
                snapshot=snapshot.digest,
                run_id=request.run_id,
            )
        ),
    )


def build_query_fragment_entries(
    context: QueryFragmentCacheContext,
    *,
    extras_builder: Callable[[Path, str], Mapping[str, object] | None] | None = None,
) -> list[FragmentEntryV1]:
    """Build fragment entry descriptors for scoped files.

    Returns:
        list[FragmentEntryV1]: One cache entry descriptor per scoped file.
    """
    entries: list[FragmentEntryV1] = []
    for file_path in context.files:
        rel_path = normalize_repo_relative_path(str(file_path), root=context.root)
        content_hash = file_content_hash(file_path).digest
        extras: dict[str, object] = {"file_content_hash": content_hash}
        if extras_builder is not None:
            built = extras_builder(file_path, rel_path)
            if built is not None:
                extras.update(built)
        entries.append(
            FragmentEntryV1(
                file=rel_path,
                content_hash=content_hash,
                cache_key=build_cache_key(
                    context.namespace,
                    version="v1",
                    workspace=str(context.root),
                    language=context.language,
                    target=rel_path,
                    extras=extras,
                ),
            )
        )
    return entries


def run_query_fragment_scan[TMissPayload](
    *,
    context: QueryFragmentCacheContext,
    entries: list[FragmentEntryV1],
    run_id: str | None,
    decode: Callable[[object], object | None],
    scan_misses: Callable[[list[FragmentMissV1]], tuple[TMissPayload, list[FragmentWriteV1]]],
) -> FragmentScanResult[TMissPayload]:
    """Execute one query fragment cache probe->miss-scan->persist cycle.

    Returns:
        FragmentScanResult[TMissPayload]: Hit/miss partition and optional miss payload.
    """
    return run_fragment_scan(
        request=_build_fragment_request(context=context, run_id=run_id),
        entries=entries,
        probe_runtime=FragmentProbeRuntimeV1(
            cache_get=context.cache.get,
            decode=decode,
            cache_enabled=context.cache_enabled,
            record_get=record_cache_get,
            record_decode_failure=record_cache_decode_failure,
        ),
        persist_runtime=FragmentPersistRuntimeV1(
            cache_set=lambda key, value, *, expire=None, tag=None: context.cache.set(
                key,
                value,
                expire=expire,
                tag=tag,
            ),
            cache_set_many=lambda rows, *, expire=None, tag=None: context.cache.set_many(
                rows,
                expire=expire,
                tag=tag,
            ),
            encode=contract_to_builtins,
            cache_enabled=context.cache_enabled,
            transact=context.cache.transact,
            record_set=record_cache_set,
        ),
        scan_misses=scan_misses,
    )


def _build_fragment_request(
    *, context: QueryFragmentCacheContext, run_id: str | None
) -> FragmentRequestV1:
    return FragmentRequestV1(
        namespace=context.namespace,
        workspace=str(context.root),
        language=context.language,
        ttl_seconds=context.ttl_seconds,
        tag=context.tag,
        run_id=run_id,
    )


__all__ = [
    "CachedScanRequest",
    "QueryFragmentCacheContext",
    "QueryFragmentContextBuildRequest",
    "build_query_fragment_cache_context",
    "build_query_fragment_entries",
    "cached_scan",
    "run_query_fragment_scan",
]
