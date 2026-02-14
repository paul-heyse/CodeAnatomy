"""Scope snapshot fingerprints for no-preindex cache invalidation."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated

import msgspec

from tools.cq.core.cache.contracts import (
    ScopeFileStatCacheV1,
    ScopeSnapshotCacheV1,
)
from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend
from tools.cq.core.cache.key_builder import (
    build_cache_key,
    build_namespace_cache_tag,
    build_scope_hash,
)
from tools.cq.core.cache.namespaces import (
    is_namespace_cache_enabled,
    resolve_namespace_ttl_seconds,
)
from tools.cq.core.cache.policy import default_cache_policy
from tools.cq.core.cache.telemetry import (
    record_cache_decode_failure,
    record_cache_get,
    record_cache_set,
)
from tools.cq.core.contracts import contract_to_builtins
from tools.cq.core.structs import CqCacheStruct

NonNegativeInt = Annotated[int, msgspec.Meta(ge=0)]


class ScopeFileStatV1(CqCacheStruct, frozen=True):
    """Stable stat tuple for one scope file."""

    path: str
    size_bytes: NonNegativeInt = 0
    mtime_ns: NonNegativeInt = 0


class ScopeSnapshotFingerprintV1(CqCacheStruct, frozen=True):
    """Deterministic scope snapshot fingerprint payload."""

    language: str
    scope_globs: tuple[str, ...] = ()
    files: tuple[ScopeFileStatV1, ...] = ()
    digest: str = ""

    @property
    def file_count(self) -> int:
        """Return number of files included in this snapshot."""
        return len(self.files)


def _safe_rel_path(*, root: Path, file_path: Path) -> str:
    try:
        return file_path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return file_path.resolve().as_posix()


def _file_stat(*, root: Path, file_path: Path) -> ScopeFileStatV1:
    rel_path = _safe_rel_path(root=root, file_path=file_path)
    try:
        stat = file_path.stat()
        return ScopeFileStatV1(
            path=rel_path,
            size_bytes=max(0, int(stat.st_size)),
            mtime_ns=max(0, int(stat.st_mtime_ns)),
        )
    except (OSError, RuntimeError, ValueError):
        return ScopeFileStatV1(path=rel_path, size_bytes=0, mtime_ns=0)


def _normalize_scope_roots(*, root: Path, scope_roots: list[Path] | None) -> tuple[str, ...]:
    if not scope_roots:
        return (str(root.resolve()),)
    normalized: list[str] = []
    for scope_root in scope_roots:
        candidate = scope_root if scope_root.is_absolute() else root / scope_root
        normalized.append(str(candidate.resolve()))
    return tuple(sorted(set(normalized)))


def _normalize_inventory_token(
    inventory_token: Mapping[str, object] | object | None,
) -> dict[str, object]:
    if isinstance(inventory_token, Mapping):
        return {str(key): value for key, value in inventory_token.items()}
    if isinstance(inventory_token, tuple) and len(inventory_token) == 2:
        return {
            "root_mtime_ns": int(inventory_token[0]) if isinstance(inventory_token[0], int) else 0,
            "git_index_mtime_ns": int(inventory_token[1])
            if isinstance(inventory_token[1], int)
            else 0,
        }
    return {}


def build_scope_snapshot_fingerprint(
    *,
    root: Path,
    files: list[Path],
    language: str,
    scope_globs: list[str] | None = None,
    scope_roots: list[Path] | None = None,
    inventory_token: Mapping[str, object] | object | None = None,
) -> ScopeSnapshotFingerprintV1:
    """Build deterministic scope fingerprint from file metadata.

    Returns:
        Snapshot payload containing sorted file stats and digest.
    """
    namespace = "scope_snapshot"
    resolved_root = root.resolve()
    normalized_scope_roots = _normalize_scope_roots(root=resolved_root, scope_roots=scope_roots)
    normalized_inventory = _normalize_inventory_token(inventory_token)
    scope_hash = build_scope_hash(
        {
            "scope_roots": normalized_scope_roots,
            "scope_globs": tuple(scope_globs or ()),
            "inventory_token": normalized_inventory,
        }
    )
    stats = sorted(
        (_file_stat(root=resolved_root, file_path=path) for path in files),
        key=lambda item: item.path,
    )
    file_signature = tuple((item.path, item.size_bytes, item.mtime_ns) for item in stats)
    policy = default_cache_policy(root=resolved_root)
    cache_enabled = is_namespace_cache_enabled(policy=policy, namespace=namespace)
    cache = get_cq_cache_backend(root=resolved_root)
    cache_key = build_cache_key(
        namespace,
        version="v1",
        workspace=str(resolved_root),
        language=language,
        target=scope_hash or language,
        extras={
            "scope_roots": normalized_scope_roots,
            "scope_globs": tuple(scope_globs or ()),
            "inventory_token": normalized_inventory,
            "file_signature": file_signature,
        },
    )
    if cache_enabled:
        cached = cache.get(cache_key)
        record_cache_get(namespace=namespace, hit=isinstance(cached, dict), key=cache_key)
        if isinstance(cached, dict):
            try:
                payload = msgspec.convert(cached, type=ScopeSnapshotCacheV1)
                return ScopeSnapshotFingerprintV1(
                    language=payload.language,
                    scope_globs=tuple(payload.scope_globs),
                    files=tuple(
                        ScopeFileStatV1(
                            path=item.path,
                            size_bytes=item.size_bytes,
                            mtime_ns=item.mtime_ns,
                        )
                        for item in payload.files
                    ),
                    digest=payload.digest,
                )
            except (RuntimeError, TypeError, ValueError):
                record_cache_decode_failure(namespace=namespace)

    payload = {
        "language": language,
        "scope_globs": tuple(scope_globs or ()),
        "scope_roots": normalized_scope_roots,
        "inventory_token": normalized_inventory,
        "files": file_signature,
    }
    digest = hashlib.sha256(msgspec.json.encode(payload)).hexdigest()
    snapshot = ScopeSnapshotFingerprintV1(
        language=language,
        scope_globs=tuple(scope_globs or ()),
        files=tuple(stats),
        digest=digest,
    )
    if cache_enabled:
        ttl_seconds = resolve_namespace_ttl_seconds(policy=policy, namespace=namespace)
        cache_payload = ScopeSnapshotCacheV1(
            language=snapshot.language,
            scope_globs=snapshot.scope_globs,
            scope_roots=normalized_scope_roots,
            inventory_token=normalized_inventory,
            files=[
                ScopeFileStatCacheV1(
                    path=item.path,
                    size_bytes=item.size_bytes,
                    mtime_ns=item.mtime_ns,
                )
                for item in snapshot.files
            ],
            digest=snapshot.digest,
        )
        ok = cache.set(
            cache_key,
            contract_to_builtins(cache_payload),
            expire=ttl_seconds,
            tag=build_namespace_cache_tag(
                workspace=str(resolved_root),
                language=language,
                namespace=namespace,
                scope_hash=scope_hash,
                snapshot=digest,
            ),
        )
        record_cache_set(namespace=namespace, ok=ok, key=cache_key)
    return snapshot


__all__ = [
    "ScopeFileStatV1",
    "ScopeSnapshotFingerprintV1",
    "build_scope_snapshot_fingerprint",
]
