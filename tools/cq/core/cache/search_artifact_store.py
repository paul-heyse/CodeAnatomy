"""Deterministic msgpack-backed search artifact storage on diskcache primitives."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import AbstractContextManager, ExitStack, nullcontext
from pathlib import Path
from typing import Final, Protocol, cast

import msgspec

from tools.cq.core.cache.contracts import SearchArtifactBundleV1, SearchArtifactIndexEntryV1
from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend
from tools.cq.core.cache.key_builder import build_cache_key
from tools.cq.core.cache.namespaces import resolve_namespace_ttl_seconds
from tools.cq.core.cache.policy import CqCachePolicyV1, default_cache_policy
from tools.cq.core.cache.telemetry import (
    record_cache_decode_failure,
    record_cache_get,
    record_cache_set,
)
from tools.cq.core.cache.tree_sitter_blob_store import (
    decode_blob_pointer,
    encode_blob_pointer,
    read_blob,
    write_blob,
)

try:
    from diskcache import Deque, Index
except ImportError:  # pragma: no cover - optional dependency
    Deque = None
    Index = None


class _SearchArtifactIndexLike(Protocol):
    def __setitem__(self, key: str, value: object) -> None: ...
    def get(self, key: str, default: object | None = None) -> object | None: ...
    def values(self) -> Iterable[object]: ...


class _SearchArtifactDequeLike(Protocol):
    def appendleft(self, item: str) -> None: ...
    def __iter__(self) -> Iterator[str]: ...


_NAMESPACE: Final[str] = "search_artifacts"
_VERSION: Final[str] = "v2"
_MAX_INDEX_ROWS: Final[int] = 1000
_BLOB_THRESHOLD_BYTES: Final[int] = 64 * 1024

_ENCODER = msgspec.msgpack.Encoder()
_DECODER = msgspec.msgpack.Decoder(type=SearchArtifactBundleV1)


def _store_root(policy: CqCachePolicyV1) -> Path:
    return Path(policy.directory).expanduser() / "stores" / _NAMESPACE


def _global_order_path(policy: CqCachePolicyV1) -> Path:
    return _store_root(policy) / "deque" / "global_order"


def _global_index_path(policy: CqCachePolicyV1) -> Path:
    return _store_root(policy) / "index" / "global"


def _run_order_path(policy: CqCachePolicyV1, run_id: str) -> Path:
    return _store_root(policy) / "deque" / f"run_{run_id}"


def _run_index_path(policy: CqCachePolicyV1, run_id: str) -> Path:
    return _store_root(policy) / "index" / f"run_{run_id}"


def _open_deque(path: Path) -> _SearchArtifactDequeLike | None:
    if Deque is None:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    return cast("_SearchArtifactDequeLike", Deque(str(path), maxlen=_MAX_INDEX_ROWS))


def _open_index(path: Path) -> _SearchArtifactIndexLike | None:
    if Index is None:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    return cast("_SearchArtifactIndexLike", Index(str(path)))


def _bundle_key(
    *,
    workspace: str,
    run_id: str,
    query: str,
    macro: str,
    extras: dict[str, object] | None,
) -> str:
    identity: dict[str, object] = {
        "run_id": run_id,
        "query": query,
        "macro": macro,
    }
    if isinstance(extras, dict):
        identity.update({k: v for k, v in extras.items() if v is not None})
    return build_cache_key(
        _NAMESPACE,
        version=_VERSION,
        workspace=workspace,
        language="auto",
        target=f"run:{run_id}",
        extras=identity,
    )


def _decode_entry(value: object) -> SearchArtifactIndexEntryV1 | None:
    if isinstance(value, SearchArtifactIndexEntryV1):
        return value
    if isinstance(value, dict):
        try:
            return msgspec.convert(value, type=SearchArtifactIndexEntryV1)
        except (RuntimeError, TypeError, ValueError):
            return None
    return None


def _transaction_context(store: object | None) -> AbstractContextManager[object]:
    if store is None:
        return cast("AbstractContextManager[object]", nullcontext(None))
    transact = getattr(store, "transact", None)
    if not callable(transact):
        return cast("AbstractContextManager[object]", nullcontext(None))
    try:
        return cast("AbstractContextManager[object]", transact(retry=True))
    except TypeError:
        try:
            return cast("AbstractContextManager[object]", transact())
        except (RuntimeError, ValueError, OSError, AttributeError):
            return cast("AbstractContextManager[object]", nullcontext(None))
    except (RuntimeError, ValueError, OSError, AttributeError):
        return cast("AbstractContextManager[object]", nullcontext(None))


def _record_index_entry(policy: CqCachePolicyV1, entry: SearchArtifactIndexEntryV1) -> None:
    global_index = _open_index(_global_index_path(policy))
    run_index = _open_index(_run_index_path(policy, entry.run_id))
    global_order = _open_deque(_global_order_path(policy))
    run_order = _open_deque(_run_order_path(policy, entry.run_id))
    builtins_entry = msgspec.to_builtins(entry)
    with ExitStack() as stack:
        for store in (global_index, run_index, global_order, run_order):
            stack.enter_context(_transaction_context(store))
        if global_index is not None:
            global_index[entry.cache_key] = builtins_entry
        if run_index is not None:
            run_index[entry.cache_key] = builtins_entry
        if global_order is not None:
            global_order.appendleft(entry.cache_key)
        if run_order is not None:
            run_order.appendleft(entry.cache_key)


def persist_search_artifact_bundle(
    *,
    root: Path,
    bundle: SearchArtifactBundleV1,
    tag: str | None,
    key_extras: dict[str, object] | None = None,
) -> SearchArtifactIndexEntryV1 | None:
    """Persist one search artifact bundle as msgpack bytes and index it.

    Returns:
        SearchArtifactIndexEntryV1 | None: The index entry for the persisted
            bundle, or ``None`` if the write was not accepted.
    """
    policy = default_cache_policy(root=root)
    backend = get_cq_cache_backend(root=root)
    ttl_seconds = resolve_namespace_ttl_seconds(policy=policy, namespace=_NAMESPACE)
    run_id = bundle.run_id
    cache_key = _bundle_key(
        workspace=str(root.resolve()),
        run_id=run_id,
        query=bundle.query,
        macro=bundle.macro,
        extras=key_extras,
    )
    payload = _ENCODER.encode(bundle)
    stored_value: object
    if len(payload) > _BLOB_THRESHOLD_BYTES:
        blob_ref = write_blob(root=root, payload=payload)
        stored_value = encode_blob_pointer(blob_ref)
    else:
        stored_value = payload
    ok = backend.set(
        cache_key,
        stored_value,
        expire=ttl_seconds,
        tag=tag,
    )
    record_cache_set(namespace=_NAMESPACE, ok=ok, key=cache_key)
    if not ok:
        return None

    entry = SearchArtifactIndexEntryV1(
        run_id=run_id,
        cache_key=cache_key,
        query=bundle.query,
        macro=bundle.macro,
        created_ms=float(bundle.created_ms),
    )
    _record_index_entry(policy, entry)
    return entry


def list_search_artifact_entries(
    *,
    root: Path,
    run_id: str | None,
    limit: int,
) -> list[SearchArtifactIndexEntryV1]:
    """List indexed artifact entries, newest first.

    Returns:
        list[SearchArtifactIndexEntryV1]: Matching search artifact index entries,
            ordered by newest first.
    """
    bounded_limit = max(1, int(limit))
    policy = default_cache_policy(root=root)

    if run_id:
        return _list_run_entries(policy=policy, run_id=run_id, limit=bounded_limit)
    return _list_global_entries(policy=policy, limit=bounded_limit)


def _list_run_entries(
    *,
    policy: CqCachePolicyV1,
    run_id: str,
    limit: int,
) -> list[SearchArtifactIndexEntryV1]:
    run_index = _open_index(_run_index_path(policy, run_id))
    if run_index is None:
        return []
    rows = [entry for value in run_index.values() if (entry := _decode_entry(value)) is not None]
    rows.sort(key=lambda row: row.created_ms, reverse=True)
    return rows[:limit]


def _list_global_entries(
    *,
    policy: CqCachePolicyV1,
    limit: int,
) -> list[SearchArtifactIndexEntryV1]:
    global_order = _open_deque(_global_order_path(policy))
    global_index = _open_index(_global_index_path(policy))
    if global_order is None or global_index is None:
        return []

    rows = _rows_from_global_order(
        global_order=global_order, global_index=global_index, limit=limit
    )
    if rows:
        return rows
    return _rows_from_global_index(global_index=global_index, limit=limit)


def _rows_from_global_order(
    *,
    global_order: _SearchArtifactDequeLike,
    global_index: _SearchArtifactIndexLike,
    limit: int,
) -> list[SearchArtifactIndexEntryV1]:
    rows: list[SearchArtifactIndexEntryV1] = []
    seen: set[str] = set()
    for cache_key in global_order:
        if not isinstance(cache_key, str) or cache_key in seen:
            continue
        seen.add(cache_key)
        entry = _decode_entry(global_index.get(cache_key))
        if entry is None:
            continue
        rows.append(entry)
        if len(rows) >= limit:
            break
    return rows


def _rows_from_global_index(
    *,
    global_index: _SearchArtifactIndexLike,
    limit: int,
) -> list[SearchArtifactIndexEntryV1]:
    rows = [entry for value in global_index.values() if (entry := _decode_entry(value)) is not None]
    rows.sort(key=lambda row: row.created_ms, reverse=True)
    return rows[:limit]


def _touch_cached_entry(*, root: Path, cache_key: str) -> None:
    backend = get_cq_cache_backend(root=root)
    cache = getattr(backend, "cache", None)
    if cache is None:
        return
    policy = default_cache_policy(root=root)
    ttl_seconds = resolve_namespace_ttl_seconds(policy=policy, namespace=_NAMESPACE)
    try:
        cache.touch(cache_key, expire=ttl_seconds, retry=True)
    except (RuntimeError, TypeError, ValueError, OSError):
        return


def _decode_bundle_payload(
    *,
    root: Path,
    payload: object,
) -> tuple[SearchArtifactBundleV1 | None, bool]:
    bundle: SearchArtifactBundleV1 | None = None
    attempted = False
    if isinstance(payload, (bytes, bytearray, memoryview)):
        attempted = True
        try:
            bundle = _DECODER.decode(payload)
        except (RuntimeError, TypeError, ValueError):
            bundle = None
    elif isinstance(payload, dict):
        attempted = True
        blob_ref = decode_blob_pointer(payload)
        if blob_ref is not None:
            blob_payload = read_blob(root=root, ref=blob_ref)
            if isinstance(blob_payload, (bytes, bytearray, memoryview)):
                try:
                    bundle = _DECODER.decode(blob_payload)
                except (RuntimeError, TypeError, ValueError):
                    bundle = None
            else:
                bundle = None
        else:
            try:
                bundle = msgspec.convert(payload, type=SearchArtifactBundleV1)
            except (RuntimeError, TypeError, ValueError):
                bundle = None
    return bundle, attempted


def load_search_artifact_bundle(
    *,
    root: Path,
    run_id: str,
) -> tuple[SearchArtifactBundleV1 | None, SearchArtifactIndexEntryV1 | None]:
    """Load newest search artifact bundle for ``run_id``.

    Returns:
        tuple[SearchArtifactBundleV1 | None, SearchArtifactIndexEntryV1 | None]:
            A tuple of ``(bundle, index_entry)`` where either value can be
            ``None`` when unavailable.
    """
    entries = list_search_artifact_entries(root=root, run_id=run_id, limit=1)
    if not entries:
        return None, None

    entry = entries[0]
    backend = get_cq_cache_backend(root=root)
    payload = backend.get(entry.cache_key)
    hit = isinstance(payload, (bytes, bytearray, memoryview, dict))
    record_cache_get(namespace=_NAMESPACE, hit=hit, key=entry.cache_key)
    bundle, attempted_decode = _decode_bundle_payload(root=root, payload=payload)
    if bundle is None:
        if attempted_decode:
            record_cache_decode_failure(namespace=_NAMESPACE)
        return None, entry
    _touch_cached_entry(root=root, cache_key=entry.cache_key)
    return bundle, entry


__all__ = [
    "list_search_artifact_entries",
    "load_search_artifact_bundle",
    "persist_search_artifact_bundle",
]
