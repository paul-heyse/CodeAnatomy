"""File-backed blob store for large tree-sitter cache payloads."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from hashlib import blake2b
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import cast

from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend
from tools.cq.core.cache.namespaces import resolve_namespace_ttl_seconds
from tools.cq.core.cache.policy import default_cache_policy
from tools.cq.core.cache.tree_sitter_blob_store_contracts import TreeSitterBlobRefV1

_BLOB_DIR = "tree_sitter_blobs"
_BLOB_NAMESPACE = "tree_sitter"
_BLOB_TAG = "ns:tree_sitter|kind:blob"


def _read_bytes_via_context(
    read_fn: Callable[..., object],
    storage_key: str,
    *,
    with_retry: bool,
) -> bytes | None:
    if with_retry:
        reader_cm = cast(
            "AbstractContextManager[object]",
            read_fn(storage_key, retry=True),
        )
    else:
        reader_cm = cast("AbstractContextManager[object]", read_fn(storage_key))
    with reader_cm as reader:
        read_method = getattr(reader, "read", None)
        if not callable(read_method):
            return None
        payload = read_method()
        if isinstance(payload, memoryview):
            return payload.tobytes()
        if isinstance(payload, (bytes, bytearray)):
            return bytes(payload)
    return None


def _blob_key(blob_id: str) -> str:
    return f"cq:tree_sitter_blob:v1:{blob_id}"


def _blob_root(root: Path) -> Path:
    return root / _BLOB_DIR


def _blob_path(root: Path, blob_id: str) -> Path:
    return _blob_root(root) / f"{blob_id}.bin"


def _cache_object(root: Path) -> object | None:
    backend = get_cq_cache_backend(root=root)
    return getattr(backend, "cache", None)


def _ensure_tag_index(cache: object) -> None:
    create_tag_index = getattr(cache, "create_tag_index", None)
    if not callable(create_tag_index):
        return
    try:
        create_tag_index()
    except (RuntimeError, TypeError, ValueError, OSError, AttributeError):
        return


def drop_tag_index(*, root: Path) -> None:
    """Best-effort drop the blob-cache tag index."""
    cache = _cache_object(root)
    if cache is None:
        return
    drop_fn = getattr(cache, "drop_tag_index", None)
    if not callable(drop_fn):
        return
    try:
        drop_fn()
    except (RuntimeError, TypeError, ValueError, OSError, AttributeError):
        return


def _persist_to_cache(*, root: Path, storage_key: str, payload: bytes) -> bool:
    cache = _cache_object(root)
    if cache is None:
        return False
    set_fn = getattr(cache, "set", None)
    if not callable(set_fn):
        return False
    _ensure_tag_index(cache)
    policy = default_cache_policy(root=root)
    ttl_seconds = resolve_namespace_ttl_seconds(policy=policy, namespace=_BLOB_NAMESPACE)
    with NamedTemporaryFile("w+b", delete=True) as tmp:
        tmp.write(payload)
        tmp.flush()
        tmp.seek(0)
        try:
            return bool(
                set_fn(
                    storage_key,
                    tmp,
                    read=True,
                    expire=ttl_seconds,
                    tag=_BLOB_TAG,
                    retry=True,
                )
            )
        except TypeError:
            tmp.seek(0)
            return bool(
                set_fn(
                    storage_key,
                    tmp,
                    read=True,
                    expire=ttl_seconds,
                    tag=_BLOB_TAG,
                )
            )
        except (RuntimeError, ValueError, OSError, AttributeError):
            return False


def _read_from_cache(*, root: Path, storage_key: str) -> bytes | None:
    cache = _cache_object(root)
    if cache is None:
        return None
    read_fn = getattr(cache, "read", None)
    if callable(read_fn):
        try:
            payload = _read_bytes_via_context(read_fn, storage_key, with_retry=True)
            if payload is not None:
                return payload
        except TypeError:
            try:
                payload = _read_bytes_via_context(read_fn, storage_key, with_retry=False)
                if payload is not None:
                    return payload
            except (RuntimeError, ValueError, OSError, AttributeError):
                pass
        except (RuntimeError, ValueError, OSError, AttributeError):
            pass
    backend = get_cq_cache_backend(root=root)
    value = backend.get(storage_key)
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    return None


def write_blob(*, root: Path, payload: bytes) -> TreeSitterBlobRefV1:
    """Persist blob payload and return stable blob reference.

    Returns:
        TreeSitterBlobRefV1: Persisted blob reference metadata.
    """
    blob_id = blake2b(payload, digest_size=16).hexdigest()
    storage_key = _blob_key(blob_id)
    if _persist_to_cache(root=root, storage_key=storage_key, payload=payload):
        return TreeSitterBlobRefV1(
            blob_id=blob_id,
            storage_key=storage_key,
            size_bytes=len(payload),
            path=None,
        )

    path = _blob_path(root, blob_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_bytes(payload)
    return TreeSitterBlobRefV1(
        blob_id=blob_id,
        storage_key=storage_key,
        size_bytes=len(payload),
        path=str(path),
    )


def read_blob(*, root: Path, ref: TreeSitterBlobRefV1) -> bytes | None:
    """Load blob bytes from a stored blob reference.

    Returns:
        bytes | None: Blob bytes when available.
    """
    payload = _read_from_cache(root=root, storage_key=ref.storage_key)
    if payload is not None:
        return payload
    if ref.path is None:
        return None
    path = Path(ref.path)
    if not path.exists():
        return None
    try:
        return path.read_bytes()
    except OSError:
        return None


def encode_blob_pointer(ref: TreeSitterBlobRefV1) -> dict[str, object]:
    """Encode blob reference as a cache-storable pointer payload.

    Returns:
        dict[str, object]: Serialized pointer payload.
    """
    return {
        "__tree_sitter_blob__": True,
        "blob_id": ref.blob_id,
        "storage_key": ref.storage_key,
        "size_bytes": ref.size_bytes,
        "path": ref.path,
    }


def decode_blob_pointer(value: object) -> TreeSitterBlobRefV1 | None:
    """Decode blob pointer payload from cache value when present.

    Returns:
        TreeSitterBlobRefV1 | None: Decoded blob reference when valid.
    """
    if not isinstance(value, dict):
        return None
    if value.get("__tree_sitter_blob__") is not True:
        return None
    blob_id = value.get("blob_id")
    storage_key = value.get("storage_key")
    path = value.get("path")
    size_bytes = value.get("size_bytes")
    if not isinstance(blob_id, str) or not isinstance(size_bytes, int):
        return None
    normalized_path = path if isinstance(path, str) else None
    if isinstance(storage_key, str):
        normalized_key = storage_key
    elif normalized_path is not None:
        normalized_key = _blob_key(blob_id)
    else:
        return None
    return TreeSitterBlobRefV1(
        blob_id=blob_id,
        storage_key=normalized_key,
        path=normalized_path,
        size_bytes=size_bytes,
    )


__all__ = [
    "decode_blob_pointer",
    "drop_tag_index",
    "encode_blob_pointer",
    "read_blob",
    "write_blob",
]
