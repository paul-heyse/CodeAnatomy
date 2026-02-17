"""File-backed blob store for large tree-sitter cache payloads."""

from __future__ import annotations

from hashlib import blake2b
from pathlib import Path

from tools.cq.core.cache.base_contracts import TreeSitterBlobRefV1
from tools.cq.core.cache.interface import CqCacheBackend, CqCacheStreamingBackend
from tools.cq.core.cache.namespaces import resolve_namespace_ttl_seconds
from tools.cq.core.cache.policy import default_cache_policy

_BLOB_DIR = "tree_sitter_blobs"
_BLOB_NAMESPACE = "tree_sitter"
_BLOB_TAG = "ns:tree_sitter|kind:blob"


def _blob_key(blob_id: str) -> str:
    return f"cq:tree_sitter_blob:v1:{blob_id}"


def _blob_root(root: Path) -> Path:
    return root / _BLOB_DIR


def _blob_path(root: Path, blob_id: str) -> Path:
    return _blob_root(root) / f"{blob_id}.bin"


def drop_tag_index(*, root: Path) -> None:
    """Best-effort tag-index teardown.

    The cache protocol intentionally does not expose backend-native tag-index
    operations, so this is now a no-op boundary hook.
    """
    _ = root


def _persist_to_cache(
    *,
    root: Path,
    backend: CqCacheBackend,
    storage_key: str,
    payload: bytes,
) -> bool:
    policy = default_cache_policy(root=root)
    ttl_seconds = resolve_namespace_ttl_seconds(policy=policy, namespace=_BLOB_NAMESPACE)

    if isinstance(backend, CqCacheStreamingBackend):
        ok = backend.set_streaming(
            storage_key,
            payload,
            expire=ttl_seconds,
            tag=_BLOB_TAG,
        )
        if ok:
            return True

    return backend.set(
        storage_key,
        payload,
        expire=ttl_seconds,
        tag=_BLOB_TAG,
    )


def _read_from_cache(*, backend: CqCacheBackend, storage_key: str) -> bytes | None:
    if isinstance(backend, CqCacheStreamingBackend):
        payload = backend.read_streaming(storage_key)
        if payload is not None:
            return payload

    value = backend.get(storage_key)
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    return None


def write_blob(*, root: Path, backend: CqCacheBackend, payload: bytes) -> TreeSitterBlobRefV1:
    """Persist blob payload and return stable blob reference.

    Returns:
        TreeSitterBlobRefV1: Stored blob reference metadata.
    """
    blob_id = blake2b(payload, digest_size=16).hexdigest()
    storage_key = _blob_key(blob_id)
    if _persist_to_cache(root=root, backend=backend, storage_key=storage_key, payload=payload):
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


def read_blob(*, root: Path, backend: CqCacheBackend, ref: TreeSitterBlobRefV1) -> bytes | None:
    """Load blob bytes from a stored blob reference.

    Returns:
        bytes | None: Blob payload when available.
    """
    payload = _read_from_cache(backend=backend, storage_key=ref.storage_key)
    if payload is not None:
        return payload
    if ref.path is None:
        return None
    path = Path(ref.path)
    if not path.is_absolute():
        path = root / path
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
        TreeSitterBlobRefV1 | None: Decoded blob pointer when recognized.
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
