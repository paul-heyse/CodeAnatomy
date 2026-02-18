"""Tests for test_blob_store."""

from __future__ import annotations

import os
from pathlib import Path

from tools.cq.core.cache.backend_core import close_cq_cache_backend, get_cq_cache_backend
from tools.cq.core.cache.blob_store import (
    decode_blob_pointer,
    drop_tag_index,
    encode_blob_pointer,
    read_blob,
    write_blob,
)
from tools.cq.core.cache.interface import NoopCacheBackend


class _StreamingBackend(NoopCacheBackend):
    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}
        self.set_calls = 0
        self.get_calls = 0
        self.set_streaming_calls = 0
        self.read_streaming_calls = 0

    def get(self, key: str) -> object | None:
        self.get_calls += 1
        return self.store.get(key)

    def set(
        self,
        key: str,
        value: object,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool:
        _ = (expire, tag)
        self.set_calls += 1
        if isinstance(value, (bytes, bytearray, memoryview)):
            self.store[key] = bytes(value)
            return True
        return False

    def set_streaming(
        self,
        key: str,
        payload: bytes,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool:
        _ = (expire, tag)
        self.set_streaming_calls += 1
        self.store[key] = payload
        return True

    def read_streaming(self, key: str) -> bytes | None:
        self.read_streaming_calls += 1
        return self.store.get(key)


class _PlainBackend(NoopCacheBackend):
    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}
        self.set_calls = 0
        self.get_calls = 0

    def get(self, key: str) -> object | None:
        self.get_calls += 1
        return self.store.get(key)

    def set(
        self,
        key: str,
        value: object,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool:
        _ = (expire, tag)
        self.set_calls += 1
        if isinstance(value, (bytes, bytearray, memoryview)):
            self.store[key] = bytes(value)
            return True
        return False


def test_blob_store_roundtrip(tmp_path: Path) -> None:
    """Store and retrieve binary payloads via pointer-based tree-sitter blob API."""
    close_cq_cache_backend()
    os.environ["CQ_CACHE_ENABLED"] = "1"
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")

    payload = b"x" * (128 * 1024)
    backend = get_cq_cache_backend(root=tmp_path)
    ref = write_blob(root=tmp_path, backend=backend, payload=payload)
    assert ref.blob_id
    assert ref.storage_key.startswith("cq:tree_sitter_blob:v1:")
    assert ref.size_bytes == len(payload)

    pointer = encode_blob_pointer(ref)
    decoded = decode_blob_pointer(pointer)
    assert decoded is not None
    assert decoded.storage_key == ref.storage_key
    assert read_blob(root=tmp_path, backend=backend, ref=decoded) == payload

    drop_tag_index(root=tmp_path)

    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)
    os.environ.pop("CQ_CACHE_DIR", None)


def test_blob_store_prefers_streaming_capability(tmp_path: Path) -> None:
    """Use streaming methods directly when backend exposes streaming capability."""
    payload = b"stream-payload"
    backend = _StreamingBackend()

    ref = write_blob(root=tmp_path, backend=backend, payload=payload)
    assert ref.path is None
    assert backend.set_streaming_calls == 1
    assert backend.set_calls == 0
    assert read_blob(root=tmp_path, backend=backend, ref=ref) == payload
    assert backend.read_streaming_calls == 1


def test_blob_store_falls_back_to_standard_get_set_when_not_streaming(tmp_path: Path) -> None:
    """Fallback to plain cache operations for non-streaming backends."""
    payload = b"plain-payload"
    backend = _PlainBackend()

    ref = write_blob(root=tmp_path, backend=backend, payload=payload)
    assert ref.path is None
    assert backend.set_calls == 1
    assert read_blob(root=tmp_path, backend=backend, ref=ref) == payload
    assert backend.get_calls == 1
