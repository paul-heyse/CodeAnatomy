"""Tests for test_blob_store."""

from __future__ import annotations

import os
from pathlib import Path

from tools.cq.core.cache.blob_store import (
    decode_blob_pointer,
    drop_tag_index,
    encode_blob_pointer,
    read_blob,
    write_blob,
)
from tools.cq.core.cache.diskcache_backend import close_cq_cache_backend, get_cq_cache_backend


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
