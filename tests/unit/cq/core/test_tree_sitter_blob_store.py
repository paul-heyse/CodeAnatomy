from __future__ import annotations

import os
from pathlib import Path

from tools.cq.core.cache import close_cq_cache_backend
from tools.cq.core.cache.tree_sitter_blob_store import (
    decode_blob_pointer,
    drop_tag_index,
    encode_blob_pointer,
    read_blob,
    write_blob,
)


def test_tree_sitter_blob_store_roundtrip(tmp_path: Path) -> None:
    close_cq_cache_backend()
    os.environ["CQ_CACHE_ENABLED"] = "1"
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")

    payload = b"x" * (128 * 1024)
    ref = write_blob(root=tmp_path, payload=payload)
    assert ref.blob_id
    assert ref.storage_key.startswith("cq:tree_sitter_blob:v1:")
    assert ref.size_bytes == len(payload)

    pointer = encode_blob_pointer(ref)
    decoded = decode_blob_pointer(pointer)
    assert decoded is not None
    assert decoded.storage_key == ref.storage_key
    assert read_blob(root=tmp_path, ref=decoded) == payload

    drop_tag_index(root=tmp_path)

    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)
    os.environ.pop("CQ_CACHE_DIR", None)
