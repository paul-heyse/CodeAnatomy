"""Tests for test_tree_sitter_cache_store."""

from __future__ import annotations

import os
from pathlib import Path

from tools.cq.core.cache import close_cq_cache_backend
from tools.cq.core.cache.base_contracts import TreeSitterCacheEnvelopeV1
from tools.cq.core.cache.tree_sitter_cache_store import (
    build_tree_sitter_cache_key,
    load_tree_sitter_payload,
    persist_tree_sitter_payload,
)


def test_tree_sitter_cache_store_roundtrip(tmp_path: Path) -> None:
    """Persist and load a small tree-sitter cache envelope from disk."""
    close_cq_cache_backend()
    os.environ["CQ_CACHE_ENABLED"] = "1"
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")

    cache_key = build_tree_sitter_cache_key(
        workspace=str(tmp_path),
        language="python",
        target="src/sample.py",
        fingerprints={
            "file_hash": "f" * 24,
            "grammar_hash": "g" * 24,
            "query_pack_hash": "q" * 24,
            "scope_hash": "s" * 24,
        },
    )
    envelope = TreeSitterCacheEnvelopeV1(
        language="python",
        file_hash="f" * 24,
        grammar_hash="g" * 24,
        query_pack_hash="q" * 24,
        scope_hash="s" * 24,
        payload={"enrichment_status": "applied"},
    )

    assert persist_tree_sitter_payload(
        root=tmp_path,
        cache_key=cache_key,
        envelope=envelope,
        tag="ns:tree_sitter|lang:python",
    )
    loaded = load_tree_sitter_payload(root=tmp_path, cache_key=cache_key)
    assert loaded is not None
    assert loaded.payload.get("enrichment_status") == "applied"

    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)
    os.environ.pop("CQ_CACHE_DIR", None)



def test_tree_sitter_cache_store_uses_blob_pointer_for_large_payload(tmp_path: Path) -> None:
    """Store large payloads via blob pointer and read back identical content."""
    close_cq_cache_backend()
    os.environ["CQ_CACHE_ENABLED"] = "1"
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")

    cache_key = build_tree_sitter_cache_key(
        workspace=str(tmp_path),
        language="python",
        target="src/large.py",
        fingerprints={
            "file_hash": "f" * 24,
            "grammar_hash": "g" * 24,
            "query_pack_hash": "q" * 24,
            "scope_hash": "s" * 24,
        },
    )
    envelope = TreeSitterCacheEnvelopeV1(
        language="python",
        file_hash="f" * 24,
        grammar_hash="g" * 24,
        query_pack_hash="q" * 24,
        scope_hash="s" * 24,
        payload={"blob": "x" * (128 * 1024)},
    )
    assert persist_tree_sitter_payload(
        root=tmp_path,
        cache_key=cache_key,
        envelope=envelope,
        tag="ns:tree_sitter|lang:python",
    )
    loaded = load_tree_sitter_payload(root=tmp_path, cache_key=cache_key)
    assert loaded is not None
    assert loaded.payload.get("blob") == "x" * (128 * 1024)

    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)
    os.environ.pop("CQ_CACHE_DIR", None)
