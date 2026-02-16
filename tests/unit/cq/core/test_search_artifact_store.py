"""Tests for test_search_artifact_store."""

from __future__ import annotations

import os
from pathlib import Path

from tools.cq.core.cache import close_cq_cache_backend, get_cq_cache_backend
from tools.cq.core.cache.contracts import SearchArtifactBundleV1
from tools.cq.core.cache.search_artifact_store import (
    list_search_artifact_entries,
    load_search_artifact_bundle,
    persist_search_artifact_bundle,
)


def _sample_bundle(run_id: str) -> SearchArtifactBundleV1:
    return SearchArtifactBundleV1(
        run_id=run_id,
        query="stable_id",
        macro="search",
        summary={"query": "stable_id"},
        diagnostics={"timed_out": False},
        created_ms=10.0,
    )


def test_search_artifact_store_roundtrip(tmp_path: Path) -> None:
    """Persist and load a tiny search artifact bundle end-to-end."""
    close_cq_cache_backend()
    os.environ["CQ_CACHE_ENABLED"] = "1"
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")

    bundle = _sample_bundle("run-1")
    entry = persist_search_artifact_bundle(
        root=tmp_path,
        bundle=bundle,
        tag="ns:search_artifacts|lang:auto",
        key_extras={"scope_hash": "abc"},
    )
    assert entry is not None

    backend = get_cq_cache_backend(root=tmp_path)
    raw = backend.get(entry.cache_key)
    assert isinstance(raw, (bytes, bytearray, memoryview))

    listed = list_search_artifact_entries(root=tmp_path, run_id="run-1", limit=10)
    assert listed
    assert listed[0].cache_key == entry.cache_key

    loaded, loaded_entry = load_search_artifact_bundle(root=tmp_path, run_id="run-1")
    assert loaded_entry is not None
    assert loaded is not None
    assert loaded.query == "stable_id"

    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)
    os.environ.pop("CQ_CACHE_DIR", None)


def test_search_artifact_store_large_bundle_roundtrip(tmp_path: Path) -> None:
    """Persist and load a large artifact payload through blob-backed cache path."""
    close_cq_cache_backend()
    os.environ["CQ_CACHE_ENABLED"] = "1"
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")

    bundle = SearchArtifactBundleV1(
        run_id="run-large",
        query="stable_id",
        macro="search",
        summary={"query": "stable_id", "blob": "x" * (128 * 1024)},
        diagnostics={"timed_out": False},
        created_ms=11.0,
    )
    entry = persist_search_artifact_bundle(
        root=tmp_path,
        bundle=bundle,
        tag="ns:search_artifacts|lang:auto",
        key_extras={"scope_hash": "xyz"},
    )
    assert entry is not None

    loaded, loaded_entry = load_search_artifact_bundle(root=tmp_path, run_id="run-large")
    assert loaded_entry is not None
    assert loaded is not None
    assert loaded.summary.get("blob") == "x" * (128 * 1024)

    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)
    os.environ.pop("CQ_CACHE_DIR", None)
