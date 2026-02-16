"""Tests for test_run_lifecycle."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import pytest
from tools.cq.core.cache import close_cq_cache_backend, get_cq_cache_backend
from tools.cq.core.cache.key_builder import (
    build_namespace_cache_tag,
    build_run_cache_tag,
)
from tools.cq.core.cache.policy import CqCachePolicyV1
from tools.cq.core.cache.run_lifecycle import (
    CacheWriteTagRequestV1,
    maybe_evict_run_cache_tag,
    resolve_write_cache_tag,
)


@pytest.fixture(autouse=True)
def _close_cache_backends() -> Generator[None]:
    close_cq_cache_backend()
    yield
    close_cq_cache_backend()



def test_resolve_write_cache_tag_uses_run_tag_for_ephemeral_namespace() -> None:
    """Use run-id tag when namespace is marked ephemeral."""
    policy = CqCachePolicyV1(namespace_ephemeral={"search_candidates": True})
    tag = resolve_write_cache_tag(
        CacheWriteTagRequestV1(
            policy=policy,
            workspace="/repo",
            language="python",
            namespace="search_candidates",
            scope_hash="scope123",
            snapshot="snap123",
            run_id="run123",
        )
    )
    assert tag.startswith("ws:")
    assert "|lang:python|" in tag
    assert "|run:" in tag
    assert "ns:search_candidates" not in tag
    assert "scope:scope123" not in tag
    assert "snap:snap123" not in tag



def test_resolve_write_cache_tag_uses_namespace_tag_for_persistent_namespace() -> None:
    """Use namespace tag when namespace is configured as persistent."""
    policy = CqCachePolicyV1(namespace_ephemeral={"search_candidates": False})
    tag = resolve_write_cache_tag(
        CacheWriteTagRequestV1(
            policy=policy,
            workspace="/repo",
            language="python",
            namespace="search_candidates",
            scope_hash="scope123",
            snapshot="snap123",
            run_id="run123",
        )
    )
    assert tag.startswith("ws:")
    assert "|lang:python|" in tag
    assert "|ns:search_candidates|" in tag
    assert "|scope:" in tag
    assert "scope:scope123" not in tag
    assert "|snap:" in tag
    assert "snap:snap123" not in tag
    assert "|run:" not in tag


def test_maybe_evict_run_cache_tag_removes_only_ephemeral_entries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verify run-tag eviction does not remove persistent namespace cache entries."""
    monkeypatch.setenv("CQ_CACHE_ENABLED", "1")
    monkeypatch.setenv("CQ_CACHE_DIR", str(tmp_path / "cq_cache"))
    monkeypatch.setenv("CQ_CACHE_EVICT_RUN_TAG_ON_EXIT", "1")

    backend = get_cq_cache_backend(root=tmp_path)
    workspace = str(tmp_path.resolve())
    run_tag = build_run_cache_tag(workspace=workspace, language="python", run_id="run123")
    persistent_tag = build_namespace_cache_tag(
        workspace=workspace,
        language="python",
        namespace="search_candidates",
    )

    assert backend.set("run_key", {"v": 1}, expire=60, tag=run_tag) is True
    assert backend.set("persistent_key", {"v": 2}, expire=60, tag=persistent_tag) is True
    assert maybe_evict_run_cache_tag(root=tmp_path, language="python", run_id="run123") is True
    assert backend.get("run_key") is None
    assert backend.get("persistent_key") == {"v": 2}
