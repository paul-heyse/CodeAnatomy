"""Tests for cache backend lifecycle management."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.cache.backend_lifecycle import (
    close_cq_cache_backend,
    get_cq_cache_backend,
    set_cq_cache_backend,
)
from tools.cq.core.cache.interface import NoopCacheBackend


class _DummyBackend(NoopCacheBackend):
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_set_get_close_backend_for_workspace(tmp_path: Path) -> None:
    """Verify backend registration, lookup, and close for one workspace."""
    workspace = tmp_path / "repo"
    workspace.mkdir(parents=True, exist_ok=True)
    close_cq_cache_backend(root=workspace)

    backend = _DummyBackend()
    set_cq_cache_backend(root=workspace, backend=backend)

    resolved = get_cq_cache_backend(root=workspace)
    assert resolved is backend

    close_cq_cache_backend(root=workspace)
    assert backend.closed is True


def test_workspace_backend_isolation(tmp_path: Path) -> None:
    """Verify backend lifecycle operations stay isolated per workspace root."""
    workspace_a = tmp_path / "a"
    workspace_b = tmp_path / "b"
    workspace_a.mkdir(parents=True, exist_ok=True)
    workspace_b.mkdir(parents=True, exist_ok=True)
    close_cq_cache_backend()

    backend_a = _DummyBackend()
    backend_b = _DummyBackend()
    set_cq_cache_backend(root=workspace_a, backend=backend_a)
    set_cq_cache_backend(root=workspace_b, backend=backend_b)

    assert get_cq_cache_backend(root=workspace_a) is backend_a
    assert get_cq_cache_backend(root=workspace_b) is backend_b

    close_cq_cache_backend(root=workspace_a)
    assert backend_a.closed is True
    assert backend_b.closed is False

    close_cq_cache_backend()
