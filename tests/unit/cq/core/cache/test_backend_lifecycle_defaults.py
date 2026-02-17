"""Tests for backend lifecycle process-default registry seams."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.cache.backend_lifecycle import (
    close_cq_cache_backend,
    get_cq_cache_backend,
    get_default_backend_registry,
    set_default_backend_registry,
)
from tools.cq.core.cache.backend_registry import BackendRegistry
from tools.cq.core.cache.interface import NoopCacheBackend


def test_set_default_backend_registry_replaces_process_registry(tmp_path: Path) -> None:
    """Injected default registry should be used by backend lifecycle accessors."""
    original = get_default_backend_registry()
    injected = BackendRegistry()
    workspace = str(tmp_path.resolve())
    backend = NoopCacheBackend()
    try:
        set_default_backend_registry(injected)
        injected.set(workspace, backend)
        assert get_cq_cache_backend(root=tmp_path) is backend
    finally:
        close_cq_cache_backend(root=tmp_path)
        set_default_backend_registry(original)
