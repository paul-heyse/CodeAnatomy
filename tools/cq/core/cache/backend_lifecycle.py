"""Workspace-scoped CQ cache backend lifecycle management."""

from __future__ import annotations

import atexit
import threading
from pathlib import Path

from tools.cq.core.cache.backend_registry import BackendRegistry
from tools.cq.core.cache.interface import CqCacheBackend, NoopCacheBackend
from tools.cq.core.cache.policy import default_cache_policy

_BACKEND_LOCK = threading.Lock()
_DEFAULT_BACKEND_REGISTRY_LOCK = threading.Lock()
_DEFAULT_BACKEND_REGISTRY_STATE: dict[str, BackendRegistry | None] = {"registry": None}


def get_default_backend_registry() -> BackendRegistry:
    """Return process-default workspace backend registry."""
    with _DEFAULT_BACKEND_REGISTRY_LOCK:
        registry = _DEFAULT_BACKEND_REGISTRY_STATE["registry"]
        if registry is None:
            registry = BackendRegistry()
            _DEFAULT_BACKEND_REGISTRY_STATE["registry"] = registry
        return registry


def set_default_backend_registry(registry: BackendRegistry | None) -> None:
    """Set or clear process-default backend registry."""
    with _DEFAULT_BACKEND_REGISTRY_LOCK:
        _DEFAULT_BACKEND_REGISTRY_STATE["registry"] = registry


def _close_backends(backends: list[CqCacheBackend]) -> None:
    for backend in backends:
        backend.close()


def _collect_stale_backends_locked(registry: BackendRegistry) -> list[CqCacheBackend]:
    stale: list[CqCacheBackend] = []
    for workspace, backend in registry.items():
        if not Path(workspace).exists():
            stale.append(backend)
            registry.pop(workspace)
    return stale


def _build_workspace_backend(*, root: Path) -> CqCacheBackend:
    policy = default_cache_policy(root=root)
    if not policy.enabled:
        return NoopCacheBackend()
    from tools.cq.core.cache.diskcache_backend import _build_diskcache_backend

    return _build_diskcache_backend(policy)


def get_cq_cache_backend(*, root: Path) -> CqCacheBackend:
    """Return workspace-keyed CQ cache backend."""
    workspace = str(root.resolve())
    stale: list[CqCacheBackend]
    with _BACKEND_LOCK:
        registry = get_default_backend_registry()
        stale = _collect_stale_backends_locked(registry)
        existing = registry.get(workspace)
        if existing is not None:
            backend: CqCacheBackend = existing
        else:
            backend = _build_workspace_backend(root=root)
            registry.set(workspace, backend)
    _close_backends(stale)
    return backend


def set_cq_cache_backend(*, root: Path, backend: CqCacheBackend) -> None:
    """Inject a workspace-scoped cache backend (primarily for tests)."""
    workspace = str(root.resolve())
    stale: list[CqCacheBackend]
    with _BACKEND_LOCK:
        registry = get_default_backend_registry()
        stale = _collect_stale_backends_locked(registry)
        existing = registry.get(workspace)
        if existing is not None and existing is not backend:
            stale.append(existing)
        registry.set(workspace, backend)
    _close_backends(stale)


def close_cq_cache_backend(*, root: Path | None = None) -> None:
    """Close and clear workspace-backed cache backend(s)."""
    backends: list[CqCacheBackend]
    with _BACKEND_LOCK:
        registry = get_default_backend_registry()
        if root is None:
            backends = registry.values()
            registry.clear()
        else:
            workspace = str(root.resolve())
            backend = registry.pop(workspace)
            backends = [backend] if backend is not None else []
    for backend in backends:
        backend.close()


atexit.register(close_cq_cache_backend)


__all__ = [
    "close_cq_cache_backend",
    "get_cq_cache_backend",
    "get_default_backend_registry",
    "set_cq_cache_backend",
    "set_default_backend_registry",
]
