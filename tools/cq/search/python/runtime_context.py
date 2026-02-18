"""Runtime cache context for Python enrichment."""

from __future__ import annotations

import threading

import msgspec

from tools.cq.search._shared.bounded_cache import BoundedCache
from tools.cq.search.cache.registry import CACHE_REGISTRY

_MAX_TREE_CACHE_ENTRIES = 64


class PythonEnrichmentRuntimeContext(msgspec.Struct):
    """Mutable runtime state for Python enrichment internals."""

    ast_cache: BoundedCache[str, tuple[object, str]] = msgspec.field(
        default_factory=lambda: BoundedCache(max_size=_MAX_TREE_CACHE_ENTRIES, policy="fifo")
    )
    cache_registered: bool = False


_DEFAULT_PYTHON_RUNTIME_CONTEXT_LOCK = threading.Lock()
_DEFAULT_PYTHON_RUNTIME_CONTEXT: PythonEnrichmentRuntimeContext | None = None


def get_default_python_runtime_context() -> PythonEnrichmentRuntimeContext:
    """Return process-default runtime context for Python enrichment."""
    global _DEFAULT_PYTHON_RUNTIME_CONTEXT
    with _DEFAULT_PYTHON_RUNTIME_CONTEXT_LOCK:
        context = _DEFAULT_PYTHON_RUNTIME_CONTEXT
        if context is None:
            context = PythonEnrichmentRuntimeContext()
            _DEFAULT_PYTHON_RUNTIME_CONTEXT = context
        return context


def set_default_python_runtime_context(context: PythonEnrichmentRuntimeContext | None) -> None:
    """Install or clear process-default Python runtime context."""
    global _DEFAULT_PYTHON_RUNTIME_CONTEXT
    with _DEFAULT_PYTHON_RUNTIME_CONTEXT_LOCK:
        _DEFAULT_PYTHON_RUNTIME_CONTEXT = context


def ensure_python_cache_registered(ctx: PythonEnrichmentRuntimeContext) -> None:
    """Register cache handle with global cache registry once per context."""
    if ctx.cache_registered:
        return
    CACHE_REGISTRY.register_cache("python", "python_enrichment:ast", ctx.ast_cache)
    ctx.cache_registered = True


__all__ = [
    "PythonEnrichmentRuntimeContext",
    "ensure_python_cache_registered",
    "get_default_python_runtime_context",
    "set_default_python_runtime_context",
]
