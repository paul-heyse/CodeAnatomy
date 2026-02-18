"""Runtime cache context for Python enrichment."""

from __future__ import annotations

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


_DEFAULT_PYTHON_RUNTIME_CONTEXT_STATE: dict[str, PythonEnrichmentRuntimeContext | None] = {
    "context": None
}


def get_default_python_runtime_context() -> PythonEnrichmentRuntimeContext:
    """Return process-default runtime context for Python enrichment."""
    context = _DEFAULT_PYTHON_RUNTIME_CONTEXT_STATE["context"]
    if context is None:
        context = PythonEnrichmentRuntimeContext()
        _DEFAULT_PYTHON_RUNTIME_CONTEXT_STATE["context"] = context
    return context


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
]
