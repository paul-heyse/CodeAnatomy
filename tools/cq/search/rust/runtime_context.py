"""Runtime cache context for Rust ast-grep enrichment."""

from __future__ import annotations

import threading

import msgspec

from tools.cq.search._shared.bounded_cache import BoundedCache
from tools.cq.search.cache.registry import CACHE_REGISTRY

_MAX_AST_CACHE_ENTRIES = 64


class RustEnrichmentRuntimeContext(msgspec.Struct):
    """Mutable runtime state for Rust enrichment internals."""

    ast_cache: BoundedCache[str, tuple[object, str]] = msgspec.field(
        default_factory=lambda: BoundedCache(max_size=_MAX_AST_CACHE_ENTRIES, policy="fifo")
    )
    cache_registered: bool = False


_DEFAULT_RUST_RUNTIME_CONTEXT_LOCK = threading.Lock()
_DEFAULT_RUST_RUNTIME_CONTEXT: RustEnrichmentRuntimeContext | None = None


def get_default_rust_runtime_context() -> RustEnrichmentRuntimeContext:
    """Return process-default runtime context for Rust enrichment."""
    global _DEFAULT_RUST_RUNTIME_CONTEXT
    with _DEFAULT_RUST_RUNTIME_CONTEXT_LOCK:
        context = _DEFAULT_RUST_RUNTIME_CONTEXT
        if context is None:
            context = RustEnrichmentRuntimeContext()
            _DEFAULT_RUST_RUNTIME_CONTEXT = context
        return context


def set_default_rust_runtime_context(context: RustEnrichmentRuntimeContext | None) -> None:
    """Install or clear process-default Rust runtime context."""
    global _DEFAULT_RUST_RUNTIME_CONTEXT
    with _DEFAULT_RUST_RUNTIME_CONTEXT_LOCK:
        _DEFAULT_RUST_RUNTIME_CONTEXT = context


def ensure_rust_cache_registered(ctx: RustEnrichmentRuntimeContext) -> None:
    """Register cache handle with global cache registry once per context."""
    if ctx.cache_registered:
        return
    CACHE_REGISTRY.register_cache("rust", "rust_enrichment:ast", ctx.ast_cache)
    ctx.cache_registered = True


__all__ = [
    "RustEnrichmentRuntimeContext",
    "ensure_rust_cache_registered",
    "get_default_rust_runtime_context",
    "set_default_rust_runtime_context",
]
