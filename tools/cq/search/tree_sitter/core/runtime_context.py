"""Mutable runtime context for tree-sitter subsystem state."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field


@dataclass
class TreeSitterRuntimeContext:
    """Aggregated mutable state for tree-sitter runtime helpers."""

    parse_sessions: dict[str, object] = field(default_factory=dict)
    query_runtime_state: object | None = None
    rust_tree_cache: object | None = None
    rust_tree_cache_evictions: int = 0


_CONTEXT_LOCK = threading.Lock()
_DEFAULT_CONTEXT_STATE: dict[str, TreeSitterRuntimeContext | None] = {"context": None}


def get_default_context() -> TreeSitterRuntimeContext:
    """Return process-global runtime context, creating lazily."""
    with _CONTEXT_LOCK:
        context = _DEFAULT_CONTEXT_STATE["context"]
        if context is None:
            context = TreeSitterRuntimeContext()
            _DEFAULT_CONTEXT_STATE["context"] = context
        return context


def set_default_context(context: TreeSitterRuntimeContext | None) -> None:
    """Install or clear process-global runtime context (test seam)."""
    with _CONTEXT_LOCK:
        _DEFAULT_CONTEXT_STATE["context"] = context


__all__ = ["TreeSitterRuntimeContext", "get_default_context", "set_default_context"]
