"""Single-authority process-wide session caches."""

from __future__ import annotations

from weakref import WeakKeyDictionary

from datafusion import SessionContext

SESSION_CONTEXT_CACHE: dict[str, SessionContext] = {}
SESSION_RUNTIME_CACHE: dict[str, object] = {}
RUNTIME_SETTINGS_OVERLAY: WeakKeyDictionary[SessionContext, dict[str, str]] = WeakKeyDictionary()

__all__ = [
    "RUNTIME_SETTINGS_OVERLAY",
    "SESSION_CONTEXT_CACHE",
    "SESSION_RUNTIME_CACHE",
]
