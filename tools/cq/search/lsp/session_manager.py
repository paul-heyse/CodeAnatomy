"""Shared workspace-keyed LSP session manager."""

from __future__ import annotations

import threading
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

SessionT = TypeVar("SessionT")


class LspSessionManager[SessionT]:
    """Thread-safe root-keyed session cache with restart-on-failure helper."""

    def __init__(
        self,
        *,
        make_session: Callable[[Path], SessionT],
        close_session: Callable[[SessionT], None],
        ensure_started: Callable[[SessionT, float], None],
    ) -> None:
        """Initialize root-keyed session manager callbacks.

        Args:
            make_session: Factory for a new root-bound session.
            close_session: Session shutdown callback.
            ensure_started: Session startup/health callback.
        """
        self._make_session = make_session
        self._close_session = close_session
        self._ensure_started = ensure_started
        self._lock = threading.Lock()
        self._sessions: dict[str, SessionT] = {}

    def for_root(self, root: Path, *, startup_timeout_seconds: float) -> SessionT:
        """Get or start session for workspace root.

        Returns:
            Started session bound to the resolved workspace root.
        """
        root_key = str(root.resolve())
        with self._lock:
            session = self._sessions.get(root_key)
            if session is None:
                session = self._make_session(root)
                self._sessions[root_key] = session
            try:
                self._ensure_started(session, startup_timeout_seconds)
            except (OSError, RuntimeError, TimeoutError, ValueError, TypeError):
                self._close_session(session)
                session = self._make_session(root)
                self._sessions[root_key] = session
                self._ensure_started(session, startup_timeout_seconds)
            return session

    def close_all(self) -> None:
        """Close all sessions and clear cache."""
        with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            self._close_session(session)

    def reset_root(self, root: Path) -> None:
        """Close and evict one workspace session."""
        root_key = str(root.resolve())
        with self._lock:
            session = self._sessions.pop(root_key, None)
        if session is not None:
            self._close_session(session)


__all__ = ["LspSessionManager"]
