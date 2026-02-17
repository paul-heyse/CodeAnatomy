"""Protocol types for runtime profile mixins."""

from __future__ import annotations

from typing import Any, Protocol

from datafusion import SessionContext, SQLOptions


class RuntimeProfileLike(Protocol):
    """Structural interface for runtime profile mixins."""

    def __getattr__(self, name: str) -> Any:
        """Return dynamically-resolved attributes used across runtime mixins."""
        ...

    def session_context(self) -> SessionContext:
        """Return the runtime SessionContext."""
        ...

    def sql_options(self) -> SQLOptions:
        """Return SQL options bound to the runtime profile."""
        ...
