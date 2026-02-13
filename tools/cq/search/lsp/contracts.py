"""Shared request/capability protocol helpers for LSP clients."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Protocol, cast


class LspRequestClient(Protocol):
    """Runtime protocol for session-backed LSP request clients."""

    def _send_request(
        self,
        method: str,
        params: dict[str, object],
        *,
        timeout_seconds: float | None = None,
    ) -> object:
        """Dispatch one LSP request and return raw result payload."""
        ...

    def capabilities_snapshot(self) -> dict[str, object]:
        """Return server capability snapshot."""
        ...


LspRequestCallable = Callable[[str, dict[str, object]], object]


def resolve_request_callable(session: object) -> LspRequestCallable | None:
    """Resolve a best-effort request callable from an LSP session object.

    Returns:
        Callable request function when available, else ``None``.
    """
    request = getattr(session, "_send_request", None)
    if not callable(request):
        return None
    return cast("LspRequestCallable", request)


def resolve_capabilities_snapshot(session: object) -> dict[str, object]:
    """Resolve best-effort server capabilities from a session object.

    Returns:
        Dictionary-form capability snapshot when available, else empty dict.
    """
    snapshot_fn = getattr(session, "capabilities_snapshot", None)
    if callable(snapshot_fn):
        snapshot = snapshot_fn()
        if isinstance(snapshot, Mapping):
            return {str(key): value for key, value in snapshot.items()}

    raw_caps = getattr(session, "_server_capabilities", None)
    if isinstance(raw_caps, Mapping):
        return {str(key): value for key, value in raw_caps.items()}
    return {}


__all__ = [
    "LspRequestCallable",
    "LspRequestClient",
    "resolve_capabilities_snapshot",
    "resolve_request_callable",
]
