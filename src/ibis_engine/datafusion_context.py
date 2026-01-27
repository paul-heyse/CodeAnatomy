"""Helpers for extracting DataFusion SessionContext from Ibis backends."""

from __future__ import annotations

from datafusion import SessionContext


def datafusion_context(backend: object) -> SessionContext:
    """Return a DataFusion SessionContext from an Ibis backend.

    Raises
    ------
    ValueError
        Raised when the backend does not expose a DataFusion SessionContext.

    Returns
    -------
    datafusion.SessionContext
        DataFusion SessionContext detected on the backend.
    """
    for attr in ("con", "_context", "_ctx", "ctx", "session_context"):
        ctx = getattr(backend, attr, None)
        if isinstance(ctx, SessionContext):
            return ctx
    msg = "Ibis backend does not expose a DataFusion SessionContext."
    raise ValueError(msg)


__all__ = ["datafusion_context"]
