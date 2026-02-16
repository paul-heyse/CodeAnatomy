"""Session lifecycle helpers."""

from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def create_session_context(profile: DataFusionRuntimeProfile) -> SessionContext:
    """Create or reuse the session context for a runtime profile.

    Returns:
    -------
    SessionContext
        Resolved DataFusion session context.
    """
    return profile.session_context()


def context_cache_key(profile: DataFusionRuntimeProfile) -> str:
    """Return stable context cache key for a runtime profile.

    Returns:
    -------
    str
        Stable cache key value for this runtime profile.
    """
    return profile.context_cache_key()


__all__ = ["context_cache_key", "create_session_context"]
