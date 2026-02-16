"""Profile and session-runtime helpers."""

from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from datafusion_engine.session.runtime_compile import effective_ident_normalization
from datafusion_engine.session.runtime_session import (
    SessionRuntime,
    session_runtime_for_context,
)


def runtime_for_profile(
    profile: DataFusionRuntimeProfile,
    *,
    use_cache: bool = True,
) -> SessionRuntime:
    """Return session runtime for a profile."""
    _ = use_cache
    return profile.session_runtime()


def runtime_for_context(
    ctx: SessionContext,
    *,
    profile: DataFusionRuntimeProfile,
) -> SessionRuntime:
    """Return session runtime for an explicit context/profile pair.

    Raises:
        RuntimeError: If the runtime cannot be resolved for the provided
            context/profile.
    """
    runtime = session_runtime_for_context(profile, ctx)
    if runtime is None:
        msg = "Failed to build session runtime for provided context/profile."
        raise RuntimeError(msg)
    return runtime


__all__ = [
    "DataFusionRuntimeProfile",
    "SessionRuntime",
    "effective_ident_normalization",
    "runtime_for_context",
    "runtime_for_profile",
]
