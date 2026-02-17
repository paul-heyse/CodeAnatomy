"""Rust extraction session/provider bridge helpers."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion import SessionContext

from datafusion_engine.extensions import datafusion_ext


def build_extraction_session(config_payload: Mapping[str, object]) -> SessionContext:
    """Build a session context via native extension bridge.

    Returns:
        SessionContext: Native extension session context.

    Raises:
        TypeError: If the extension response is not SessionContext-compatible.
    """
    ctx = datafusion_ext.build_extraction_session(config_payload)
    if isinstance(ctx, SessionContext):
        return ctx
    inner = getattr(ctx, "ctx", None)
    if isinstance(inner, SessionContext):
        return inner
    msg = "build_extraction_session did not return a SessionContext-compatible object."
    raise TypeError(msg)


def register_dataset_provider(
    ctx: SessionContext,
    request_payload: Mapping[str, object],
) -> Mapping[str, object]:
    """Register dataset provider via native extension bridge.

    Returns:
        Mapping[str, object]: Normalized provider registration payload.

    Raises:
        TypeError: If the extension response is not a mapping payload.
    """
    response = datafusion_ext.register_dataset_provider(ctx, request_payload)
    if not isinstance(response, Mapping):
        msg = "register_dataset_provider returned a non-mapping payload."
        raise TypeError(msg)
    return {str(key): value for key, value in response.items()}


__all__ = ["build_extraction_session", "register_dataset_provider"]
