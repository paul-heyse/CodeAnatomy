"""Rust extraction session/provider bridge helpers."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion import SessionContext

from datafusion_engine.extensions import datafusion_ext


def extraction_session_payload(
    *,
    parallelism: int | None = None,
    batch_size: int | None = None,
    memory_limit_bytes: int | None = None,
) -> dict[str, object]:
    """Build normalized extraction-session payload for the native bridge.

    Returns:
        dict[str, object]: Payload suitable for ``build_extraction_session``.
    """
    payload: dict[str, object] = {}
    if parallelism is not None:
        payload["parallelism"] = int(parallelism)
    if batch_size is not None:
        payload["batch_size"] = int(batch_size)
    if memory_limit_bytes is not None:
        payload["memory_limit_bytes"] = int(memory_limit_bytes)
    return payload


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


def register_dataset_provider_payload(
    ctx: SessionContext,
    request_payload: Mapping[str, object],
) -> Mapping[str, object]:
    """Register dataset provider and return normalized payload.

    Returns:
        Mapping[str, object]: Normalized provider registration payload.
    """
    return register_dataset_provider(ctx, request_payload)


__all__ = [
    "build_extraction_session",
    "extraction_session_payload",
    "register_dataset_provider",
    "register_dataset_provider_payload",
]
