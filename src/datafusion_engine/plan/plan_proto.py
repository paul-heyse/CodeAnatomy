"""Plan proto serialization helpers for plan artifacts."""

from __future__ import annotations

from collections.abc import Callable


def proto_serialization_enabled() -> bool:
    """Return True when plan proto serialization should be attempted."""
    try:
        from datafusion_engine.extensions import datafusion_ext
    except ImportError:
        return True
    return not bool(getattr(datafusion_ext, "IS_STUB", False))


def plan_to_proto_bytes(plan: object | None, *, enabled: bool) -> bytes | None:
    """Serialize a plan object to proto bytes when supported.

    Returns:
        bytes | None: Serialized proto bytes when available, otherwise `None`.
    """
    if plan is None:
        return None
    if not enabled or not proto_serialization_enabled():
        return None
    method = getattr(plan, "to_proto", None)
    if not callable(method):
        return None
    try:
        payload = method()
    except (RuntimeError, TypeError, ValueError, AttributeError):
        # DataFusion may raise errors when proto codecs are unavailable.
        return None
    if isinstance(payload, (bytes, bytearray, memoryview)):
        return bytes(payload)
    return None


def plan_proto_payload[T](
    plan: object | None,
    wrapper: Callable[[bytes], T],
    *,
    enabled: bool,
) -> T | None:
    """Serialize a plan and wrap proto bytes in a typed payload.

    Returns:
        T | None: Wrapped payload when serialization succeeds, otherwise `None`.
    """
    payload = plan_to_proto_bytes(plan, enabled=enabled)
    if payload is None:
        return None
    return wrapper(payload)


__all__ = ["plan_proto_payload", "plan_to_proto_bytes", "proto_serialization_enabled"]
