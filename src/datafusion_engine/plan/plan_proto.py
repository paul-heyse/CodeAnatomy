"""Plan proto serialization helpers extracted from bundle_artifact."""

from __future__ import annotations

from datafusion_engine.plan.bundle_artifact import (
    _plan_proto_bytes,
    _proto_serialization_enabled,
)


def proto_serialization_enabled() -> bool:
    """Return whether DataFusion plan proto serialization is enabled."""
    return _proto_serialization_enabled()


def plan_to_proto_bytes(plan: object | None, *, enabled: bool) -> bytes | None:
    """Serialize a plan object to proto bytes when supported.

    Returns:
        bytes | None: Serialized proto payload when supported and available.
    """
    return _plan_proto_bytes(plan, enabled=enabled)


__all__ = ["plan_to_proto_bytes", "proto_serialization_enabled"]
