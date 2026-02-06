"""Runtime-boundary helpers for CQ search contracts."""

from __future__ import annotations

from collections.abc import Mapping

import msgspec

from tools.cq.core.contracts import contract_to_builtins

_RUNTIME_ONLY_ATTR_NAMES: frozenset[str] = frozenset(
    {
        "ast",
        "tree",
        "node",
        "parser",
        "wrapper",
        "session",
        "cache",
    }
)


class RuntimeBoundarySummary(msgspec.Struct):
    """Lightweight typed summary for runtime-boundary checks."""

    has_runtime_only_keys: bool = False


def to_mapping_payload(value: object) -> dict[str, object]:
    """Convert contract value into a builtins mapping payload.

    Returns:
        dict[str, object]: Mapping payload suitable for boundary transport.

    Raises:
        TypeError: If converted payload is not mapping-shaped.
    """
    payload = contract_to_builtins(value)
    if isinstance(payload, dict):
        return payload
    msg = f"Expected mapping payload, got {type(payload).__name__}"
    raise TypeError(msg)


def has_runtime_only_keys(payload: Mapping[str, object]) -> bool:
    """Return whether payload appears to contain runtime-only keys."""
    return any(key in _RUNTIME_ONLY_ATTR_NAMES for key in payload)


__all__ = [
    "RuntimeBoundarySummary",
    "has_runtime_only_keys",
    "to_mapping_payload",
]
