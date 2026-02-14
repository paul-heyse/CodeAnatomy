"""Contracts for DataFusion planning pipeline runtime requests."""

from __future__ import annotations

from typing import Any

import msgspec


class PlanWithDeltaPinsRequestV1(msgspec.Struct, frozen=True):
    """Request envelope for two-pass planning with Delta pinning."""

    view_nodes: tuple[Any, ...]
    runtime_profile: Any | None
    snapshot: dict[str, object] | None = None
    semantic_context: Any | None = None


__all__ = ["PlanWithDeltaPinsRequestV1"]
