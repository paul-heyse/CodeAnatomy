"""Contracts for DataFusion planning pipeline runtime requests."""

from __future__ import annotations

from typing import Protocol

import msgspec


class ViewNodeLike(Protocol):
    """Structural subset required for plan-with-pins requests."""

    name: str


class RuntimeProfileLike(Protocol):
    """Structural runtime-profile contract for planning requests."""

    def session_runtime(self) -> object: ...


class SemanticContextLike(Protocol):
    """Structural semantic context contract for plan request payloads."""


class PlanWithDeltaPinsRequestV1(msgspec.Struct, frozen=True):
    """Request envelope for two-pass planning with Delta pinning."""

    view_nodes: tuple[ViewNodeLike, ...]
    runtime_profile: RuntimeProfileLike | None
    snapshot: dict[str, object] | None = None
    semantic_context: SemanticContextLike | None = None


__all__ = ["PlanWithDeltaPinsRequestV1"]
