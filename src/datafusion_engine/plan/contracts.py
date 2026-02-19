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


class PlanArtifactPolicyV1(msgspec.Struct, frozen=True):
    """Serialization policy for plan artifacts across process boundaries."""

    cross_process_format: str = "substrait"
    allow_proto_internal: bool = True


class PlanningSurfaceManifestV2(msgspec.Struct, frozen=True):
    """Canonical planning-surface manifest shared by Python and Rust layers."""

    expr_planners: tuple[str, ...] = ()
    relation_planners: tuple[str, ...] = ()
    type_planners: tuple[str, ...] = ()
    table_factories: tuple[str, ...] = ()
    planning_config_keys: dict[str, str] = msgspec.field(default_factory=dict)


__all__ = [
    "PlanArtifactPolicyV1",
    "PlanWithDeltaPinsRequestV1",
    "PlanningSurfaceManifestV2",
]
