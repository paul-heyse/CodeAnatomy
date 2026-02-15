"""Contracts for diskcache-backed CQ coordination primitives."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class LaneCoordinationPolicyV1(CqStruct, frozen=True):
    """Coordination policy for bounded tree-sitter lane execution."""

    semaphore_key: str = "cq:tree_sitter:lanes"
    lock_key_suffix: str = ":lock"
    reentrant_key_suffix: str = ":reentrant"
    lane_limit: int = 4
    ttl_seconds: int = 15


__all__ = ["LaneCoordinationPolicyV1"]
