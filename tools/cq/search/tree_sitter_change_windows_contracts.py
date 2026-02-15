"""Serializable contracts for changed-range query windows."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class QueryWindowSourceV1(CqStruct, frozen=True):
    """Serializable byte window row."""

    start_byte: int
    end_byte: int


class QueryWindowSetV1(CqStruct, frozen=True):
    """Window set emitted by changed-range planning."""

    windows: tuple[QueryWindowSourceV1, ...] = ()


__all__ = ["QueryWindowSetV1", "QueryWindowSourceV1"]
