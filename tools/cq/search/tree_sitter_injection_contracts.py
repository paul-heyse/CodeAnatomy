"""Contracts for executable tree-sitter injection runtime."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqStruct


class InjectionRuntimeResultV1(CqStruct, frozen=True):
    """Result of parsing injected ranges for one parser language."""

    language: str
    plan_count: int = 0
    combined_count: int = 0
    parsed: bool = False
    included_ranges_applied: bool = False
    errors: tuple[str, ...] = ()
    metadata: dict[str, object] = msgspec.field(default_factory=dict)


__all__ = ["InjectionRuntimeResultV1"]
