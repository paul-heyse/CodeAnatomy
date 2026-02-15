"""Contracts for tree-sitter tags extraction."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqStruct


class RustTagEventV1(CqStruct, frozen=True):
    """One Rust tag event row derived from tags query captures."""

    role: str
    kind: str
    name: str
    start_byte: int
    end_byte: int
    metadata: dict[str, object] = msgspec.field(default_factory=dict)


__all__ = ["RustTagEventV1"]
