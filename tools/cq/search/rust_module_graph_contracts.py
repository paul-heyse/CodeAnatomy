"""Contracts for Rust module/import graph evidence."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqStruct


class RustModuleEdgeV1(CqStruct, frozen=True):
    """Module graph edge row."""

    source: str
    target: str
    kind: str = "imports"


class RustModuleGraphV1(CqStruct, frozen=True):
    """Rust module graph derived from tree-sitter enrichment facts."""

    modules: tuple[str, ...] = ()
    imports: tuple[str, ...] = ()
    edges: tuple[RustModuleEdgeV1, ...] = msgspec.field(default_factory=tuple)


__all__ = ["RustModuleEdgeV1", "RustModuleGraphV1"]
