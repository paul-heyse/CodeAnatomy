"""Contracts for structured Rust module/import graph evidence."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqOutputStruct


class RustModuleNodeV1(CqOutputStruct, frozen=True):
    """Normalized Rust module node."""

    module_id: str
    module_name: str
    file_path: str | None = None


class RustImportEdgeV1(CqOutputStruct, frozen=True):
    """Normalized Rust import edge."""

    source_module_id: str
    target_path: str
    visibility: str = "private"
    is_reexport: bool = False


class RustModuleGraphV1(CqOutputStruct, frozen=True):
    """Typed module graph payload for Rust enrichment."""

    modules: tuple[RustModuleNodeV1, ...] = ()
    edges: tuple[RustImportEdgeV1, ...] = ()
    metadata: dict[str, object] = msgspec.field(default_factory=dict)


__all__ = ["RustImportEdgeV1", "RustModuleGraphV1", "RustModuleNodeV1"]
