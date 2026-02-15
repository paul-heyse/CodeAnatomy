"""Rust contracts: macro expansion and module graph evidence types."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqOutputStruct

# --- From macro_expansion_contracts.py ---


class RustMacroExpansionRequestV1(CqOutputStruct, frozen=True):
    """One rust-analyzer macro expansion request."""

    file_path: str
    line: int
    col: int
    macro_call_id: str


class RustMacroExpansionResultV1(CqOutputStruct, frozen=True):
    """One rust-analyzer macro expansion response."""

    macro_call_id: str
    name: str | None = None
    expansion: str | None = None
    source: str = "rust_analyzer"
    applied: bool = False


# --- From module_graph_contracts.py ---


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


__all__ = [
    "RustImportEdgeV1",
    "RustMacroExpansionRequestV1",
    "RustMacroExpansionResultV1",
    "RustModuleGraphV1",
    "RustModuleNodeV1",
]
