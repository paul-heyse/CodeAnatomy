"""Contracts for optional rust-analyzer macro expansion enrichment."""

from __future__ import annotations

from tools.cq.core.structs import CqOutputStruct


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


__all__ = ["RustMacroExpansionRequestV1", "RustMacroExpansionResultV1"]
