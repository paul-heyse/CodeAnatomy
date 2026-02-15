"""Contracts for Rust macro expansion evidence derived from tree-sitter lanes."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class RustMacroExpansionEvidenceV1(CqStruct, frozen=True):
    """One macro expansion evidence row."""

    macro_name: str
    expansion_hint: str
    confidence: str = "medium"


__all__ = ["RustMacroExpansionEvidenceV1"]
