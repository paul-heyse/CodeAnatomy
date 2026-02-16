"""Typed Rust enrichment fact structs."""

from __future__ import annotations

from typing import Literal

import msgspec

from tools.cq.core.structs import CqStruct

RustScopeKind = Literal[
    "function_item",
    "struct_item",
    "enum_item",
    "trait_item",
    "impl_item",
    "mod_item",
    "macro_invocation",
]

RustVisibility = Literal["pub", "pub(crate)", "pub(super)", "private"]

RustItemRole = Literal[
    "test",
    "benchmark",
    "async_fn",
    "trait_method",
    "impl_method",
    "regular",
]


class RustStructureFacts(CqStruct, frozen=True):
    """Structural metadata from Rust enrichment.

    Fields populated by enrichment.py:_base_rust_payload.
    """

    node_kind: str | None = None
    scope_kind: RustScopeKind | None = None
    scope_name: str | None = None
    scope_chain: list[str] = msgspec.field(default_factory=list)


class RustDefinitionFacts(CqStruct, frozen=True):
    """Definition metadata from Rust enrichment.

    Fields populated by enrichment.py:_apply_definition_metadata.
    """

    visibility: RustVisibility | None = None
    attributes: list[str] = msgspec.field(default_factory=list)
    item_role: RustItemRole | None = None


class RustMacroExpansionFacts(CqStruct, frozen=True):
    """Macro expansion metadata from Rust enrichment.

    Fields populated by enrichment.py when macro expansion is enabled.
    """

    macro_expansion_results: list[dict[str, object]] = msgspec.field(default_factory=list)


class RustEnrichmentFacts(CqStruct, frozen=True):
    """Rust tree-sitter enrichment payload.

    Aggregates all Rust enrichment fact categories.
    """

    structure: RustStructureFacts | None = None
    definition: RustDefinitionFacts | None = None
    macro_expansion: RustMacroExpansionFacts | None = None


__all__ = [
    "RustDefinitionFacts",
    "RustEnrichmentFacts",
    "RustItemRole",
    "RustMacroExpansionFacts",
    "RustScopeKind",
    "RustStructureFacts",
    "RustVisibility",
]
