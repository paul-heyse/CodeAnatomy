"""Rust-lane helpers split from root search modules."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.search.rust.evidence import (
    RustFactPayloadV1,
    RustMacroEvidenceV1,
    attach_rust_evidence,
    build_macro_evidence,
)

if TYPE_CHECKING:
    from tools.cq.search._shared.core import RustEnrichmentRequest


def enrich_context_by_byte_range(
    source: str,
    *,
    byte_start: int,
    byte_end: int,
) -> dict[str, object]:
    """Lazy proxy for enrich_context_by_byte_range to avoid circular import.

    Returns:
        dict[str, object]: Structured Rust enrichment payload for the byte range.
    """
    from tools.cq.search.rust.enrichment import enrich_context_by_byte_range as _fn

    return _fn(source, byte_start=byte_start, byte_end=byte_end)


def extract_rust_context(request: RustEnrichmentRequest) -> dict[str, object]:
    """Lazy proxy for extract_rust_context to avoid circular import.

    Returns:
        dict[str, object]: Structured Rust enrichment payload for the request.
    """
    from tools.cq.search.rust.enrichment import extract_rust_context as _fn

    return _fn(request)


def runtime_available() -> bool:
    """Lazy proxy for runtime_available to avoid circular import.

    Returns:
        bool: ``True`` when Rust tree-sitter runtime dependencies are available.
    """
    from tools.cq.search.rust.enrichment import runtime_available as _fn

    return _fn()


def clear_runtime_cache() -> None:
    """Lazy proxy for clear_tree_sitter_rust_cache to avoid circular import."""
    from tools.cq.search.tree_sitter.rust_lane.runtime import clear_tree_sitter_rust_cache

    clear_tree_sitter_rust_cache()


__all__ = [
    "RustFactPayloadV1",
    "RustMacroEvidenceV1",
    "attach_rust_evidence",
    "build_macro_evidence",
    "clear_runtime_cache",
    "enrich_context_by_byte_range",
    "extract_rust_context",
    "runtime_available",
]
