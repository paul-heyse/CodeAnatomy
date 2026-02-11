"""LDMD (Lightweight Document Metadata) format for progressive disclosure."""

from __future__ import annotations

from tools.cq.ldmd.format import (
    LdmdIndex,
    LdmdParseError,
    build_index,
    get_neighbors,
    get_slice,
    search_sections,
)

__all__ = [
    "LdmdIndex",
    "LdmdParseError",
    "build_index",
    "get_neighbors",
    "get_slice",
    "search_sections",
]
