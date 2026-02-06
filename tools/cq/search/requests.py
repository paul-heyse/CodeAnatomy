"""Shared request structs for CQ search and enrichment entrypoints."""

from __future__ import annotations

from pathlib import Path

import msgspec

from tools.cq.core.structs import CqStruct
from tools.cq.query.language import QueryLanguage
from tools.cq.search.classifier import QueryMode
from tools.cq.search.profiles import SearchLimits


class RgRunRequest(CqStruct, frozen=True):
    """Input contract for native ripgrep JSON execution."""

    root: Path
    pattern: str
    mode: QueryMode
    lang_types: tuple[str, ...]
    limits: SearchLimits
    include_globs: list[str] = msgspec.field(default_factory=list)
    exclude_globs: list[str] = msgspec.field(default_factory=list)


class CandidateCollectionRequest(CqStruct, frozen=True):
    """Input contract for raw candidate collection."""

    root: Path
    pattern: str
    mode: QueryMode
    limits: SearchLimits
    lang: QueryLanguage
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None


class PythonNodeEnrichmentRequest(CqStruct, frozen=True):
    """Input contract for node-anchored Python enrichment."""

    sg_root: object
    node: object
    source_bytes: bytes
    line: int
    col: int
    cache_key: str
    byte_start: int | None = None
    byte_end: int | None = None
    session: object | None = None


class PythonByteRangeEnrichmentRequest(CqStruct, frozen=True):
    """Input contract for byte-range anchored Python enrichment."""

    sg_root: object
    source_bytes: bytes
    byte_start: int
    byte_end: int
    cache_key: str
    resolved_node: object | None = None
    resolved_line: int | None = None
    resolved_col: int | None = None
    session: object | None = None


class RustEnrichmentRequest(CqStruct, frozen=True):
    """Input contract for byte-range anchored Rust enrichment."""

    source: str
    byte_start: int
    byte_end: int
    cache_key: str | None = None
    max_scope_depth: int = 24


__all__ = [
    "CandidateCollectionRequest",
    "PythonByteRangeEnrichmentRequest",
    "PythonNodeEnrichmentRequest",
    "RgRunRequest",
    "RustEnrichmentRequest",
]
