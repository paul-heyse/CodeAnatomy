"""Contracts for language-aware LSP front-door caching and execution."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.structs import CqStruct
from tools.cq.query.language import QueryLanguage


class LanguageLspEnrichmentRequest(CqStruct, frozen=True):
    """Request envelope for language-aware front-door LSP enrichment."""

    language: QueryLanguage
    mode: str
    root: Path
    file_path: Path
    line: int
    col: int
    symbol_hint: str | None = None
    run_id: str | None = None


class LanguageLspEnrichmentOutcome(CqStruct, frozen=True):
    """Normalized LSP enrichment result for front-door callers."""

    payload: dict[str, object] | None = None
    timed_out: bool = False
    failure_reason: str | None = None
    provider_root: Path | None = None


class LspOutcomeCacheV1(CqStruct, frozen=True):
    """Serialized cache payload for front-door LSP outcomes."""

    payload: dict[str, object] | None = None
    timed_out: bool = False
    failure_reason: str | None = None


__all__ = [
    "LanguageLspEnrichmentOutcome",
    "LanguageLspEnrichmentRequest",
    "LspOutcomeCacheV1",
]
