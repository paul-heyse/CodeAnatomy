"""Contracts for language-aware static semantic front-door caching and execution."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.structs import CqStruct
from tools.cq.query.language import QueryLanguage


class LanguageSemanticEnrichmentRequest(CqStruct, frozen=True):
    """Request envelope for language-aware front-door static semantic enrichment."""

    language: QueryLanguage
    mode: str
    root: Path
    file_path: Path
    line: int
    col: int
    symbol_hint: str | None = None
    run_id: str | None = None


class LanguageSemanticEnrichmentOutcome(CqStruct, frozen=True):
    """Normalized static semantic enrichment result for front-door callers."""

    payload: dict[str, object] | None = None
    timed_out: bool = False
    failure_reason: str | None = None
    provider_root: Path | None = None
    macro_expansion_count: int | None = None


class SemanticOutcomeCacheV1(CqStruct, frozen=True):
    """Serialized cache payload for front-door static semantic outcomes."""

    payload: dict[str, object] | None = None
    timed_out: bool = False
    failure_reason: str | None = None


__all__ = [
    "LanguageSemanticEnrichmentOutcome",
    "LanguageSemanticEnrichmentRequest",
    "SemanticOutcomeCacheV1",
]
