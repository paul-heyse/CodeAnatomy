"""Pipeline contracts: partition plans and shared search models."""

from __future__ import annotations

from pathlib import Path

import msgspec

from tools.cq.core.structs import CqSettingsStruct, CqStruct
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.language import (
    DEFAULT_QUERY_LANGUAGE_SCOPE,
    QueryLanguage,
    QueryLanguageScope,
)
from tools.cq.search._shared.types import QueryMode, SearchLimits

# --- From partition_contracts.py ---


class SearchPartitionPlanV1(CqStruct, frozen=True):
    """Serializable envelope for one language partition search execution."""

    root: str
    language: QueryLanguage
    query: str
    mode: str
    include_strings: bool = False
    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
    max_total_matches: int = 0
    run_id: str | None = None


# --- From models.py ---


class SearchConfig(CqSettingsStruct, frozen=True):
    """Resolved configuration for smart search execution."""

    root: Path
    query: str
    mode: QueryMode
    limits: SearchLimits
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE
    mode_requested: QueryMode | None = None
    mode_chain: tuple[QueryMode, ...] = msgspec.field(default_factory=tuple)
    fallback_applied: bool = False
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None
    include_strings: bool = False
    with_neighborhood: bool = False
    argv: list[str] = msgspec.field(default_factory=list)
    tc: Toolchain | None = None
    started_ms: float = 0.0
    run_id: str | None = None


class SearchRequest(CqSettingsStruct, frozen=True):
    """Typed input request for smart search entrypoints."""

    root: Path
    query: str
    mode: QueryMode | None = None
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None
    include_strings: bool = False
    with_neighborhood: bool = False
    limits: SearchLimits | None = None
    tc: Toolchain | None = None
    argv: list[str] | None = None
    started_ms: float | None = None
    run_id: str | None = None


class CandidateSearchRequest(CqSettingsStruct, frozen=True):
    """Typed candidate-search request."""

    root: Path
    query: str
    mode: QueryMode
    limits: SearchLimits
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None


SmartSearchContext = SearchConfig


__all__ = [
    "CandidateSearchRequest",
    "SearchConfig",
    "SearchPartitionPlanV1",
    "SearchRequest",
    "SmartSearchContext",
]
