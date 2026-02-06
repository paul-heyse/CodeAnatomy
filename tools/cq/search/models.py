"""Shared search models for smart search."""

from __future__ import annotations

from pathlib import Path
from typing import TypedDict

import msgspec

from tools.cq.core.structs import CqStruct
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.language import DEFAULT_QUERY_LANGUAGE_SCOPE, QueryLanguageScope
from tools.cq.search.classifier import QueryMode
from tools.cq.search.profiles import SearchLimits


class SearchKwargs(TypedDict, total=False):
    """Keyword options for smart search entrypoints."""

    mode: QueryMode | None
    lang_scope: QueryLanguageScope
    include_globs: list[str] | None
    exclude_globs: list[str] | None
    include_strings: bool
    limits: SearchLimits | None
    tc: Toolchain | None
    argv: list[str] | None
    started_ms: float | None


class CandidateSearchKwargs(TypedDict, total=False):
    """Keyword options for candidate search command generation."""

    lang_scope: QueryLanguageScope
    include_globs: list[str] | None
    exclude_globs: list[str] | None


class SearchConfig(CqStruct, frozen=True):
    """Resolved configuration for smart search execution."""

    root: Path
    query: str
    mode: QueryMode
    limits: SearchLimits
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None
    include_strings: bool = False
    argv: list[str] = msgspec.field(default_factory=list)
    tc: Toolchain | None = None
    started_ms: float = 0.0


class SearchRequest(CqStruct, frozen=True):
    """Typed input request for smart search entrypoints."""

    root: Path
    query: str
    mode: QueryMode | None = None
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None
    include_strings: bool = False
    limits: SearchLimits | None = None
    tc: Toolchain | None = None
    argv: list[str] | None = None
    started_ms: float | None = None


class CandidateSearchRequest(CqStruct, frozen=True):
    """Typed candidate-search request."""

    root: Path
    query: str
    mode: QueryMode
    limits: SearchLimits
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None


__all__ = [
    "CandidateSearchKwargs",
    "CandidateSearchRequest",
    "SearchConfig",
    "SearchKwargs",
    "SearchRequest",
]
