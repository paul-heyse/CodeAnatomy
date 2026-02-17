"""Shared request/settings contracts for search subsystem."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import msgspec

from tools.cq.core.structs import CqSettingsStruct, CqStruct
from tools.cq.core.types import QueryLanguage

if TYPE_CHECKING:
    from tools.cq.search._shared.types import QueryMode, SearchLimits
    from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext
    from tools.cq.search.rg.contracts import RgRunSettingsV1


class PythonClassifierSessionLike(Protocol):
    """Runtime session surface required by Python enrichment extractors."""

    classifier_cache: ClassifierCacheContext


class PythonNodeEnrichmentSettingsV1(CqSettingsStruct, frozen=True):
    """Serializable settings for node-anchored Python enrichment."""

    source_bytes: bytes
    line: int
    col: int
    cache_key: str
    byte_start: int | None = None
    byte_end: int | None = None
    query_budget_ms: int | None = None


class PythonByteRangeEnrichmentSettingsV1(CqSettingsStruct, frozen=True):
    """Serializable settings for byte-range Python enrichment."""

    source_bytes: bytes
    byte_start: int
    byte_end: int
    cache_key: str
    resolved_line: int | None = None
    resolved_col: int | None = None
    query_budget_ms: int | None = None


@dataclass(frozen=True, slots=True)
class PythonNodeRuntimeV1:
    """Runtime-only handles for node-anchored Python enrichment."""

    sg_root: object
    node: object
    session: PythonClassifierSessionLike | None = None


@dataclass(frozen=True, slots=True)
class PythonByteRangeRuntimeV1:
    """Runtime-only handles for byte-range Python enrichment."""

    sg_root: object
    resolved_node: object | None = None
    session: PythonClassifierSessionLike | None = None


class RgRunRequest(CqStruct, frozen=True):
    """Input contract for native ripgrep execution."""

    root: Path
    pattern: str
    mode: QueryMode
    lang_types: tuple[str, ...]
    limits: SearchLimits
    include_globs: list[str] = msgspec.field(default_factory=list)
    exclude_globs: list[str] = msgspec.field(default_factory=list)
    operation: str = "json"
    paths: tuple[str, ...] = (".",)
    extra_patterns: tuple[str, ...] = ()

    def to_settings(self) -> RgRunSettingsV1:
        """Return serializable rg execution settings contract."""
        from tools.cq.search.rg.contracts import RgRunSettingsV1

        return RgRunSettingsV1(
            pattern=self.pattern,
            mode=self.mode.value,
            lang_types=self.lang_types,
            include_globs=tuple(self.include_globs),
            exclude_globs=tuple(self.exclude_globs),
            operation=self.operation,
            paths=self.paths,
            extra_patterns=self.extra_patterns,
        )


class CandidateCollectionRequest(CqStruct, frozen=True):
    """Input contract for raw candidate collection."""

    root: Path
    pattern: str
    mode: QueryMode
    limits: SearchLimits
    lang: QueryLanguage
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None
    pcre2_available: bool = False


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
    query_budget_ms: int | None = None
    session: PythonClassifierSessionLike | None = None

    def to_settings(self) -> PythonNodeEnrichmentSettingsV1:
        """Return serializable settings subset for transport/cache boundaries."""
        return PythonNodeEnrichmentSettingsV1(
            source_bytes=self.source_bytes,
            line=self.line,
            col=self.col,
            cache_key=self.cache_key,
            byte_start=self.byte_start,
            byte_end=self.byte_end,
            query_budget_ms=self.query_budget_ms,
        )

    def to_runtime(self) -> PythonNodeRuntimeV1:
        """Return runtime-only handle container."""
        return PythonNodeRuntimeV1(sg_root=self.sg_root, node=self.node, session=self.session)


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
    query_budget_ms: int | None = None
    session: PythonClassifierSessionLike | None = None

    def to_settings(self) -> PythonByteRangeEnrichmentSettingsV1:
        """Return serializable settings subset for transport/cache boundaries."""
        return PythonByteRangeEnrichmentSettingsV1(
            source_bytes=self.source_bytes,
            byte_start=self.byte_start,
            byte_end=self.byte_end,
            cache_key=self.cache_key,
            resolved_line=self.resolved_line,
            resolved_col=self.resolved_col,
            query_budget_ms=self.query_budget_ms,
        )

    def to_runtime(self) -> PythonByteRangeRuntimeV1:
        """Return runtime-only handle container."""
        return PythonByteRangeRuntimeV1(
            sg_root=self.sg_root,
            resolved_node=self.resolved_node,
            session=self.session,
        )


class RustEnrichmentRequest(CqStruct, frozen=True):
    """Input contract for byte-range anchored Rust enrichment."""

    source: str
    byte_start: int
    byte_end: int
    cache_key: str | None = None
    max_scope_depth: int = 24
    query_budget_ms: int | None = None


__all__ = [
    "CandidateCollectionRequest",
    "PythonByteRangeEnrichmentRequest",
    "PythonByteRangeEnrichmentSettingsV1",
    "PythonByteRangeRuntimeV1",
    "PythonNodeEnrichmentRequest",
    "PythonNodeEnrichmentSettingsV1",
    "PythonNodeRuntimeV1",
    "RgRunRequest",
    "RustEnrichmentRequest",
]
