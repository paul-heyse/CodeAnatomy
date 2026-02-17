"""Type definitions for Smart Search pipeline.

Extracted from smart_search.py to reduce module size and improve maintainability.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import TYPE_CHECKING

from tools.cq.core.locations import SourceSpan
from tools.cq.core.structs import CqStruct
from tools.cq.query.language import QueryLanguage
from tools.cq.search.pipeline.classifier import MatchCategory, SymtableEnrichment
from tools.cq.search.pipeline.context_window import ContextWindow
from tools.cq.search.pipeline.enrichment_contracts import (
    PythonEnrichmentV1,
    PythonSemanticEnrichmentV1,
    RustTreeSitterEnrichmentV1,
)

if TYPE_CHECKING:
    from ast_grep_py import SgNode, SgRoot

    from tools.cq.core.front_door_assembly import InsightNeighborhoodV1
    from tools.cq.core.schema import Finding, Section
    from tools.cq.core.semantic_contracts import SemanticProvider
    from tools.cq.core.summary_contract import CqSummary
    from tools.cq.search.objects.resolve import ObjectResolutionRuntime
    from tools.cq.search.pipeline.contracts import SearchConfig

# Type aliases
_PythonSemanticAnchorKey = tuple[str, int, int, str]


class RawMatch(CqStruct, frozen=True):
    """Raw match from ripgrep candidate generation.

    Parameters
    ----------
    span
        Source span for the match.
    text
        Full line content.
    match_text
        Exact matched substring.
    match_start
        Character-column offset of match start.
    match_end
        Character-column offset of match end.
    match_byte_start
        Line-relative UTF-8 byte offset of match start.
    match_byte_end
        Line-relative UTF-8 byte offset of match end.
    match_abs_byte_start
        Absolute UTF-8 byte offset of match start when provided by ripgrep.
    match_abs_byte_end
        Absolute UTF-8 byte offset of match end when provided by ripgrep.
    submatch_index
        Which submatch on this line.
    """

    span: SourceSpan
    text: str
    match_text: str
    match_start: int
    match_end: int
    match_byte_start: int
    match_byte_end: int
    match_abs_byte_start: int | None = None
    match_abs_byte_end: int | None = None
    submatch_index: int = 0

    @property
    def file(self) -> str:
        """Return the file path for backward compatibility."""
        return self.span.file

    @property
    def line(self) -> int:
        """Return the start line for backward compatibility."""
        return self.span.start_line

    @property
    def col(self) -> int:
        """Return the start column for backward compatibility."""
        return self.span.start_col


@dataclass(frozen=True, slots=True)
class SearchSummaryInputs:
    """Summary computation inputs."""

    config: SearchConfig
    stats: SearchStats
    matches: list[EnrichedMatch]
    languages: tuple[QueryLanguage, ...]
    language_stats: dict[QueryLanguage, SearchStats]
    file_globs: list[str] | None = None
    limit: int | None = None
    pattern: str | None = None


@dataclass(frozen=True, slots=True)
class ClassificationResult:
    """Match classification result."""

    category: MatchCategory
    confidence: float
    evidence_kind: str
    node_kind: str | None
    containing_scope: str | None


@dataclass(frozen=True, slots=True)
class ResolvedNodeContext:
    """Resolved AST node context."""

    sg_root: SgRoot
    node: SgNode
    line: int
    col: int


@dataclass(frozen=True, slots=True)
class MatchEnrichment:
    """Match enrichment data."""

    symtable: SymtableEnrichment | None
    context_window: ContextWindow | None
    context_snippet: str | None
    rust_tree_sitter: RustTreeSitterEnrichmentV1 | None
    python_enrichment: PythonEnrichmentV1 | None
    python_semantic_enrichment: PythonSemanticEnrichmentV1 | None


@dataclass(frozen=True, slots=True)
class MatchClassifyOptions:
    """Classification options."""

    enable_symtable: bool = True
    force_semantic_enrichment: bool = False
    enable_python_semantic: bool = False
    enable_deep_enrichment: bool = True


class SearchStats(CqStruct, frozen=True):
    """Statistics from candidate generation phase.

    Parameters
    ----------
    scanned_files
        Number of files scanned.
    scanned_files_is_estimate
        Whether scanned_files is an estimate.
    matched_files
        Number of files with matches.
    total_matches
        Total match count.
    truncated
        Whether results were truncated.
    timed_out
        Whether search timed out.
    dropped_by_scope
        Number of candidate matches discarded due to scope extension mismatch.
    """

    scanned_files: int
    matched_files: int
    total_matches: int
    scanned_files_is_estimate: bool = True
    truncated: bool = False
    timed_out: bool = False
    max_files_hit: bool = False
    max_matches_hit: bool = False
    dropped_by_scope: int = 0
    rg_stats: dict[str, object] | None = None


class EnrichedMatch(CqStruct, frozen=True):
    """Fully enriched match with classification and context.

    Parameters
    ----------
    span
        Source span for the match.
    text
        Full line content.
    match_text
        Exact matched substring.
    category
        Classified match category.
    confidence
        Classification confidence.
    evidence_kind
        Classification evidence source.
    node_kind
        AST node kind, if available.
    containing_scope
        Containing function/class name.
    context_window
        Line range for context.
    context_snippet
        Source code snippet.
    symtable
        Symtable enrichment data.
    rust_tree_sitter
        Optional best-effort Rust context details from tree-sitter-rust.
    python_enrichment
        Optional best-effort Python context details from python_enrichment.
    """

    span: SourceSpan
    text: str
    match_text: str
    category: MatchCategory
    confidence: float
    evidence_kind: str
    node_kind: str | None = None
    containing_scope: str | None = None
    context_window: ContextWindow | None = None
    context_snippet: str | None = None
    symtable: SymtableEnrichment | None = None
    rust_tree_sitter: RustTreeSitterEnrichmentV1 | None = None
    python_enrichment: PythonEnrichmentV1 | None = None
    python_semantic_enrichment: PythonSemanticEnrichmentV1 | None = None
    language: QueryLanguage = "python"

    @property
    def file(self) -> str:
        """Return the file path for backward compatibility."""
        return self.span.file

    @property
    def line(self) -> int:
        """Return the start line for backward compatibility."""
        return self.span.start_line

    @property
    def col(self) -> int:
        """Return the start column for backward compatibility."""
        return self.span.start_col


class ClassificationBatchTask(CqStruct, frozen=True):
    """Typed task envelope for process-pool classification."""

    root: str
    lang: QueryLanguage
    batch: list[tuple[int, RawMatch]]


class ClassificationBatchResult(CqStruct, frozen=True):
    """Typed result envelope for process-pool classification."""

    index: int
    match: EnrichedMatch


@dataclass(frozen=True, slots=True)
class LanguageSearchResult:
    """Search result for a single language partition."""

    lang: QueryLanguage
    raw_matches: list[RawMatch]
    stats: SearchStats
    pattern: str
    enriched_matches: list[EnrichedMatch]
    dropped_by_scope: int
    python_semantic_prefetch: _PythonSemanticPrefetchResult | None = None


@dataclass(frozen=True, slots=True)
class _PythonSemanticPrefetchResult:
    """Python semantic prefetch result."""

    payloads: dict[_PythonSemanticAnchorKey, dict[str, object]] = dataclass_field(
        default_factory=dict
    )
    attempted_keys: set[_PythonSemanticAnchorKey] = dataclass_field(default_factory=set)
    telemetry: dict[str, int] = dataclass_field(default_factory=dict)
    diagnostics: list[dict[str, object]] = dataclass_field(default_factory=list)


@dataclass(slots=True)
class _PythonSemanticOverviewAccumulator:
    """Accumulator for Python semantic overview building."""

    primary_symbol: str | None = None
    enclosing_class: str | None = None
    total_incoming: int = 0
    total_outgoing: int = 0
    total_implementations: int = 0
    diagnostics: int = 0
    enriched_count: int = 0


@dataclass(slots=True)
class _SearchSemanticOutcome:
    """Search semantic enrichment outcome."""

    provider: SemanticProvider = "none"
    target_language: str | None = None
    payload: dict[str, object] | None = None
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    timed_out: int = 0
    reasons: list[str] = dataclass_field(default_factory=list)


@dataclass(slots=True)
class _SearchAssemblyInputs:
    """Assembly phase inputs."""

    enriched_matches: list[EnrichedMatch]
    object_runtime: ObjectResolutionRuntime
    summary: CqSummary
    sections: list[Section]
    all_diagnostics: list[Finding]
    definition_matches: list[EnrichedMatch]
    candidate_findings: list[Finding]
    primary_target_finding: Finding | None
    primary_target_match: EnrichedMatch | None
    insight_neighborhood: InsightNeighborhoodV1 | None
    neighborhood_notes: list[str]


@dataclass(frozen=True, slots=True)
class _NeighborhoodPreviewInputs:
    """Neighborhood preview computation inputs."""

    primary_target_finding: Finding | None
    definition_matches: list[EnrichedMatch]
    has_target_candidates: bool


@dataclass(frozen=True, slots=True)
class SearchResultAssembly:
    """Assembly inputs for compatibility with package-level orchestrators."""

    context: SearchConfig
    partition_results: list[LanguageSearchResult]


PythonSemanticPrefetchResult = _PythonSemanticPrefetchResult


__all__ = [
    "ClassificationBatchResult",
    "ClassificationBatchTask",
    "ClassificationResult",
    "EnrichedMatch",
    "LanguageSearchResult",
    "MatchClassifyOptions",
    "MatchEnrichment",
    "PythonSemanticPrefetchResult",
    "RawMatch",
    "ResolvedNodeContext",
    "SearchResultAssembly",
    "SearchStats",
    "SearchSummaryInputs",
]
