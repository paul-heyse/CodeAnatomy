"""Smart Search pipeline for semantically-enriched code search."""

from __future__ import annotations

import multiprocessing
import re
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Unpack, cast

import msgspec

from tools.cq.core.locations import (
    SourceSpan,
    line_relative_byte_range_to_absolute,
)
from tools.cq.core.multilang_orchestrator import (
    execute_by_language_scope,
    merge_partitioned_items,
)
from tools.cq.core.multilang_summary import (
    assert_multilang_summary,
    build_multilang_summary,
)
from tools.cq.core.requests import SummaryBuildRequest
from tools.cq.core.run_context import RunContext
from tools.cq.core.schema import (
    Anchor,
    CqResult,
    DetailPayload,
    Finding,
    ScoreDetails,
    Section,
    ms,
)
from tools.cq.core.structs import CqStruct
from tools.cq.query.language import (
    DEFAULT_QUERY_LANGUAGE,
    DEFAULT_QUERY_LANGUAGE_SCOPE,
    QueryLanguage,
    QueryLanguageScope,
    expand_language_scope,
    file_globs_for_scope,
    is_path_in_lang_scope,
    ripgrep_type_for_language,
    ripgrep_types_for_scope,
)
from tools.cq.search.classifier import (
    HeuristicResult,
    MatchCategory,
    NodeClassification,
    QueryMode,
    SymtableEnrichment,
    classify_from_node,
    classify_from_records,
    classify_from_resolved_node,
    classify_heuristic,
    clear_caches,
    detect_query_mode,
    enrich_with_symtable_from_table,
    get_cached_source,
    get_def_lines_cached,
    get_node_index,
    get_sg_root,
    get_symtable_table,
)
from tools.cq.search.collector import RgCollector
from tools.cq.search.context import SmartSearchContext
from tools.cq.search.enrichment.core import normalize_python_payload, normalize_rust_payload
from tools.cq.search.models import (
    CandidateSearchKwargs,
    CandidateSearchRequest,
    SearchConfig,
    SearchKwargs,
    SearchRequest,
)
from tools.cq.search.multilang_diagnostics import (
    build_cross_language_diagnostics,
    build_language_capabilities,
    diagnostics_to_summary_payload,
    is_python_oriented_query_text,
)
from tools.cq.search.profiles import INTERACTIVE, SearchLimits
from tools.cq.search.python_analysis_session import get_python_analysis_session
from tools.cq.search.python_enrichment import (
    _ENRICHMENT_ERRORS as _PYTHON_ENRICHMENT_ERRORS,
)
from tools.cq.search.python_enrichment import enrich_python_context_by_byte_range
from tools.cq.search.requests import (
    CandidateCollectionRequest,
    PythonByteRangeEnrichmentRequest,
    RgRunRequest,
)
from tools.cq.search.rg_native import build_rg_command, run_rg_json
from tools.cq.search.rust_enrichment import enrich_rust_context_by_byte_range
from tools.cq.search.tree_sitter_python import get_tree_sitter_python_cache_stats
from tools.cq.search.tree_sitter_rust import get_tree_sitter_rust_cache_stats

if TYPE_CHECKING:
    from ast_grep_py import SgNode, SgRoot

# Derive smart search limits from INTERACTIVE profile
SMART_SEARCH_LIMITS = msgspec.structs.replace(
    INTERACTIVE,
    max_files=200,
    max_matches_per_file=50,
    max_total_matches=500,
    max_file_size_bytes=2 * 1024 * 1024,
    timeout_seconds=30.0,
)
_CASE_SENSITIVE_DEFAULT = True

# Evidence disclosure cap to keep output high-signal
MAX_EVIDENCE = 100
_RUST_ENRICHMENT_ERRORS = (RuntimeError, TypeError, ValueError, AttributeError, UnicodeError)
MAX_SEARCH_CLASSIFY_WORKERS = 4


@lru_cache(maxsize=1)
def _get_context_helpers() -> tuple[
    Callable[[int, list[tuple[int, int]], int], dict[str, int]],
    Callable[..., str | None],
]:
    """Lazily import context helpers to avoid circular imports.

    Returns:
    -------
    tuple[Callable[[int, list[tuple[int, int]], int], dict[str, int]], Callable[..., str | None]]
        Context window and snippet helpers.
    """
    from tools.cq.macros.calls import _compute_context_window, _extract_context_snippet

    return _compute_context_window, _extract_context_snippet


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
    category: MatchCategory
    confidence: float
    evidence_kind: str
    node_kind: str | None
    containing_scope: str | None


@dataclass(frozen=True, slots=True)
class ResolvedNodeContext:
    sg_root: SgRoot
    node: SgNode
    line: int
    col: int


@dataclass(frozen=True, slots=True)
class MatchEnrichment:
    symtable: SymtableEnrichment | None
    context_window: dict[str, int] | None
    context_snippet: str | None
    rust_tree_sitter: dict[str, object] | None
    python_enrichment: dict[str, object] | None


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
    context_window: dict[str, int] | None = None
    context_snippet: str | None = None
    symtable: SymtableEnrichment | None = None
    rust_tree_sitter: dict[str, object] | None = None
    python_enrichment: dict[str, object] | None = None
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


# Kind weights for relevance scoring
KIND_WEIGHTS: dict[MatchCategory, float] = {
    "definition": 1.0,
    "callsite": 0.8,
    "import": 0.7,
    "from_import": 0.7,
    "reference": 0.6,
    "assignment": 0.5,
    "annotation": 0.5,
    "text_match": 0.3,
    "docstring_match": 0.2,
    "comment_match": 0.15,
    "string_match": 0.1,
}


def build_candidate_searcher(
    root: Path,
    query: str,
    mode: QueryMode,
    limits: SearchLimits,
    **kwargs: Unpack[CandidateSearchKwargs],
) -> tuple[list[str], str]:
    """Build native ``rg`` command for candidate generation.

    Parameters
    ----------
    root
        Repository root to search from.
    query
        Search query string.
    mode
        Query mode (identifier/regex/literal).
    limits
        Search safety limits.
    include_globs
        File patterns to include.
    exclude_globs
        File patterns to exclude.

    Returns:
    -------
    tuple[list[str], str]
        Ripgrep command and effective pattern string.
    """
    request = CandidateSearchRequest(
        root=root,
        query=query,
        mode=mode,
        limits=limits,
        lang_scope=kwargs.get("lang_scope", DEFAULT_QUERY_LANGUAGE_SCOPE),
        include_globs=kwargs.get("include_globs"),
        exclude_globs=kwargs.get("exclude_globs"),
    )
    config = SearchConfig(
        root=request.root,
        query=request.query,
        mode=request.mode,
        lang_scope=request.lang_scope,
        limits=request.limits,
        include_globs=request.include_globs,
        exclude_globs=request.exclude_globs,
        include_strings=False,
        argv=[],
        tc=None,
        started_ms=0.0,
    )
    return _build_candidate_searcher(config)


def _build_candidate_searcher(config: SearchConfig) -> tuple[list[str], str]:
    if config.mode == QueryMode.IDENTIFIER:
        # Word boundary match for identifiers
        pattern = rf"\b{re.escape(config.query)}\b"
    elif config.mode == QueryMode.LITERAL:
        # Exact literal match (non-regex)
        pattern = config.query
    else:
        # User-provided regex (pass through)
        pattern = config.query

    command = build_rg_command(
        pattern=pattern,
        mode=config.mode,
        lang_types=tuple(ripgrep_types_for_scope(config.lang_scope)),
        include_globs=config.include_globs or [],
        exclude_globs=config.exclude_globs or [],
        limits=config.limits,
    )
    return command, pattern


def _build_search_stats(collector: RgCollector, *, timed_out: bool) -> SearchStats:
    scanned_files = len(collector.seen_files)
    matched_files = len(collector.seen_files)
    total_matches = len(collector.matches)
    scanned_files_is_estimate = True
    summary_stats = collector.summary_stats
    if isinstance(summary_stats, dict):
        searches = summary_stats.get("searches")
        searches_with_match = summary_stats.get("searches_with_match")
        matches_reported = summary_stats.get("matches")
        if isinstance(searches, int):
            scanned_files = searches
            scanned_files_is_estimate = False
        if isinstance(searches_with_match, int):
            matched_files = searches_with_match
        if isinstance(matches_reported, int):
            total_matches = matches_reported

    return SearchStats(
        scanned_files=scanned_files,
        matched_files=matched_files,
        total_matches=total_matches,
        truncated=collector.truncated,
        timed_out=timed_out,
        max_files_hit=collector.max_files_hit,
        max_matches_hit=collector.max_matches_hit,
        scanned_files_is_estimate=scanned_files_is_estimate,
    )


def collect_candidates(request: CandidateCollectionRequest) -> tuple[list[RawMatch], SearchStats]:
    """Execute native ``rg`` search and collect raw matches.

    Parameters
    ----------
    root
        Repository root path.
    pattern
        Effective pattern passed to ripgrep.
    mode
        Search mode.
    limits
        Search safety limits.
    lang
        Concrete language partition for this candidate pass.
    include_globs
        Optional include globs for the candidate pass.
    exclude_globs
        Optional exclude globs for the candidate pass.

    Returns:
    -------
    tuple[list[RawMatch], SearchStats]
        Raw matches and collection statistics.
    """
    proc = run_rg_json(
        RgRunRequest(
            root=request.root,
            pattern=request.pattern,
            mode=request.mode,
            lang_types=(ripgrep_type_for_language(request.lang),),
            include_globs=request.include_globs or [],
            exclude_globs=request.exclude_globs or [],
            limits=request.limits,
        )
    )
    collector = RgCollector(limits=request.limits, match_factory=RawMatch)
    for event in proc.events:
        collector.handle_event(event)
    collector.finalize()
    scope_filtered = [
        match for match in collector.matches if is_path_in_lang_scope(match.file, request.lang)
    ]
    dropped_by_scope = len(collector.matches) - len(scope_filtered)
    stats = _build_search_stats(collector, timed_out=proc.timed_out)
    stats = msgspec.structs.replace(
        stats,
        matched_files=len({match.file for match in scope_filtered}),
        total_matches=len(scope_filtered),
        dropped_by_scope=dropped_by_scope,
    )
    return scope_filtered, stats


def classify_match(
    raw: RawMatch,
    root: Path,
    *,
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
    enable_symtable: bool = True,
    force_semantic_enrichment: bool = False,
) -> EnrichedMatch:
    """Run three-stage classification pipeline on a raw match.

    Parameters
    ----------
    raw
        Raw match from ripgrep.
    root
        Repository root for file resolution.
    lang
        Query language used by parsing/classification stages.
    enable_symtable
        Whether to run symtable enrichment.
    force_semantic_enrichment
        Force AST-driven enrichment even when heuristics could short-circuit.

    Returns:
    -------
    EnrichedMatch
        Fully classified and enriched match.
    """
    match_text = raw.match_text

    # Stage 1: Fast heuristic
    heuristic = classify_heuristic(raw.text, raw.col, match_text)

    if heuristic.skip_deeper and heuristic.category is not None and not force_semantic_enrichment:
        return _build_heuristic_enriched(raw, heuristic, match_text, lang=lang)

    file_path = root / raw.file
    resolved_python = _resolve_python_node_context(file_path, raw) if lang == "python" else None
    classification = _resolve_match_classification(
        raw,
        file_path,
        heuristic,
        root,
        lang=lang,
        resolved_python=resolved_python,
    )
    symtable_enrichment = _maybe_symtable_enrichment(
        file_path,
        raw,
        classification,
        lang=lang,
        enable_symtable=enable_symtable,
    )
    rust_tree_sitter = _maybe_rust_tree_sitter_enrichment(
        file_path,
        raw,
        lang=lang,
    )
    python_enrichment = _maybe_python_enrichment(
        file_path,
        raw,
        lang=lang,
        resolved_python=resolved_python,
    )
    context_window, context_snippet = _build_context_enrichment(file_path, raw, lang=lang)
    enrichment = MatchEnrichment(
        symtable=symtable_enrichment,
        context_window=context_window,
        context_snippet=context_snippet,
        rust_tree_sitter=rust_tree_sitter,
        python_enrichment=python_enrichment,
    )
    return _build_enriched_match(
        raw,
        match_text,
        classification,
        enrichment,
        lang=lang,
    )


def _build_heuristic_enriched(
    raw: RawMatch,
    heuristic: HeuristicResult,
    match_text: str,
    *,
    lang: QueryLanguage,
) -> EnrichedMatch:
    category = heuristic.category or "text_match"
    return EnrichedMatch(
        span=raw.span,
        text=raw.text,
        match_text=match_text,
        category=category,
        confidence=heuristic.confidence,
        evidence_kind="heuristic",
        language=lang,
    )


def _resolve_match_classification(
    raw: RawMatch,
    file_path: Path,
    heuristic: HeuristicResult,
    root: Path,
    *,
    lang: QueryLanguage,
    resolved_python: ResolvedNodeContext | None = None,
) -> ClassificationResult:
    lang_suffixes = {".py", ".pyi"} if lang == "python" else {".rs"}
    if file_path.suffix not in lang_suffixes:
        return _classification_from_heuristic(
            heuristic, default_confidence=0.4, evidence_kind="rg_only"
        )

    record_result = classify_from_records(file_path, root, raw.line, raw.col, lang=lang)
    ast_result = record_result or _classify_from_node(
        file_path,
        raw,
        lang=lang,
        resolved_python=resolved_python,
    )
    if ast_result is not None:
        return _classification_from_node(ast_result)
    if heuristic.category is not None:
        return _classification_from_heuristic(heuristic, default_confidence=heuristic.confidence)
    return _default_classification()


def _classify_from_node(
    file_path: Path,
    raw: RawMatch,
    *,
    lang: QueryLanguage,
    resolved_python: ResolvedNodeContext | None = None,
) -> NodeClassification | None:
    if lang == "python" and resolved_python is not None:
        return classify_from_resolved_node(resolved_python.node)
    sg_root = get_sg_root(file_path, lang=lang)
    if sg_root is None:
        return None
    return classify_from_node(sg_root, raw.line, raw.col, lang=lang)


def _classification_from_node(result: NodeClassification) -> ClassificationResult:
    return ClassificationResult(
        category=result.category,
        confidence=result.confidence,
        evidence_kind=result.evidence_kind,
        node_kind=result.node_kind,
        containing_scope=result.containing_scope,
    )


def _classification_from_heuristic(
    heuristic: HeuristicResult,
    *,
    default_confidence: float,
    evidence_kind: str = "heuristic",
) -> ClassificationResult:
    category = heuristic.category or "text_match"
    confidence = heuristic.confidence if heuristic.category is not None else default_confidence
    return ClassificationResult(
        category=category,
        confidence=confidence,
        evidence_kind=evidence_kind,
        node_kind=None,
        containing_scope=None,
    )


def _default_classification() -> ClassificationResult:
    return ClassificationResult(
        category="text_match",
        confidence=0.50,
        evidence_kind="rg_only",
        node_kind=None,
        containing_scope=None,
    )


def _maybe_symtable_enrichment(
    file_path: Path,
    raw: RawMatch,
    classification: ClassificationResult,
    *,
    lang: QueryLanguage,
    enable_symtable: bool,
) -> SymtableEnrichment | None:
    if lang != "python":
        return None
    if not enable_symtable:
        return None
    if classification.category not in {"definition", "callsite", "reference", "assignment"}:
        return None
    source = get_cached_source(file_path)
    if source is None:
        return None
    table = get_symtable_table(file_path, source)
    if table is None:
        return None
    return enrich_with_symtable_from_table(table, raw.match_text, raw.line)


def _build_context_enrichment(
    file_path: Path,
    raw: RawMatch,
    *,
    lang: QueryLanguage,
) -> tuple[dict[str, int] | None, str | None]:
    source = get_cached_source(file_path)
    if source is None:
        return None, None
    source_lines = source.splitlines()
    def_lines = get_def_lines_cached(file_path, lang=lang)
    if not def_lines:
        return None, None
    compute_context_window, extract_context_snippet = _get_context_helpers()
    context_window = compute_context_window(raw.line, def_lines, len(source_lines))
    context_snippet = extract_context_snippet(
        source_lines,
        context_window["start_line"],
        context_window["end_line"],
        match_line=raw.line,
    )
    return context_window, context_snippet


def _build_enriched_match(
    raw: RawMatch,
    match_text: str,
    classification: ClassificationResult,
    enrichment: MatchEnrichment,
    *,
    lang: QueryLanguage,
) -> EnrichedMatch:
    containing_scope = classification.containing_scope
    if containing_scope is None and enrichment.rust_tree_sitter is not None:
        impl_type = enrichment.rust_tree_sitter.get("impl_type")
        scope_name = enrichment.rust_tree_sitter.get("scope_name")
        if isinstance(impl_type, str) and isinstance(scope_name, str):
            containing_scope = f"{impl_type}::{scope_name}"
        elif isinstance(scope_name, str):
            containing_scope = scope_name

    return EnrichedMatch(
        span=raw.span,
        text=raw.text,
        match_text=match_text,
        category=classification.category,
        confidence=classification.confidence,
        evidence_kind=classification.evidence_kind,
        node_kind=classification.node_kind,
        containing_scope=containing_scope,
        context_window=enrichment.context_window,
        context_snippet=enrichment.context_snippet,
        symtable=enrichment.symtable,
        rust_tree_sitter=enrichment.rust_tree_sitter,
        python_enrichment=enrichment.python_enrichment,
        language=lang,
    )


def _raw_match_abs_byte_range(raw: RawMatch, source_bytes: bytes) -> tuple[int, int] | None:
    """Resolve absolute file-byte range for a raw line-relative match.

    Returns:
    -------
    tuple[int, int] | None
        Absolute start/end byte offsets when a range can be resolved.
    """
    abs_range = line_relative_byte_range_to_absolute(
        source_bytes,
        line=raw.line,
        byte_start=raw.match_byte_start,
        byte_end=raw.match_byte_end,
    )
    if abs_range is not None:
        return abs_range
    # Fallback: derive from line/column when line-relative byte data is missing.
    line_start_byte = line_relative_byte_range_to_absolute(
        source_bytes,
        line=raw.line,
        byte_start=0,
        byte_end=1,
    )
    if line_start_byte is None:
        return None
    start = line_start_byte[0] + max(0, raw.col)
    end = max(start + 1, start + max(1, raw.match_end - raw.match_start))
    return start, min(end, len(source_bytes))


def _resolve_python_node_context(
    file_path: Path,
    raw: RawMatch,
) -> ResolvedNodeContext | None:
    sg_root = get_sg_root(file_path, lang="python")
    if sg_root is None:
        return None
    index = get_node_index(file_path, sg_root, lang="python")
    node = index.find_containing(raw.line, raw.col)
    if node is None:
        return None
    return ResolvedNodeContext(
        sg_root=sg_root,
        node=node,
        line=raw.line,
        col=raw.col,
    )


def _maybe_rust_tree_sitter_enrichment(
    file_path: Path,
    raw: RawMatch,
    *,
    lang: QueryLanguage,
) -> dict[str, object] | None:
    if lang != "rust":
        return None
    source = get_cached_source(file_path)
    if source is None:
        return None
    source_bytes = source.encode("utf-8", errors="replace")
    abs_range = _raw_match_abs_byte_range(raw, source_bytes)
    if abs_range is None:
        return None
    byte_start, byte_end = abs_range
    try:
        payload = enrich_rust_context_by_byte_range(
            source,
            byte_start=byte_start,
            byte_end=byte_end,
            cache_key=str(file_path),
        )
        return normalize_rust_payload(payload)
    except _RUST_ENRICHMENT_ERRORS:
        return None


def _maybe_python_enrichment(
    file_path: Path,
    raw: RawMatch,
    *,
    lang: QueryLanguage,
    resolved_python: ResolvedNodeContext | None = None,
) -> dict[str, object] | None:
    """Attempt Python context enrichment for a match.

    Parameters
    ----------
    file_path
        Path to the source file.
    raw
        Raw match from ripgrep.
    lang
        Query language.

    Returns:
    -------
    dict[str, object] | None
        Enrichment payload, or None if not applicable.
    """
    if lang != "python":
        return None
    sg_root = (
        resolved_python.sg_root
        if resolved_python is not None
        else get_sg_root(file_path, lang=lang)
    )
    if sg_root is None:
        return None
    source = get_cached_source(file_path)
    if source is None:
        return None
    session = get_python_analysis_session(file_path, source, sg_root=sg_root)
    source_bytes = source.encode("utf-8", errors="replace")
    abs_range = _raw_match_abs_byte_range(raw, source_bytes)
    if abs_range is None:
        return None
    byte_start, byte_end = abs_range
    try:
        payload = enrich_python_context_by_byte_range(
            PythonByteRangeEnrichmentRequest(
                sg_root=sg_root,
                source_bytes=source_bytes,
                byte_start=byte_start,
                byte_end=byte_end,
                cache_key=str(file_path),
                resolved_node=resolved_python.node if resolved_python is not None else None,
                resolved_line=resolved_python.line if resolved_python is not None else None,
                resolved_col=resolved_python.col if resolved_python is not None else None,
                session=session,
            )
        )
        return normalize_python_payload(payload)
    except _PYTHON_ENRICHMENT_ERRORS:
        return None


def _classify_file_role(file_path: str) -> str:
    """Classify file role for ranking.

    Parameters
    ----------
    file_path
        Relative file path.

    Returns:
    -------
    str
        One of "src", "test", "doc", "lib", "other".
    """
    path_lower = file_path.lower()

    if "/test" in path_lower or "test_" in path_lower or "_test.py" in path_lower:
        return "test"
    if "/doc" in path_lower or "/docs/" in path_lower:
        return "doc"
    if "/vendor/" in path_lower or "/third_party/" in path_lower:
        return "lib"
    if "/src/" in path_lower or "/lib/" in path_lower:
        return "src"

    return "other"


def compute_relevance_score(match: EnrichedMatch) -> float:
    """Compute relevance score for ranking.

    Parameters
    ----------
    match
        Enriched match to score.

    Returns:
    -------
    float
        Relevance score (higher is better).
    """
    # Base weight from category
    base = KIND_WEIGHTS.get(match.category, 0.3)

    # File role multiplier
    role = _classify_file_role(match.file)
    role_mult = {
        "src": 1.0,
        "lib": 0.9,
        "other": 0.7,
        "test": 0.5,
        "doc": 0.3,
    }.get(role, 0.7)

    # Path depth penalty (prefer shallow paths)
    depth = match.file.count("/")
    depth_penalty = min(0.2, depth * 0.02)

    # Confidence factor
    conf_factor = match.confidence

    return base * role_mult * conf_factor - depth_penalty


def _evidence_to_bucket(evidence_kind: str) -> str:
    """Map evidence kind to confidence bucket.

    Parameters
    ----------
    evidence_kind
        Evidence kind string.

    Returns:
    -------
    str
        Confidence bucket name.
    """
    return {
        "resolved_ast": "high",
        "resolved_ast_record": "high",
        "resolved_ast_heuristic": "medium",
        "heuristic": "medium",
        "rg_only": "low",
    }.get(evidence_kind, "medium")


def _category_message(category: MatchCategory, match: EnrichedMatch) -> str:
    """Generate human-readable message for category.

    Parameters
    ----------
    category
        Match category.
    match
        Enriched match.

    Returns:
    -------
    str
        Human-readable message.
    """
    messages = {
        "definition": "Function/class definition",
        "callsite": "Function call",
        "import": "Import statement",
        "from_import": "From import",
        "reference": "Reference",
        "assignment": "Assignment",
        "annotation": "Type annotation",
        "docstring_match": "Match in docstring",
        "comment_match": "Match in comment",
        "string_match": "Match in string literal",
        "text_match": "Text match",
    }
    base = messages.get(category, "Match")
    if match.containing_scope:
        return f"{base} in {match.containing_scope}"
    return base


def build_finding(match: EnrichedMatch, _root: Path) -> Finding:
    """Convert EnrichedMatch to Finding.

    Used by the smart search pipeline to emit standardized findings.

    Parameters
    ----------
    match
        Enriched match.
    _root
        Repository root (unused; kept for interface compatibility).

    Returns:
    -------
    Finding
        Finding object.
    """
    score = _build_score_details(match)
    data = _build_match_data(match)
    details = DetailPayload(kind=match.category, score=score, data=data)
    return Finding(
        category=match.category,
        message=_category_message(match.category, match),
        anchor=Anchor.from_span(match.span),
        severity="info",
        details=details,
    )


def _build_score_details(match: EnrichedMatch) -> ScoreDetails:
    return ScoreDetails(
        confidence_score=match.confidence,
        confidence_bucket=_evidence_to_bucket(match.evidence_kind),
        evidence_kind=match.evidence_kind,
    )


def _build_match_data(match: EnrichedMatch) -> dict[str, object]:
    data: dict[str, object] = {
        "match_text": match.match_text,
        "language": match.language,
    }
    # Rec 10: Only include line_text when context_snippet is absent
    if not match.context_snippet:
        data["line_text"] = match.text
    _populate_optional_fields(data, match)
    _merge_enrichment_payloads(data, match)
    return data


def _populate_optional_fields(data: dict[str, object], match: EnrichedMatch) -> None:
    """Add optional context fields to the match data dict.

    Parameters
    ----------
    data
        Target dict (mutated in place).
    match
        Enriched match to extract fields from.
    """
    if match.context_window:
        data["context_window"] = match.context_window
    if match.context_snippet:
        data["context_snippet"] = match.context_snippet
    if match.containing_scope:
        data["containing_scope"] = match.containing_scope
    if match.node_kind:
        data["node_kind"] = match.node_kind
    if match.symtable:
        flags = _symtable_flags(match.symtable)
        if flags:
            data["binding_flags"] = flags


def _merge_enrichment_payloads(data: dict[str, object], match: EnrichedMatch) -> None:
    """Merge language-specific enrichment payloads into the data dict.

    Parameters
    ----------
    data
        Target dict (mutated in place).
    match
        Enriched match with optional enrichment payloads.
    """
    enrichment: dict[str, object] = {"language": match.language}
    if match.rust_tree_sitter:
        enrichment["rust"] = match.rust_tree_sitter
    if match.python_enrichment:
        enrichment["python"] = match.python_enrichment
    if match.symtable:
        enrichment["symtable"] = match.symtable
    if len(enrichment) > 1:
        data["enrichment"] = enrichment


def _symtable_flags(symtable: SymtableEnrichment) -> list[str]:
    flags: list[str] = []
    if symtable.is_imported:
        flags.append("imported")
    if symtable.is_assigned:
        flags.append("assigned")
    if symtable.is_parameter:
        flags.append("parameter")
    if symtable.is_free:
        flags.append("closure_var")
    if symtable.is_global:
        flags.append("global")
    if symtable.is_referenced:
        flags.append("referenced")
    if symtable.is_local:
        flags.append("local")
    if symtable.is_nonlocal:
        flags.append("nonlocal")
    return flags


def build_followups(
    matches: list[EnrichedMatch],
    query: str,
    mode: QueryMode,
) -> list[Finding]:
    """Generate actionable next commands.

    Parameters
    ----------
    matches
        List of enriched matches.
    query
        Original search query.
    mode
        Query mode.

    Returns:
    -------
    list[Finding]
        Follow-up suggestions.
    """
    findings: list[Finding] = []

    if mode == QueryMode.IDENTIFIER:
        defs = [m for m in matches if m.category == "definition"]
        if defs:
            findings.append(
                Finding(
                    category="next_step",
                    message=f"Find callers: /cq calls {query}",
                    severity="info",
                    details=DetailPayload(
                        kind="next_step",
                        data={"cmd": f"/cq calls {query}"},
                    ),
                )
            )
            findings.append(
                Finding(
                    category="next_step",
                    message=f'Find definitions: /cq q "entity=function name={query}"',
                    severity="info",
                    details=DetailPayload(
                        kind="next_step",
                        data={"cmd": f'/cq q "entity=function name={query}"'},
                    ),
                )
            )
            findings.append(
                Finding(
                    category="next_step",
                    message=f'Find callers (transitive): /cq q "entity=function name={query} expand=callers(depth=2)"',
                    severity="info",
                    details=DetailPayload(
                        kind="next_step",
                        data={
                            "cmd": f'/cq q "entity=function name={query} expand=callers(depth=2)"'
                        },
                    ),
                )
            )

        calls = [m for m in matches if m.category == "callsite"]
        if calls:
            findings.append(
                Finding(
                    category="next_step",
                    message=f"Analyze impact: /cq impact {query}",
                    severity="info",
                    details=DetailPayload(
                        kind="next_step",
                        data={"cmd": f"/cq impact {query}"},
                    ),
                )
            )

    return findings


_SUMMARY_ARGS_COUNT = 5
_SUMMARY_KWARGS_ERROR = "build_summary does not accept kwargs when using SearchSummaryInputs."
_SUMMARY_ARITY_ERROR = "build_summary expects (query, mode, stats, matches, limits)."


def build_summary(*args: object, **kwargs: object) -> dict[str, object]:
    """Build summary dict for CqResult.

    Accepts either a ``SearchSummaryInputs`` instance or the legacy positional
    arguments (query, mode, stats, matches, limits) with keyword overrides.

    Returns:
    -------
    dict[str, object]
        Summary dictionary.
    """
    inputs = _coerce_summary_inputs(args, kwargs)
    return _build_summary(inputs)


def _coerce_summary_inputs(
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> SearchSummaryInputs:
    if len(args) == 1 and isinstance(args[0], SearchSummaryInputs):
        if kwargs:
            msg = _SUMMARY_KWARGS_ERROR
            raise TypeError(msg)
        return args[0]
    if len(args) != _SUMMARY_ARGS_COUNT:
        msg = _SUMMARY_ARITY_ERROR
        raise TypeError(msg)
    query, mode, stats, matches, limits = args
    config = SearchConfig(
        root=Path(),
        query=cast("str", query),
        mode=cast("QueryMode", mode),
        lang_scope=cast(
            "QueryLanguageScope",
            kwargs.get("lang_scope", DEFAULT_QUERY_LANGUAGE_SCOPE),
        ),
        limits=cast("SearchLimits", limits),
        include_globs=cast("list[str] | None", kwargs.get("include")),
        exclude_globs=cast("list[str] | None", kwargs.get("exclude")),
        include_strings=False,
        argv=[],
        tc=None,
        started_ms=0.0,
    )
    languages = tuple(expand_language_scope(config.lang_scope))
    default_stats = cast("SearchStats", stats)
    return SearchSummaryInputs(
        config=config,
        stats=default_stats,
        matches=cast("list[EnrichedMatch]", matches),
        languages=languages,
        language_stats=dict.fromkeys(languages, default_stats),
        file_globs=cast("list[str] | None", kwargs.get("file_globs")),
        limit=cast("int | None", kwargs.get("limit")),
        pattern=cast("str | None", kwargs.get("pattern")),
    )


def _build_summary(inputs: SearchSummaryInputs) -> dict[str, object]:
    config = inputs.config
    language_stats: dict[QueryLanguage, dict[str, object]] = {
        lang: {
            "scanned_files": stat.scanned_files,
            "scanned_files_is_estimate": stat.scanned_files_is_estimate,
            "matched_files": stat.matched_files,
            "total_matches": stat.total_matches,
            "timed_out": stat.timed_out,
            "truncated": stat.truncated,
            "caps_hit": (
                "timeout"
                if stat.timed_out
                else (
                    "max_files"
                    if stat.max_files_hit
                    else ("max_total_matches" if stat.max_matches_hit else "none")
                )
            ),
        }
        for lang, stat in inputs.language_stats.items()
    }
    common = {
        "query": config.query,
        "mode": config.mode.value,
        "file_globs": inputs.file_globs or file_globs_for_scope(config.lang_scope),
        "include": config.include_globs or [],
        "exclude": config.exclude_globs or [],
        "context_lines": {"before": 1, "after": 1},
        "limit": inputs.limit if inputs.limit is not None else config.limits.max_total_matches,
        "scanned_files": inputs.stats.scanned_files,
        "scanned_files_is_estimate": inputs.stats.scanned_files_is_estimate,
        "matched_files": inputs.stats.matched_files,
        "total_matches": inputs.stats.total_matches,
        "returned_matches": len(inputs.matches),
        "scan_method": "hybrid",
        "pattern": inputs.pattern,
        "case_sensitive": True,
        "caps_hit": (
            "timeout"
            if inputs.stats.timed_out
            else (
                "max_files"
                if inputs.stats.max_files_hit
                else ("max_total_matches" if inputs.stats.max_matches_hit else "none")
            )
        ),
        "truncated": inputs.stats.truncated,
        "timed_out": inputs.stats.timed_out,
    }
    return build_multilang_summary(
        SummaryBuildRequest(
            common=common,
            lang_scope=config.lang_scope,
            language_order=inputs.languages,
            languages=language_stats,
            cross_language_diagnostics=[],
            language_capabilities=build_language_capabilities(lang_scope=config.lang_scope),
        )
    )


def build_sections(
    matches: list[EnrichedMatch],
    root: Path,
    query: str,
    mode: QueryMode,
    *,
    include_strings: bool = False,
) -> list[Section]:
    """Build organized sections for CqResult.

    Parameters
    ----------
    matches
        List of enriched matches.
    root
        Repository root.
    query
        Original query.
    mode
        Query mode.
    include_strings
        Include string/comment/docstring matches.

    Returns:
    -------
    list[Section]
        Organized sections.
    """
    non_code_categories: set[MatchCategory] = {
        "docstring_match",
        "comment_match",
        "string_match",
    }
    sorted_matches = sorted(matches, key=compute_relevance_score, reverse=True)
    visible_matches = _filter_visible_matches(
        sorted_matches,
        include_strings=include_strings,
        non_code_categories=non_code_categories,
    )

    sections: list[Section] = [
        _build_top_contexts_section(visible_matches, root),
    ]
    sections.extend(_build_identifier_sections(visible_matches, root, mode))
    sections.append(_build_kind_counts_section(matches))

    non_code_section = _build_non_code_section(sorted_matches, root, non_code_categories)
    if non_code_section is not None:
        sections.append(non_code_section)

    sections.append(_build_hot_files_section(matches))

    followups_section = _build_followups_section(matches, query, mode)
    if followups_section is not None:
        sections.append(followups_section)

    return sections


def _filter_visible_matches(
    sorted_matches: list[EnrichedMatch],
    *,
    include_strings: bool,
    non_code_categories: set[MatchCategory],
) -> list[EnrichedMatch]:
    if include_strings:
        return sorted_matches
    return [m for m in sorted_matches if m.category not in non_code_categories]


def _group_matches_by_context(
    matches: list[EnrichedMatch],
) -> dict[str, list[EnrichedMatch]]:
    grouped: dict[str, list[EnrichedMatch]] = {}
    for match in matches:
        key = f"{match.containing_scope} ({match.file})" if match.containing_scope else match.file
        grouped.setdefault(key, []).append(match)
    return grouped


def _build_top_contexts_section(
    matches: list[EnrichedMatch],
    root: Path,
) -> Section:
    grouped = _group_matches_by_context(matches)
    group_scores = [
        (key, max(compute_relevance_score(m) for m in group), group)
        for key, group in grouped.items()
    ]
    group_scores.sort(key=lambda t: t[1], reverse=True)
    top_contexts: list[Finding] = []
    for key, _score, group in group_scores[:20]:
        rep = group[0]
        finding = build_finding(rep, root)
        finding.message = f"{key}"
        top_contexts.append(finding)
    return Section(title="Top Contexts", findings=top_contexts)


def _build_identifier_sections(
    matches: list[EnrichedMatch],
    root: Path,
    mode: QueryMode,
) -> list[Section]:
    if mode != QueryMode.IDENTIFIER:
        return []
    sections: list[Section] = []
    defs = [m for m in matches if m.category == "definition"]
    imps = [m for m in matches if m.category in {"import", "from_import"}]
    calls = [m for m in matches if m.category == "callsite"]
    if defs:
        sections.append(
            Section(title="Definitions", findings=[build_finding(m, root) for m in defs[:5]])
        )
    if imps:
        sections.append(
            Section(
                title="Imports",
                findings=[build_finding(m, root) for m in imps[:10]],
                collapsed=True,
            )
        )
    if calls:
        sections.append(
            Section(
                title="Callsites",
                findings=[build_finding(m, root) for m in calls[:10]],
                collapsed=True,
            )
        )
    return sections


def _build_kind_counts_section(matches: list[EnrichedMatch]) -> Section:
    category_counts = Counter(m.category for m in matches)
    kind_findings = [
        Finding(
            category="count",
            message=f"{cat}: {count}",
            severity="info",
            details=DetailPayload(kind="count", data={"category": cat, "count": count}),
        )
        for cat, count in category_counts.most_common()
    ]
    return Section(title="Uses by Kind", findings=kind_findings, collapsed=True)


def _build_non_code_section(
    matches: list[EnrichedMatch],
    root: Path,
    non_code_categories: set[MatchCategory],
) -> Section | None:
    non_code = [m for m in matches if m.category in non_code_categories]
    if not non_code:
        return None
    return Section(
        title="Non-Code Matches (Strings / Comments / Docstrings)",
        findings=[build_finding(m, root) for m in non_code[:20]],
        collapsed=True,
    )


def _build_hot_files_section(matches: list[EnrichedMatch]) -> Section:
    file_counts = Counter(m.file for m in matches)
    hot_file_findings = [
        Finding(
            category="hot_file",
            message=f"{file}: {count} matches",
            anchor=Anchor(file=file, line=1),
            severity="info",
            details=DetailPayload(kind="hot_file", data={"count": count}),
        )
        for file, count in file_counts.most_common(10)
    ]
    return Section(title="Hot Files", findings=hot_file_findings, collapsed=True)


def _build_followups_section(
    matches: list[EnrichedMatch],
    query: str,
    mode: QueryMode,
) -> Section | None:
    followup_findings = build_followups(matches, query, mode)
    if not followup_findings:
        return None
    return Section(title="Suggested Follow-ups", findings=followup_findings)


def _build_search_context(
    root: Path,
    query: str,
    kwargs: SearchKwargs,
) -> SmartSearchContext:
    request = SearchRequest(
        root=root,
        query=query,
        mode=kwargs.get("mode"),
        lang_scope=kwargs.get("lang_scope", DEFAULT_QUERY_LANGUAGE_SCOPE),
        include_globs=kwargs.get("include_globs"),
        exclude_globs=kwargs.get("exclude_globs"),
        include_strings=bool(kwargs.get("include_strings", False)),
        limits=kwargs.get("limits"),
        tc=kwargs.get("tc"),
        argv=kwargs.get("argv"),
        started_ms=kwargs.get("started_ms"),
    )
    started = request.started_ms
    if started is None:
        started = ms()
    limits = request.limits or SMART_SEARCH_LIMITS
    argv = request.argv or ["search", query]

    # Clear caches from previous runs
    clear_caches()

    actual_mode = detect_query_mode(request.query, force_mode=request.mode)
    return SearchConfig(
        root=request.root,
        query=request.query,
        mode=actual_mode,
        lang_scope=request.lang_scope,
        limits=limits,
        include_globs=request.include_globs,
        exclude_globs=request.exclude_globs,
        include_strings=request.include_strings,
        argv=argv,
        tc=request.tc,
        started_ms=started,
    )


def _run_candidate_phase(
    ctx: SmartSearchContext,
    *,
    lang: QueryLanguage,
) -> tuple[list[RawMatch], SearchStats, str]:
    pattern = rf"\b{re.escape(ctx.query)}\b" if ctx.mode == QueryMode.IDENTIFIER else ctx.query
    raw_matches, stats = collect_candidates(
        CandidateCollectionRequest(
            root=ctx.root,
            pattern=pattern,
            mode=ctx.mode,
            limits=ctx.limits,
            lang=lang,
            include_globs=ctx.include_globs,
            exclude_globs=ctx.exclude_globs,
        )
    )
    return raw_matches, stats, pattern


def _run_classification_phase(
    ctx: SmartSearchContext,
    *,
    lang: QueryLanguage,
    raw_matches: list[RawMatch],
) -> list[EnrichedMatch]:
    filtered_raw_matches = [m for m in raw_matches if is_path_in_lang_scope(m.file, lang)]
    if not filtered_raw_matches:
        return []

    indexed: list[tuple[int, RawMatch]] = list(enumerate(filtered_raw_matches))
    partitioned: dict[str, list[tuple[int, RawMatch]]] = {}
    for idx, raw in indexed:
        partitioned.setdefault(raw.file, []).append((idx, raw))
    batches = list(partitioned.values())
    workers = _resolve_search_worker_count(len(batches))

    if workers <= 1 or len(batches) <= 1:
        return [classify_match(raw, ctx.root, lang=lang) for raw in filtered_raw_matches]

    tasks = [(str(ctx.root), lang, batch) for batch in batches]
    try:
        with ProcessPoolExecutor(
            max_workers=workers,
            mp_context=multiprocessing.get_context("spawn"),
        ) as pool:
            indexed_results: list[tuple[int, EnrichedMatch]] = []
            for batch_results in pool.map(_classify_partition_batch, tasks):
                indexed_results.extend(batch_results)
    except Exception:  # noqa: BLE001 - fail-open to sequential classification
        return [classify_match(raw, ctx.root, lang=lang) for raw in filtered_raw_matches]

    indexed_results.sort(key=lambda pair: pair[0])
    return [match for _idx, match in indexed_results]


def _resolve_search_worker_count(partition_count: int) -> int:
    if partition_count <= 1:
        return 1
    return min(partition_count, MAX_SEARCH_CLASSIFY_WORKERS)


def _classify_partition_batch(
    task: tuple[str, QueryLanguage, list[tuple[int, RawMatch]]],
) -> list[tuple[int, EnrichedMatch]]:
    root_str, lang, batch = task
    root = Path(root_str)
    return [(idx, classify_match(raw, root, lang=lang)) for idx, raw in batch]


@dataclass(frozen=True, slots=True)
class _LanguageSearchResult:
    lang: QueryLanguage
    raw_matches: list[RawMatch]
    stats: SearchStats
    pattern: str
    enriched_matches: list[EnrichedMatch]
    dropped_by_scope: int


def _run_language_partitions(ctx: SmartSearchContext) -> list[_LanguageSearchResult]:
    by_lang = execute_by_language_scope(
        ctx.lang_scope,
        lambda lang: _run_single_partition(ctx, lang),
    )
    return [by_lang[lang] for lang in expand_language_scope(ctx.lang_scope)]


def _run_single_partition(
    ctx: SmartSearchContext,
    lang: QueryLanguage,
) -> _LanguageSearchResult:
    raw_matches, stats, pattern = _run_candidate_phase(ctx, lang=lang)
    enriched = _run_classification_phase(ctx, lang=lang, raw_matches=raw_matches)
    return _LanguageSearchResult(
        lang=lang,
        raw_matches=raw_matches,
        stats=stats,
        pattern=pattern,
        enriched_matches=enriched,
        dropped_by_scope=stats.dropped_by_scope,
    )


def _merge_language_matches(
    *,
    partition_results: list[_LanguageSearchResult],
    lang_scope: QueryLanguageScope,
) -> list[EnrichedMatch]:
    partitions: dict[QueryLanguage, list[EnrichedMatch]] = {}
    for partition in partition_results:
        partitions.setdefault(partition.lang, []).extend(partition.enriched_matches)
    return merge_partitioned_items(
        partitions=partitions,
        scope=lang_scope,
        get_language=lambda match: match.language,
        get_score=compute_relevance_score,
        get_location=lambda match: (match.file, match.line, match.col),
    )


def _build_cross_language_diagnostics_for_search(
    *,
    query: str,
    lang_scope: QueryLanguageScope,
    python_matches: int,
    rust_matches: int,
) -> list[Finding]:
    return build_cross_language_diagnostics(
        lang_scope=lang_scope,
        python_matches=python_matches,
        rust_matches=rust_matches,
        python_oriented=is_python_oriented_query_text(query),
    )


def _build_capability_diagnostics_for_search(
    *,
    lang_scope: QueryLanguageScope,
) -> list[Finding]:
    from tools.cq.search.multilang_diagnostics import build_capability_diagnostics

    return build_capability_diagnostics(
        features=["pattern_query"],
        lang_scope=lang_scope,
    )


def _status_from_enrichment(payload: dict[str, object] | None) -> str:
    if payload is None:
        return "skipped"
    meta = payload.get("meta")
    status = None
    if isinstance(meta, dict):
        status = meta.get("enrichment_status")
    if status is None:
        status = payload.get("enrichment_status")
    if status in {"applied", "degraded", "skipped"}:
        return cast("str", status)
    return "applied"


def _empty_enrichment_telemetry() -> dict[str, object]:
    return {
        "python": {
            "applied": 0,
            "degraded": 0,
            "skipped": 0,
            "stages": {
                "ast_grep": {"applied": 0, "degraded": 0, "skipped": 0},
                "python_ast": {"applied": 0, "degraded": 0, "skipped": 0},
                "import_detail": {"applied": 0, "degraded": 0, "skipped": 0},
                "libcst": {"applied": 0, "degraded": 0, "skipped": 0},
                "tree_sitter": {"applied": 0, "degraded": 0, "skipped": 0},
            },
            "timings_ms": {
                "ast_grep": 0.0,
                "python_ast": 0.0,
                "import_detail": 0.0,
                "libcst": 0.0,
                "tree_sitter": 0.0,
            },
        },
        "rust": {"applied": 0, "degraded": 0, "skipped": 0},
    }


def _accumulate_stage_status(
    stages_bucket: dict[str, object], stage_status: dict[str, object]
) -> None:
    for stage, stage_state in stage_status.items():
        if not isinstance(stage, str) or not isinstance(stage_state, str):
            continue
        stage_bucket = stages_bucket.get(stage)
        if isinstance(stage_bucket, dict) and stage_state in {"applied", "degraded", "skipped"}:
            stage_bucket[stage_state] = cast("int", stage_bucket.get(stage_state, 0)) + 1


def _accumulate_stage_timings(
    timings_bucket: dict[str, object],
    stage_timings: dict[str, object],
) -> None:
    for stage, stage_ms in stage_timings.items():
        if isinstance(stage, str) and isinstance(stage_ms, (int, float)):
            existing = timings_bucket.get(stage)
            base_ms = float(existing) if isinstance(existing, (int, float)) else 0.0
            timings_bucket[stage] = base_ms + float(stage_ms)


def _accumulate_python_enrichment(
    lang_bucket: dict[str, object], payload: dict[str, object]
) -> None:
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        return
    stage_status = meta.get("stage_status")
    stages_bucket = lang_bucket.get("stages")
    if isinstance(stage_status, dict) and isinstance(stages_bucket, dict):
        _accumulate_stage_status(stages_bucket, stage_status)
    stage_timings = meta.get("stage_timings_ms")
    timings_bucket = lang_bucket.get("timings_ms")
    if isinstance(stage_timings, dict) and isinstance(timings_bucket, dict):
        _accumulate_stage_timings(timings_bucket, stage_timings)


def _attach_enrichment_cache_stats(telemetry: dict[str, object]) -> None:
    rust_bucket = telemetry.get("rust")
    if isinstance(rust_bucket, dict):
        rust_bucket.update(get_tree_sitter_rust_cache_stats())
    python_bucket = telemetry.get("python")
    if isinstance(python_bucket, dict):
        python_bucket["tree_sitter_cache"] = get_tree_sitter_python_cache_stats()


def _build_enrichment_telemetry(matches: list[EnrichedMatch]) -> dict[str, object]:
    """Build additive observability stats for enrichment stages.

    Returns:
    -------
    dict[str, object]
        Per-language enrichment status counters and Rust cache metrics.
    """
    telemetry: dict[str, object] = _empty_enrichment_telemetry()
    for match in matches:
        lang_bucket = telemetry.get(match.language)
        if not isinstance(lang_bucket, dict):
            continue
        payload = match.python_enrichment if match.language == "python" else match.rust_tree_sitter
        status = _status_from_enrichment(payload)
        lang_bucket[status] = cast("int", lang_bucket.get(status, 0)) + 1
        if match.language == "python" and isinstance(payload, dict):
            _accumulate_python_enrichment(lang_bucket, payload)

    _attach_enrichment_cache_stats(telemetry)
    return telemetry


def _assemble_smart_search_result(
    ctx: SmartSearchContext,
    partition_results: list[_LanguageSearchResult],
) -> CqResult:
    enriched_matches = _merge_language_matches(
        partition_results=partition_results,
        lang_scope=ctx.lang_scope,
    )
    language_stats: dict[QueryLanguage, SearchStats] = {
        result.lang: result.stats for result in partition_results
    }
    patterns = {result.lang: result.pattern for result in partition_results}
    merged_stats = SearchStats(
        scanned_files=sum(stat.scanned_files for stat in language_stats.values()),
        matched_files=sum(stat.matched_files for stat in language_stats.values()),
        total_matches=sum(stat.total_matches for stat in language_stats.values()),
        scanned_files_is_estimate=any(
            stat.scanned_files_is_estimate for stat in language_stats.values()
        ),
        truncated=any(stat.truncated for stat in language_stats.values()),
        timed_out=any(stat.timed_out for stat in language_stats.values()),
        max_files_hit=any(stat.max_files_hit for stat in language_stats.values()),
        max_matches_hit=any(stat.max_matches_hit for stat in language_stats.values()),
    )
    summary_inputs = SearchSummaryInputs(
        config=ctx,
        stats=merged_stats,
        matches=enriched_matches,
        languages=tuple(expand_language_scope(ctx.lang_scope)),
        language_stats=language_stats,
        file_globs=file_globs_for_scope(ctx.lang_scope),
        limit=ctx.limits.max_total_matches,
        pattern=patterns.get("python") or next(iter(patterns.values()), None),
    )
    summary = build_summary(summary_inputs)
    dropped_by_scope = {
        result.lang: result.dropped_by_scope
        for result in partition_results
        if result.dropped_by_scope > 0
    }
    python_matches = sum(1 for match in enriched_matches if match.language == "python")
    rust_matches = sum(1 for match in enriched_matches if match.language == "rust")
    diagnostics = _build_cross_language_diagnostics_for_search(
        query=ctx.query,
        lang_scope=ctx.lang_scope,
        python_matches=python_matches,
        rust_matches=rust_matches,
    )
    capability_diagnostics = _build_capability_diagnostics_for_search(
        lang_scope=ctx.lang_scope,
    )
    all_diagnostics = diagnostics + capability_diagnostics
    summary["cross_language_diagnostics"] = diagnostics_to_summary_payload(all_diagnostics)
    summary["language_capabilities"] = build_language_capabilities(lang_scope=ctx.lang_scope)
    summary["enrichment_telemetry"] = _build_enrichment_telemetry(enriched_matches)
    if dropped_by_scope:
        summary["dropped_by_scope"] = dropped_by_scope
    assert_multilang_summary(summary)
    sections = build_sections(
        enriched_matches,
        ctx.root,
        ctx.query,
        ctx.mode,
        include_strings=ctx.include_strings,
    )
    if all_diagnostics:
        sections.append(Section(title="Cross-Language Diagnostics", findings=all_diagnostics))

    run_ctx = RunContext.from_parts(
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.tc,
        started_ms=ctx.started_ms,
    )
    run = run_ctx.to_runmeta("search")

    return CqResult(
        run=run,
        summary=summary,
        sections=sections,
        key_findings=(sections[0].findings[:5] if sections else []) + all_diagnostics,
        evidence=[build_finding(m, ctx.root) for m in enriched_matches[:MAX_EVIDENCE]],
    )


def smart_search(
    root: Path,
    query: str,
    **kwargs: Unpack[SearchKwargs],
) -> CqResult:
    """Execute Smart Search pipeline.

    Parameters
    ----------
    root
        Repository root path.
    query
        Search query string.
    kwargs
        Optional overrides: mode, include_globs, exclude_globs, include_strings,
        limits, tc, argv.

    Returns:
    -------
    CqResult
        Complete search results.
    """
    ctx = _build_search_context(root, query, kwargs)
    partition_results = _run_language_partitions(ctx)
    return _assemble_smart_search_result(ctx, partition_results)


__all__ = [
    "KIND_WEIGHTS",
    "SMART_SEARCH_LIMITS",
    "EnrichedMatch",
    "RawMatch",
    "SearchStats",
    "SmartSearchContext",
    "build_candidate_searcher",
    "build_finding",
    "build_followups",
    "build_sections",
    "build_summary",
    "classify_match",
    "collect_candidates",
    "compute_relevance_score",
    "smart_search",
]
