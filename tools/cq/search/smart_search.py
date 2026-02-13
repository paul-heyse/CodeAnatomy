"""Smart Search pipeline for semantically-enriched code search."""

from __future__ import annotations

import multiprocessing
import re
from collections import Counter
from concurrent.futures import Future
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from pathlib import Path
from typing import TYPE_CHECKING, cast

import msgspec

from tools.cq.core.cache import build_cache_key, build_cache_tag, get_cq_cache_backend
from tools.cq.core.cache.contracts import SearchPartitionCacheV1
from tools.cq.core.contracts import contract_to_builtins, require_mapping
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
from tools.cq.core.public_serialization import to_public_dict, to_public_list
from tools.cq.core.requests import SummaryBuildRequest
from tools.cq.core.run_context import RunContext
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
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
    constrain_include_globs_for_language,
    expand_language_scope,
    file_globs_for_scope,
    is_path_in_lang_scope,
    language_extension_exclude_globs,
    ripgrep_type_for_language,
    ripgrep_types_for_scope,
)
from tools.cq.search.candidate_normalizer import build_definition_candidate_finding
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
from tools.cq.search.context_window import (
    compute_search_context_window,
    extract_search_context_snippet,
)
from tools.cq.search.contracts import (
    coerce_pyrefly_diagnostics,
    coerce_pyrefly_overview,
    coerce_pyrefly_telemetry,
)
from tools.cq.search.enrichment.core import normalize_python_payload, normalize_rust_payload
from tools.cq.search.lsp_contract_state import (
    LspContractStateInputV1,
    LspContractStateV1,
    derive_lsp_contract_state,
)
from tools.cq.search.lsp_front_door_adapter import (
    LanguageLspEnrichmentRequest,
    enrich_with_language_lsp,
    infer_language_for_path,
    lsp_runtime_enabled,
    provider_for_language,
)
from tools.cq.search.models import (
    CandidateSearchRequest,
    SearchConfig,
    SearchRequest,
)
from tools.cq.search.multilang_diagnostics import (
    build_cross_language_diagnostics,
    build_language_capabilities,
    diagnostics_to_summary_payload,
    is_python_oriented_query_text,
)
from tools.cq.search.pipeline import SearchPipeline
from tools.cq.search.profiles import INTERACTIVE, SearchLimits
from tools.cq.search.pyrefly_signal import evaluate_pyrefly_signal_from_mapping
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
from tools.cq.search.section_builder import (
    insert_neighborhood_preview,
    insert_target_candidates,
)
from tools.cq.search.tree_sitter_python import get_tree_sitter_python_cache_stats
from tools.cq.search.tree_sitter_rust import get_tree_sitter_rust_cache_stats

if TYPE_CHECKING:
    from ast_grep_py import SgNode, SgRoot

    from tools.cq.core.front_door_insight import (
        FrontDoorInsightV1,
        InsightNeighborhoodV1,
        InsightRiskV1,
    )
    from tools.cq.core.toolchain import Toolchain

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
MAX_TARGET_CANDIDATES = 3
_RUST_ENRICHMENT_ERRORS = (RuntimeError, TypeError, ValueError, AttributeError, UnicodeError)
_PYREFLY_PREFETCH_NON_FATAL_EXCEPTIONS = (
    OSError,
    RuntimeError,
    TimeoutError,
    TypeError,
    ValueError,
)
MAX_SEARCH_CLASSIFY_WORKERS = 4
MAX_PYREFLY_ENRICH_FINDINGS = 8
_PyreflyAnchorKey = tuple[str, int, int, str]


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
    pyrefly_enrichment: dict[str, object] | None


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
    pyrefly_enrichment: dict[str, object] | None = None
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
    *,
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE,
    globs: tuple[list[str] | None, list[str] | None] | None = None,
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
        lang_scope=lang_scope,
        include_globs=globs[0] if globs is not None else None,
        exclude_globs=globs[1] if globs is not None else None,
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
    enable_pyrefly: bool = False,
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
    enable_pyrefly
        Whether to attempt per-anchor Pyrefly LSP enrichment in this path.

    Returns:
    -------
    EnrichedMatch
        Fully classified and enriched match.
    """
    match_text = raw.match_text

    # Stage 1: Fast heuristic
    heuristic = classify_heuristic(raw.text, raw.col, match_text)

    if heuristic.skip_deeper and heuristic.category is not None and not force_semantic_enrichment:
        file_path = root / raw.file
        context_window, context_snippet = _build_context_enrichment(file_path, raw, lang=lang)
        return _build_heuristic_enriched(
            raw,
            heuristic,
            match_text,
            lang=lang,
            context_window=context_window,
            context_snippet=context_snippet,
        )

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
    pyrefly_enrichment = _maybe_pyrefly_enrichment(
        root,
        file_path,
        raw,
        lang=lang,
        enable_pyrefly=enable_pyrefly,
    )
    context_window, context_snippet = _build_context_enrichment(file_path, raw, lang=lang)
    enrichment = MatchEnrichment(
        symtable=symtable_enrichment,
        context_window=context_window,
        context_snippet=context_snippet,
        rust_tree_sitter=rust_tree_sitter,
        python_enrichment=python_enrichment,
        pyrefly_enrichment=pyrefly_enrichment,
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
    context_window: dict[str, int] | None = None,
    context_snippet: str | None = None,
) -> EnrichedMatch:
    category = heuristic.category or "text_match"
    return EnrichedMatch(
        span=raw.span,
        text=raw.text,
        match_text=match_text,
        category=category,
        confidence=heuristic.confidence,
        evidence_kind="heuristic",
        context_window=context_window,
        context_snippet=context_snippet,
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

    ast_result = _classify_from_node(
        file_path,
        raw,
        lang=lang,
        resolved_python=resolved_python,
    )
    if ast_result is None:
        ast_result = classify_from_records(file_path, root, raw.line, raw.col, lang=lang)
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
    context_window = compute_search_context_window(
        source_lines,
        match_line=raw.line,
        def_lines=def_lines,
    )
    context_snippet = extract_search_context_snippet(
        source_lines,
        context_window=context_window,
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
        pyrefly_enrichment=enrichment.pyrefly_enrichment,
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


def _maybe_pyrefly_enrichment(
    root: Path,
    file_path: Path,
    raw: RawMatch,
    *,
    lang: QueryLanguage,
    enable_pyrefly: bool,
) -> dict[str, object] | None:
    if not enable_pyrefly or lang != "python":
        return None
    if file_path.suffix not in {".py", ".pyi"}:
        return None
    try:
        payload, timed_out = enrich_with_language_lsp(
            LanguageLspEnrichmentRequest(
                language="python",
                mode="search",
                root=root,
                file_path=file_path,
                line=raw.line,
                col=raw.col,
                symbol_hint=raw.match_text,
            )
        )
    except (OSError, RuntimeError, TimeoutError, ValueError, TypeError):
        return None
    if timed_out and payload is None:
        return None
    return payload


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
    python_payload: dict[str, object] | None = None
    if match.python_enrichment:
        python_payload = dict(match.python_enrichment)
    elif match.language == "python":
        # Keep a stable python payload container for Python findings even when
        # only secondary enrichment sources are available.
        python_payload = {}
    if python_payload is not None:
        if match.pyrefly_enrichment:
            python_payload.setdefault("pyrefly", match.pyrefly_enrichment)
        enrichment["python"] = python_payload
    if match.pyrefly_enrichment:
        enrichment["pyrefly"] = match.pyrefly_enrichment
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
        mode_requested=cast("QueryMode", mode),
        mode_chain=(cast("QueryMode", mode),),
        fallback_applied=bool(kwargs.get("fallback_applied")),
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
        "mode_requested": (
            config.mode_requested.value if isinstance(config.mode_requested, QueryMode) else "auto"
        ),
        "mode_effective": config.mode.value,
        "mode_chain": [mode.value for mode in (config.mode_chain or (config.mode,))],
        "fallback_applied": config.fallback_applied,
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
        "pyrefly_overview": dict[str, object](),
        "pyrefly_telemetry": {
            "attempted": 0,
            "applied": 0,
            "failed": 0,
            "skipped": 0,
            "timed_out": 0,
        },
        "rust_lsp_telemetry": {
            "attempted": 0,
            "applied": 0,
            "failed": 0,
            "skipped": 0,
            "timed_out": 0,
        },
        "lsp_advanced_planes": dict[str, object](),
        "pyrefly_diagnostics": list[dict[str, object]](),
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


def _build_search_context(request: SearchRequest) -> SmartSearchContext:
    started = request.started_ms
    if started is None:
        started = ms()
    limits = request.limits or SMART_SEARCH_LIMITS
    argv = request.argv or ["search", request.query]

    # Clear caches from previous runs
    clear_caches()

    actual_mode = detect_query_mode(request.query, force_mode=request.mode)
    return SearchConfig(
        root=request.root,
        query=request.query,
        mode=actual_mode,
        lang_scope=request.lang_scope,
        mode_requested=request.mode,
        mode_chain=(actual_mode,),
        fallback_applied=False,
        limits=limits,
        include_globs=request.include_globs,
        exclude_globs=request.exclude_globs,
        include_strings=request.include_strings,
        argv=argv,
        tc=request.tc,
        started_ms=started,
    )


def _coerce_search_request(
    *,
    root: Path,
    query: str,
    kwargs: dict[str, object],
) -> SearchRequest:
    return SearchRequest(
        root=root,
        query=query,
        mode=_coerce_query_mode(kwargs.get("mode")),
        lang_scope=_coerce_lang_scope(kwargs.get("lang_scope")),
        include_globs=_coerce_glob_list(kwargs.get("include_globs")),
        exclude_globs=_coerce_glob_list(kwargs.get("exclude_globs")),
        include_strings=bool(kwargs.get("include_strings")),
        limits=_coerce_limits(kwargs.get("limits")),
        tc=cast("Toolchain | None", kwargs.get("tc")),
        argv=_coerce_argv(kwargs.get("argv")),
        started_ms=_coerce_started_ms(kwargs.get("started_ms")),
    )


def _coerce_query_mode(mode_value: object) -> QueryMode | None:
    return mode_value if isinstance(mode_value, QueryMode) else None


def _coerce_lang_scope(lang_scope_value: object) -> QueryLanguageScope:
    if lang_scope_value in {"auto", "python", "rust"}:
        return cast("QueryLanguageScope", lang_scope_value)
    return DEFAULT_QUERY_LANGUAGE_SCOPE


def _coerce_glob_list(globs_value: object) -> list[str] | None:
    if not isinstance(globs_value, list):
        return None
    return [item for item in globs_value if isinstance(item, str)]


def _coerce_limits(limits_value: object) -> SearchLimits | None:
    return limits_value if isinstance(limits_value, SearchLimits) else None


def _coerce_argv(argv_value: object) -> list[str] | None:
    if not isinstance(argv_value, list):
        return None
    return [str(item) for item in argv_value]


def _coerce_started_ms(started_ms_value: object) -> float | None:
    if isinstance(started_ms_value, bool) or not isinstance(started_ms_value, (int, float)):
        return None
    return float(started_ms_value)


def _run_candidate_phase(
    ctx: SmartSearchContext,
    *,
    lang: QueryLanguage,
    mode: QueryMode,
) -> tuple[list[RawMatch], SearchStats, str]:
    pattern = rf"\b{re.escape(ctx.query)}\b" if mode == QueryMode.IDENTIFIER else ctx.query
    include_globs = constrain_include_globs_for_language(ctx.include_globs, lang)
    exclude_globs = list(ctx.exclude_globs or [])
    exclude_globs.extend(language_extension_exclude_globs(lang))
    raw_matches, stats = collect_candidates(
        CandidateCollectionRequest(
            root=ctx.root,
            pattern=pattern,
            mode=mode,
            limits=ctx.limits,
            lang=lang,
            include_globs=include_globs,
            exclude_globs=exclude_globs,
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

    tasks = [
        ClassificationBatchTask(root=str(ctx.root), lang=lang, batch=batch) for batch in batches
    ]
    scheduler = get_worker_scheduler()
    try:
        futures = [
            scheduler.submit_cpu(_classify_partition_batch, task) for task in tasks[:workers]
        ]
        futures.extend(
            scheduler.submit_cpu(_classify_partition_batch, task) for task in tasks[workers:]
        )
        batch = scheduler.collect_bounded(
            futures,
            timeout_seconds=max(1.0, float(len(tasks))),
        )
        if batch.timed_out > 0:
            return [classify_match(raw, ctx.root, lang=lang) for raw in filtered_raw_matches]
        indexed_results: list[tuple[int, EnrichedMatch]] = []
        for batch_results in batch.done:
            indexed_results.extend((item.index, item.match) for item in batch_results)
    except (
        multiprocessing.ProcessError,
        OSError,
        RuntimeError,
        TimeoutError,
        ValueError,
        TypeError,
    ):
        return [classify_match(raw, ctx.root, lang=lang) for raw in filtered_raw_matches]

    indexed_results.sort(key=lambda pair: pair[0])
    return [match for _idx, match in indexed_results]


def _resolve_search_worker_count(partition_count: int) -> int:
    if partition_count <= 1:
        return 1
    return min(partition_count, MAX_SEARCH_CLASSIFY_WORKERS)


def _classify_partition_batch(
    task: ClassificationBatchTask,
) -> list[ClassificationBatchResult]:
    root = Path(task.root)
    return [
        ClassificationBatchResult(
            index=idx,
            match=classify_match(raw, root, lang=task.lang),
        )
        for idx, raw in task.batch
    ]


@dataclass(frozen=True, slots=True)
class _LanguageSearchResult:
    lang: QueryLanguage
    raw_matches: list[RawMatch]
    stats: SearchStats
    pattern: str
    enriched_matches: list[EnrichedMatch]
    dropped_by_scope: int
    pyrefly_prefetch: _PyreflyPrefetchResult | None = None


@dataclass(frozen=True, slots=True)
class _PyreflyPrefetchResult:
    payloads: dict[_PyreflyAnchorKey, dict[str, object]] = dataclass_field(default_factory=dict)
    attempted_keys: set[_PyreflyAnchorKey] = dataclass_field(default_factory=set)
    telemetry: dict[str, int] = dataclass_field(default_factory=dict)
    diagnostics: list[dict[str, object]] = dataclass_field(default_factory=list)


def _run_language_partitions(ctx: SmartSearchContext) -> list[_LanguageSearchResult]:
    by_lang = execute_by_language_scope(
        ctx.lang_scope,
        lambda lang: _run_single_partition(ctx, lang, mode=ctx.mode),
    )
    return [by_lang[lang] for lang in expand_language_scope(ctx.lang_scope)]


def _run_single_partition(
    ctx: SmartSearchContext,
    lang: QueryLanguage,
    *,
    mode: QueryMode,
) -> _LanguageSearchResult:
    cache = get_cq_cache_backend(root=ctx.root)
    cache_key = build_cache_key(
        "search_partition",
        version="v1",
        workspace=str(ctx.root.resolve()),
        language=lang,
        target=ctx.query,
        extras={
            "mode": mode.value,
            "include_strings": ctx.include_strings,
            "include_globs": list(ctx.include_globs or []),
            "exclude_globs": list(ctx.exclude_globs or []),
            "max_total_matches": ctx.limits.max_total_matches,
        },
    )
    cached = cache.get(cache_key)
    if isinstance(cached, dict):
        try:
            cache_payload = msgspec.convert(cached, type=SearchPartitionCacheV1)
            raw_matches = [
                msgspec.convert(item, type=RawMatch) for item in cache_payload.raw_matches
            ]
            stats = msgspec.convert(cache_payload.stats, type=SearchStats)
            enriched = [
                msgspec.convert(item, type=EnrichedMatch) for item in cache_payload.enriched_matches
            ]
            return _LanguageSearchResult(
                lang=lang,
                raw_matches=raw_matches,
                stats=stats,
                pattern=cache_payload.pattern,
                enriched_matches=enriched,
                dropped_by_scope=stats.dropped_by_scope,
                pyrefly_prefetch=None,
            )
        except (RuntimeError, TypeError, ValueError):
            pass

    raw_matches, stats, pattern = _run_candidate_phase(ctx, lang=lang, mode=mode)
    pyrefly_prefetch_future: Future[_PyreflyPrefetchResult] | None = None
    pyrefly_prefetch: _PyreflyPrefetchResult | None = None
    if lang == "python" and raw_matches:
        pyrefly_prefetch_future = get_worker_scheduler().submit_io(
            _prefetch_pyrefly_for_raw_matches,
            ctx,
            lang=lang,
            raw_matches=raw_matches,
        )
    enriched = _run_classification_phase(ctx, lang=lang, raw_matches=raw_matches)
    if pyrefly_prefetch_future is not None:
        try:
            pyrefly_prefetch = pyrefly_prefetch_future.result()
        except (OSError, RuntimeError, TimeoutError, ValueError, TypeError):
            pyrefly_prefetch = _PyreflyPrefetchResult(telemetry=_new_pyrefly_telemetry())
    cache_payload = SearchPartitionCacheV1(
        pattern=pattern,
        raw_matches=cast(
            "list[dict[str, object]]",
            contract_to_builtins(raw_matches),
        ),
        stats=require_mapping(stats),
        enriched_matches=cast(
            "list[dict[str, object]]",
            contract_to_builtins(enriched),
        ),
    )
    cache.set(
        cache_key,
        contract_to_builtins(cache_payload),
        expire=900,
        tag=build_cache_tag(workspace=str(ctx.root.resolve()), language=lang),
    )
    return _LanguageSearchResult(
        lang=lang,
        raw_matches=raw_matches,
        stats=stats,
        pattern=pattern,
        enriched_matches=enriched,
        dropped_by_scope=stats.dropped_by_scope,
        pyrefly_prefetch=pyrefly_prefetch,
    )


def _merge_pyrefly_prefetch_results(
    partition_results: list[_LanguageSearchResult],
) -> _PyreflyPrefetchResult | None:
    payloads: dict[_PyreflyAnchorKey, dict[str, object]] = {}
    attempted_keys: set[_PyreflyAnchorKey] = set()
    telemetry = _new_pyrefly_telemetry()
    diagnostics: list[dict[str, object]] = []
    saw_prefetch = False

    for partition in partition_results:
        prefetched = partition.pyrefly_prefetch
        if prefetched is None:
            continue
        saw_prefetch = True
        payloads.update(prefetched.payloads)
        attempted_keys.update(prefetched.attempted_keys)
        diagnostics.extend(prefetched.diagnostics)
        for key, value in prefetched.telemetry.items():
            if key in telemetry:
                telemetry[key] += int(value)

    if not saw_prefetch:
        return None
    return _PyreflyPrefetchResult(
        payloads=payloads,
        attempted_keys=attempted_keys,
        telemetry=telemetry,
        diagnostics=diagnostics,
    )


def _partition_total_matches(partition_results: list[_LanguageSearchResult]) -> int:
    return sum(result.stats.total_matches for result in partition_results)


def _should_fallback_to_literal(
    *,
    request: SearchRequest,
    initial_mode: QueryMode,
    partition_results: list[_LanguageSearchResult],
) -> bool:
    if request.mode is not None:
        return False
    if initial_mode != QueryMode.IDENTIFIER:
        return False
    return _partition_total_matches(partition_results) == 0


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


def _new_pyrefly_telemetry() -> dict[str, int]:
    return {
        "attempted": 0,
        "applied": 0,
        "failed": 0,
        "skipped": 0,
        "timed_out": 0,
    }


def _pyrefly_anchor_key(
    *,
    file: str,
    line: int,
    col: int,
    match_text: str,
) -> _PyreflyAnchorKey:
    return (file, line, col, match_text)


def _pyrefly_anchor_key_from_raw(raw: RawMatch) -> _PyreflyAnchorKey:
    return _pyrefly_anchor_key(
        file=raw.file,
        line=raw.line,
        col=raw.col,
        match_text=raw.match_text,
    )


def _pyrefly_anchor_key_from_match(match: EnrichedMatch) -> _PyreflyAnchorKey:
    return _pyrefly_anchor_key(
        file=match.file,
        line=match.line,
        col=match.col,
        match_text=match.match_text,
    )


def _pyrefly_failure_diagnostic() -> dict[str, object]:
    return {
        "code": "PYREFLY001",
        "severity": "warning",
        "message": "Pyrefly enrichment unavailable for one or more anchors",
        "reason": "lsp_unavailable_or_unresolved",
    }


def _pyrefly_no_signal_diagnostic(reasons: tuple[str, ...]) -> dict[str, object]:
    reason_text = ",".join(reasons) if reasons else "no_pyrefly_signal"
    return {
        "code": "PYREFLY002",
        "severity": "info",
        "message": "Pyrefly payload resolved but contained no actionable signal",
        "reason": reason_text,
    }


def _prefetch_pyrefly_for_raw_matches(
    ctx: SmartSearchContext,
    *,
    lang: QueryLanguage,
    raw_matches: list[RawMatch],
) -> _PyreflyPrefetchResult:
    if lang != "python" or not raw_matches or not lsp_runtime_enabled():
        return _PyreflyPrefetchResult(telemetry=_new_pyrefly_telemetry())

    telemetry = _new_pyrefly_telemetry()
    payloads: dict[_PyreflyAnchorKey, dict[str, object]] = {}
    attempted_keys: set[_PyreflyAnchorKey] = set()
    diagnostics: list[dict[str, object]] = []
    pyrefly_budget_used = 0

    for raw in raw_matches:
        if pyrefly_budget_used >= MAX_PYREFLY_ENRICH_FINDINGS:
            telemetry["skipped"] += 1
            continue
        if not is_path_in_lang_scope(raw.file, lang):
            continue
        key = _pyrefly_anchor_key_from_raw(raw)
        if key in attempted_keys:
            continue
        attempted_keys.add(key)
        pyrefly_budget_used += 1
        telemetry["attempted"] += 1
        file_path = ctx.root / raw.file
        try:
            payload = _maybe_pyrefly_enrichment(
                ctx.root,
                file_path,
                raw,
                lang=lang,
                enable_pyrefly=True,
            )
        except TimeoutError:
            telemetry["timed_out"] += 1
            payload = None
        except _PYREFLY_PREFETCH_NON_FATAL_EXCEPTIONS:
            telemetry["failed"] += 1
            payload = None

        if isinstance(payload, dict) and payload:
            has_signal, reasons = evaluate_pyrefly_signal_from_mapping(payload)
            if has_signal:
                telemetry["applied"] += 1
                payloads[key] = payload
            else:
                telemetry["failed"] += 1
                diagnostics.append(_pyrefly_no_signal_diagnostic(reasons))
        else:
            telemetry["failed"] += 1
            diagnostics.append(_pyrefly_failure_diagnostic())

    return _PyreflyPrefetchResult(
        payloads=payloads,
        attempted_keys=attempted_keys,
        telemetry=telemetry,
        diagnostics=diagnostics,
    )


def _pyrefly_enrich_match(
    ctx: SmartSearchContext,
    match: EnrichedMatch,
) -> dict[str, object] | None:
    if match.language != "python":
        return None
    if not lsp_runtime_enabled():
        return None
    file_path = ctx.root / match.file
    if file_path.suffix not in {".py", ".pyi"}:
        return None
    payload, timed_out = enrich_with_language_lsp(
        LanguageLspEnrichmentRequest(
            language="python",
            mode="search",
            root=ctx.root,
            file_path=file_path,
            line=match.line,
            col=match.col,
            symbol_hint=match.match_text,
        )
    )
    if timed_out and payload is None:
        msg = "pyrefly_timeout"
        raise TimeoutError(msg)
    return payload


def _seed_pyrefly_state(
    prefetched: _PyreflyPrefetchResult | None,
) -> tuple[dict[str, int], list[dict[str, object]]]:
    telemetry = _new_pyrefly_telemetry()
    diagnostics: list[dict[str, object]] = []
    if prefetched is None:
        return telemetry, diagnostics
    diagnostics.extend(prefetched.diagnostics)
    for key, value in prefetched.telemetry.items():
        if key in telemetry:
            telemetry[key] += int(value)
    return telemetry, diagnostics


def _pyrefly_payload_from_prefetch(
    prefetched: _PyreflyPrefetchResult | None,
    key: _PyreflyAnchorKey,
) -> dict[str, object] | None:
    if prefetched is None or key not in prefetched.attempted_keys:
        return None
    return prefetched.payloads.get(key)


def _fetch_pyrefly_payload(
    *,
    ctx: SmartSearchContext,
    match: EnrichedMatch,
    prefetched: _PyreflyPrefetchResult | None,
    telemetry: dict[str, int],
) -> tuple[dict[str, object] | None, bool]:
    if not lsp_runtime_enabled():
        return None, False
    key = _pyrefly_anchor_key_from_match(match)
    prefetched_payload = _pyrefly_payload_from_prefetch(prefetched, key)
    if prefetched is not None and key in prefetched.attempted_keys:
        return prefetched_payload, False

    telemetry["attempted"] += 1
    try:
        return _pyrefly_enrich_match(ctx, match), True
    except TimeoutError:
        telemetry["timed_out"] += 1
        return None, True
    except _PYREFLY_PREFETCH_NON_FATAL_EXCEPTIONS:
        return None, True


def _merge_match_with_pyrefly_payload(
    *,
    match: EnrichedMatch,
    payload: dict[str, object] | None,
    attempted_in_place: bool,
    telemetry: dict[str, int],
    diagnostics: list[dict[str, object]],
) -> EnrichedMatch:
    if isinstance(payload, dict) and payload:
        has_signal, reasons = evaluate_pyrefly_signal_from_mapping(payload)
        if not has_signal:
            if attempted_in_place:
                telemetry["failed"] += 1
                diagnostics.append(_pyrefly_no_signal_diagnostic(reasons))
            return match
        if attempted_in_place:
            telemetry["applied"] += 1
        return msgspec.structs.replace(match, pyrefly_enrichment=payload)

    if attempted_in_place:
        telemetry["failed"] += 1
        diagnostics.append(_pyrefly_failure_diagnostic())
    return match


def _pyrefly_summary_payload(
    *,
    overview: dict[str, object],
    telemetry: dict[str, int],
    diagnostics: list[dict[str, object]],
) -> tuple[dict[str, object], dict[str, object], list[dict[str, object]]]:
    telemetry_payload = coerce_pyrefly_telemetry(telemetry)
    diagnostics_payload = coerce_pyrefly_diagnostics(diagnostics)
    return (
        to_public_dict(coerce_pyrefly_overview(overview)),
        to_public_dict(telemetry_payload),
        to_public_list(diagnostics_payload),
    )


def _attach_pyrefly_enrichment(
    *,
    ctx: SmartSearchContext,
    matches: list[EnrichedMatch],
    prefetched: _PyreflyPrefetchResult | None = None,
) -> tuple[list[EnrichedMatch], dict[str, object], dict[str, object], list[dict[str, object]]]:
    if not lsp_runtime_enabled():
        return matches, {}, cast("dict[str, object]", _new_pyrefly_telemetry()), []

    telemetry, diagnostics = _seed_pyrefly_state(prefetched)

    if not matches:
        overview, telemetry_map, diagnostic_rows = _pyrefly_summary_payload(
            overview={},
            telemetry=telemetry,
            diagnostics=diagnostics,
        )
        return matches, overview, telemetry_map, diagnostic_rows

    enriched: list[EnrichedMatch] = []
    pyrefly_budget_used = 0

    for match in matches:
        if match.language != "python":
            enriched.append(match)
            continue
        if pyrefly_budget_used >= MAX_PYREFLY_ENRICH_FINDINGS:
            telemetry["skipped"] += 1
            enriched.append(match)
            continue
        pyrefly_budget_used += 1
        payload, attempted_in_place = _fetch_pyrefly_payload(
            ctx=ctx,
            match=match,
            prefetched=prefetched,
            telemetry=telemetry,
        )
        enriched.append(
            _merge_match_with_pyrefly_payload(
                match=match,
                payload=payload,
                attempted_in_place=attempted_in_place,
                telemetry=telemetry,
                diagnostics=diagnostics,
            )
        )

    overview = _build_pyrefly_overview(enriched)
    overview_map, telemetry_map, diagnostic_rows = _pyrefly_summary_payload(
        overview=overview,
        telemetry=telemetry,
        diagnostics=diagnostics,
    )
    return enriched, overview_map, telemetry_map, diagnostic_rows


def _first_string(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str) and value:
            return value
    return None


def _normalize_neighborhood_file_path(path: str) -> str:
    normalized = path.strip()
    if normalized.startswith("./"):
        normalized = normalized[2:]
    return Path(normalized).as_posix()


@dataclass(slots=True)
class _PyreflyOverviewAccumulator:
    primary_symbol: str | None = None
    enclosing_class: str | None = None
    total_incoming: int = 0
    total_outgoing: int = 0
    total_implementations: int = 0
    diagnostics: int = 0
    enriched_count: int = 0


def _count_mapping_rows(value: object) -> int:
    if not isinstance(value, list):
        return 0
    return sum(1 for row in value if isinstance(row, dict))


def _accumulate_pyrefly_overview(
    *,
    acc: _PyreflyOverviewAccumulator,
    payload: dict[str, object],
    match_text: str,
) -> None:
    acc.enriched_count += 1
    type_contract = payload.get("type_contract")
    if isinstance(type_contract, dict):
        acc.primary_symbol = _first_string(
            acc.primary_symbol,
            type_contract.get("callable_signature"),
            type_contract.get("resolved_type"),
            match_text,
        )
    class_ctx = payload.get("class_method_context")
    if isinstance(class_ctx, dict):
        acc.enclosing_class = _first_string(acc.enclosing_class, class_ctx.get("enclosing_class"))
    call_graph = payload.get("call_graph")
    if isinstance(call_graph, dict):
        incoming = call_graph.get("incoming_total")
        outgoing = call_graph.get("outgoing_total")
        if isinstance(incoming, int):
            acc.total_incoming += incoming
        if isinstance(outgoing, int):
            acc.total_outgoing += outgoing
    grounding = payload.get("symbol_grounding")
    if isinstance(grounding, dict):
        acc.total_implementations += _count_mapping_rows(grounding.get("implementation_targets"))
    acc.diagnostics += _count_mapping_rows(payload.get("anchor_diagnostics"))


def _build_pyrefly_overview(matches: list[EnrichedMatch]) -> dict[str, object]:
    acc = _PyreflyOverviewAccumulator()
    for match in matches:
        payload = match.pyrefly_enrichment
        if isinstance(payload, dict):
            _accumulate_pyrefly_overview(acc=acc, payload=payload, match_text=match.match_text)
    return {
        "primary_symbol": acc.primary_symbol,
        "enclosing_class": acc.enclosing_class,
        "total_incoming_callers": acc.total_incoming,
        "total_outgoing_callees": acc.total_outgoing,
        "total_implementations": acc.total_implementations,
        "targeted_diagnostics": acc.diagnostics,
        "matches_enriched": acc.enriched_count,
    }


def _merge_matches_and_pyrefly(
    ctx: SmartSearchContext,
    partition_results: list[_LanguageSearchResult],
) -> tuple[
    list[EnrichedMatch],
    dict[str, object],
    dict[str, object],
    list[dict[str, object]],
]:
    enriched_matches = _merge_language_matches(
        partition_results=partition_results,
        lang_scope=ctx.lang_scope,
    )
    prefetched_pyrefly = _merge_pyrefly_prefetch_results(partition_results)
    return _attach_pyrefly_enrichment(
        ctx=ctx,
        matches=enriched_matches,
        prefetched=prefetched_pyrefly,
    )


def _build_search_summary(
    ctx: SmartSearchContext,
    partition_results: list[_LanguageSearchResult],
    enriched_matches: list[EnrichedMatch],
    *,
    pyrefly_overview: dict[str, object],
    pyrefly_telemetry: dict[str, object],
    pyrefly_diagnostics: list[dict[str, object]],
) -> tuple[dict[str, object], list[Finding]]:
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
    capability_diagnostics = _build_capability_diagnostics_for_search(lang_scope=ctx.lang_scope)
    all_diagnostics = diagnostics + capability_diagnostics
    summary["cross_language_diagnostics"] = diagnostics_to_summary_payload(all_diagnostics)
    summary["language_capabilities"] = build_language_capabilities(lang_scope=ctx.lang_scope)
    summary["enrichment_telemetry"] = _build_enrichment_telemetry(enriched_matches)
    summary["pyrefly_overview"] = pyrefly_overview
    summary["pyrefly_telemetry"] = pyrefly_telemetry
    summary.setdefault(
        "rust_lsp_telemetry",
        {"attempted": 0, "applied": 0, "failed": 0, "skipped": 0, "timed_out": 0},
    )
    summary["pyrefly_diagnostics"] = pyrefly_diagnostics
    if dropped_by_scope:
        summary["dropped_by_scope"] = dropped_by_scope
    assert_multilang_summary(summary)
    return summary, all_diagnostics


def _collect_definition_candidates(
    ctx: SmartSearchContext,
    enriched_matches: list[EnrichedMatch],
) -> tuple[list[EnrichedMatch], list[Finding]]:
    ranked_matches = sorted(enriched_matches, key=compute_relevance_score, reverse=True)
    definition_pairs: list[tuple[EnrichedMatch, Finding]] = []
    for match in ranked_matches:
        candidate = build_definition_candidate_finding(
            match,
            ctx.root,
            build_finding_fn=build_finding,
        )
        if candidate is None:
            continue
        definition_pairs.append((match, candidate))
        if len(definition_pairs) >= MAX_TARGET_CANDIDATES:
            break
    definition_matches = [match for match, _finding in definition_pairs]
    candidate_findings = [finding for _match, finding in definition_pairs]
    return definition_matches, candidate_findings


def _build_structural_neighborhood_preview(
    ctx: SmartSearchContext,
    *,
    primary_target_finding: Finding | None,
    definition_matches: list[EnrichedMatch],
) -> tuple[InsightNeighborhoodV1 | None, list[Finding], list[str]]:
    from tools.cq.core.front_door_insight import build_neighborhood_from_slices
    from tools.cq.neighborhood.scan_snapshot import ScanSnapshot
    from tools.cq.neighborhood.structural_collector import (
        StructuralNeighborhoodCollectRequest,
        collect_structural_neighborhood,
    )

    if primary_target_finding is None or primary_target_finding.anchor is None:
        return None, [], []

    target_name = (
        str(primary_target_finding.details.get("name", "")).strip()
        or primary_target_finding.message.split(":")[-1].strip()
        or ctx.query
    )
    target_file = _normalize_neighborhood_file_path(primary_target_finding.anchor.file)
    target_language = (
        definition_matches[0].language
        if definition_matches
        else str(primary_target_finding.details.get("language", "python"))
    )
    snapshot_language: QueryLanguage = "rust" if target_language == "rust" else "python"
    try:
        snapshot = ScanSnapshot.build_from_repo(ctx.root, lang=snapshot_language)
        slices, degrades = collect_structural_neighborhood(
            StructuralNeighborhoodCollectRequest(
                target_name=target_name,
                target_file=target_file,
                target_line=primary_target_finding.anchor.line,
                target_col=int(primary_target_finding.anchor.col or 0),
                snapshot=snapshot,
                max_per_slice=5,
            )
        )
    except (OSError, RuntimeError, TimeoutError, ValueError, TypeError) as exc:
        return None, [], [f"structural_scan_unavailable:{type(exc).__name__}"]

    neighborhood = build_neighborhood_from_slices(
        slices,
        preview_per_slice=5,
        source="structural",
    )
    findings: list[Finding] = []
    for slice_item in slices:
        labels = [
            node.display_label or node.name
            for node in slice_item.preview[:5]
            if (node.display_label or node.name)
        ]
        message = f"{slice_item.title}: {slice_item.total}"
        if labels:
            message += f" (top: {', '.join(labels)})"
        findings.append(
            Finding(
                category="neighborhood",
                message=message,
                severity="info",
                details=DetailPayload(
                    kind="neighborhood",
                    data={
                        "slice_kind": slice_item.kind,
                        "total": slice_item.total,
                        "preview": labels,
                    },
                ),
            )
        )
    notes = [f"{degrade.stage}:{degrade.category or degrade.severity}" for degrade in degrades]
    return neighborhood, findings, list(dict.fromkeys(notes))


def _apply_search_lsp_insight(
    *,
    ctx: SmartSearchContext,
    insight: FrontDoorInsightV1,
    summary: dict[str, object],
    primary_target_finding: Finding | None,
    top_definition_match: EnrichedMatch | None,
) -> FrontDoorInsightV1:
    from tools.cq.core.front_door_insight import augment_insight_with_lsp

    outcome = _collect_search_lsp_outcome(
        ctx=ctx,
        primary_target_finding=primary_target_finding,
        top_definition_match=top_definition_match,
    )
    if outcome.payload is not None:
        insight = augment_insight_with_lsp(insight, outcome.payload)
        advanced_planes = outcome.payload.get("advanced_planes")
        if isinstance(advanced_planes, dict):
            summary["lsp_advanced_planes"] = dict(advanced_planes)
    _update_search_summary_lsp_telemetry(summary, outcome)
    lsp_state = _derive_search_lsp_state(outcome)
    return msgspec.structs.replace(
        insight,
        degradation=msgspec.structs.replace(
            insight.degradation,
            lsp=lsp_state.status,
            notes=tuple(dict.fromkeys([*insight.degradation.notes, *lsp_state.reasons])),
        ),
    )


@dataclass(slots=True)
class _SearchLspOutcome:
    provider: str = "none"
    target_language: str | None = None
    payload: dict[str, object] | None = None
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    timed_out: int = 0
    reasons: list[str] = dataclass_field(default_factory=list)


@dataclass(slots=True)
class _SearchAssemblyInputs:
    enriched_matches: list[EnrichedMatch]
    summary: dict[str, object]
    sections: list[Section]
    all_diagnostics: list[Finding]
    definition_matches: list[EnrichedMatch]
    candidate_findings: list[Finding]
    primary_target_finding: Finding | None
    insight_neighborhood: InsightNeighborhoodV1 | None
    neighborhood_notes: list[str]


def _collect_search_lsp_outcome(
    *,
    ctx: SmartSearchContext,
    primary_target_finding: Finding | None,
    top_definition_match: EnrichedMatch | None,
) -> _SearchLspOutcome:
    outcome = _SearchLspOutcome()
    if (
        primary_target_finding is None
        or primary_target_finding.anchor is None
        or top_definition_match is None
    ):
        return outcome

    anchor = primary_target_finding.anchor
    target_file_path = ctx.root / anchor.file
    inferred_language = infer_language_for_path(target_file_path)
    target_language = (
        top_definition_match.language
        if top_definition_match.language in {"python", "rust"}
        else inferred_language
    )
    if target_language not in {"python", "rust"}:
        outcome.reasons.append("provider_unavailable")
        return outcome

    outcome.provider = provider_for_language(target_language)
    outcome.target_language = target_language
    if not lsp_runtime_enabled():
        outcome.reasons.append("not_attempted_runtime_disabled")
        return outcome

    outcome.attempted = 1
    if target_language == "python" and isinstance(top_definition_match.pyrefly_enrichment, dict):
        outcome.payload = top_definition_match.pyrefly_enrichment
        outcome.applied = 1
        return outcome

    payload, timed_out = enrich_with_language_lsp(
        LanguageLspEnrichmentRequest(
            language=target_language,
            mode="search",
            root=ctx.root,
            file_path=target_file_path,
            line=max(1, int(anchor.line)),
            col=int(anchor.col or 0),
            symbol_hint=(
                str(primary_target_finding.details.get("name"))
                if isinstance(primary_target_finding.details.get("name"), str)
                else top_definition_match.match_text
            ),
        )
    )
    outcome.payload = payload
    outcome.timed_out = int(timed_out)
    if payload is None:
        outcome.failed = 1
        outcome.reasons.append("request_timeout" if timed_out else "request_failed")
        return outcome
    outcome.applied = 1
    return outcome


def _update_search_summary_lsp_telemetry(
    summary: dict[str, object],
    outcome: _SearchLspOutcome,
) -> None:
    if outcome.attempted <= 0 or outcome.target_language not in {"python", "rust"}:
        return
    telemetry_key = (
        "pyrefly_telemetry" if outcome.target_language == "python" else "rust_lsp_telemetry"
    )
    telemetry = summary.get(telemetry_key)
    if not isinstance(telemetry, dict):
        return
    telemetry["attempted"] = int(telemetry.get("attempted", 0) or 0) + outcome.attempted
    telemetry["applied"] = int(telemetry.get("applied", 0) or 0) + outcome.applied
    telemetry["failed"] = int(telemetry.get("failed", 0) or 0) + outcome.failed
    telemetry["timed_out"] = int(telemetry.get("timed_out", 0) or 0) + outcome.timed_out


def _derive_search_lsp_state(outcome: _SearchLspOutcome) -> LspContractStateV1:
    if outcome.provider == "none":
        outcome.reasons.append("provider_unavailable")
    elif outcome.attempted <= 0 and "not_attempted_runtime_disabled" not in outcome.reasons:
        outcome.reasons.append("not_attempted_by_design")
    provider_value = (
        "pyrefly"
        if outcome.provider == "pyrefly"
        else "rust_analyzer"
        if outcome.provider == "rust_analyzer"
        else "none"
    )
    return derive_lsp_contract_state(
        LspContractStateInputV1(
            provider=provider_value,
            available=outcome.provider != "none",
            attempted=outcome.attempted,
            applied=outcome.applied,
            failed=outcome.failed,
            timed_out=outcome.timed_out,
            reasons=tuple(dict.fromkeys(outcome.reasons)),
        )
    )


def _prepare_search_assembly_inputs(
    ctx: SmartSearchContext,
    partition_results: list[_LanguageSearchResult],
) -> _SearchAssemblyInputs:
    enriched_matches, pyrefly_overview, pyrefly_telemetry, pyrefly_diagnostics = (
        _merge_matches_and_pyrefly(ctx, partition_results)
    )
    summary, all_diagnostics = _build_search_summary(
        ctx,
        partition_results,
        enriched_matches,
        pyrefly_overview=pyrefly_overview,
        pyrefly_telemetry=pyrefly_telemetry,
        pyrefly_diagnostics=pyrefly_diagnostics,
    )
    sections = build_sections(
        enriched_matches,
        ctx.root,
        ctx.query,
        ctx.mode,
        include_strings=ctx.include_strings,
    )
    if all_diagnostics:
        sections.append(Section(title="Cross-Language Diagnostics", findings=all_diagnostics))
    definition_matches, candidate_findings = _collect_definition_candidates(ctx, enriched_matches)
    insert_target_candidates(sections, candidates=candidate_findings)
    primary_target_finding = candidate_findings[0] if candidate_findings else None
    insight_neighborhood, neighborhood_findings, neighborhood_notes = _build_structural_neighborhood_preview(
        ctx,
        primary_target_finding=primary_target_finding,
        definition_matches=definition_matches,
    )
    insert_neighborhood_preview(
        sections,
        findings=neighborhood_findings,
        has_target_candidates=bool(candidate_findings),
    )
    return _SearchAssemblyInputs(
        enriched_matches=enriched_matches,
        summary=summary,
        sections=sections,
        all_diagnostics=all_diagnostics,
        definition_matches=definition_matches,
        candidate_findings=candidate_findings,
        primary_target_finding=primary_target_finding,
        insight_neighborhood=insight_neighborhood,
        neighborhood_notes=neighborhood_notes,
    )


def _build_search_result_key_findings(inputs: _SearchAssemblyInputs) -> list[Finding]:
    key_findings = list(
        inputs.candidate_findings or (inputs.sections[0].findings[:5] if inputs.sections else [])
    )
    key_findings.extend(inputs.all_diagnostics)
    return key_findings


def _build_search_risk(
    neighborhood: InsightNeighborhoodV1 | None,
) -> InsightRiskV1 | None:
    from tools.cq.core.front_door_insight import InsightRiskCountersV1, risk_from_counters

    if neighborhood is None:
        return None
    return risk_from_counters(
        InsightRiskCountersV1(
            callers=neighborhood.callers.total,
            callees=neighborhood.callees.total,
        )
    )


def _assemble_search_insight(
    ctx: SmartSearchContext,
    inputs: _SearchAssemblyInputs,
) -> FrontDoorInsightV1:
    from tools.cq.core.front_door_insight import SearchInsightBuildRequestV1, build_search_insight

    insight = build_search_insight(
        SearchInsightBuildRequestV1(
            summary=inputs.summary,
            primary_target=inputs.primary_target_finding,
            target_candidates=tuple(inputs.candidate_findings),
            neighborhood=inputs.insight_neighborhood,
            risk=_build_search_risk(inputs.insight_neighborhood),
        )
    )
    if inputs.neighborhood_notes:
        insight = msgspec.structs.replace(
            insight,
            degradation=msgspec.structs.replace(
                insight.degradation,
                notes=tuple(dict.fromkeys([*insight.degradation.notes, *inputs.neighborhood_notes])),
            ),
        )
    top_def_match = inputs.definition_matches[0] if inputs.definition_matches else None
    return _apply_search_lsp_insight(
        ctx=ctx,
        insight=insight,
        summary=inputs.summary,
        primary_target_finding=inputs.primary_target_finding,
        top_definition_match=top_def_match,
    )


def _assemble_smart_search_result(
    ctx: SmartSearchContext,
    partition_results: list[_LanguageSearchResult],
) -> CqResult:
    from tools.cq.core.front_door_insight import to_public_front_door_insight_dict

    inputs = _prepare_search_assembly_inputs(ctx, partition_results)
    insight = _assemble_search_insight(ctx, inputs)
    inputs.summary["front_door_insight"] = to_public_front_door_insight_dict(insight)

    run_ctx = RunContext.from_parts(
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.tc,
        started_ms=ctx.started_ms,
    )
    run = run_ctx.to_runmeta("search")
    return CqResult(
        run=run,
        summary=inputs.summary,
        sections=inputs.sections,
        key_findings=_build_search_result_key_findings(inputs),
        evidence=[build_finding(m, ctx.root) for m in inputs.enriched_matches[:MAX_EVIDENCE]],
    )


def smart_search(
    root: Path,
    query: str,
    **kwargs: object,
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
    request = _coerce_search_request(root=root, query=query, kwargs=kwargs)
    ctx = _build_search_context(request)
    pipeline = SearchPipeline(ctx)
    partition_results = cast(
        "list[_LanguageSearchResult]",
        pipeline.run_partitions(_run_language_partitions),
    )
    mode_chain = [ctx.mode]
    if _should_fallback_to_literal(
        request=request,
        initial_mode=ctx.mode,
        partition_results=partition_results,
    ):
        fallback_ctx = msgspec.structs.replace(
            ctx,
            mode=QueryMode.LITERAL,
            fallback_applied=True,
        )
        fallback_partitions = cast(
            "list[_LanguageSearchResult]",
            SearchPipeline(fallback_ctx).run_partitions(_run_language_partitions),
        )
        mode_chain.append(QueryMode.LITERAL)
        ctx = msgspec.structs.replace(fallback_ctx, mode_chain=tuple(mode_chain))
        if _partition_total_matches(fallback_partitions) > 0:
            partition_results = fallback_partitions
    elif not ctx.mode_chain:
        ctx = msgspec.structs.replace(ctx, mode_chain=(ctx.mode,))
    return SearchPipeline(ctx).assemble(partition_results, _assemble_smart_search_result)


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
