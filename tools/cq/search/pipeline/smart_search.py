"""Smart Search pipeline for semantically-enriched code search."""

from __future__ import annotations

import multiprocessing
import re
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from pathlib import Path
from typing import TYPE_CHECKING, cast

import msgspec

from tools.cq.core.cache import (
    get_cq_cache_backend,
    maintenance_tick,
    maybe_evict_run_cache_tag,
    snapshot_backend_metrics,
)
from tools.cq.core.contracts import contract_to_builtins
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
    assign_result_finding_ids,
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
from tools.cq.search._shared.core import (
    CandidateCollectionRequest,
    PythonByteRangeEnrichmentRequest,
    RgRunRequest,
)
from tools.cq.search._shared.search_contracts import (
    coerce_python_semantic_diagnostics,
    coerce_python_semantic_overview,
    coerce_python_semantic_telemetry,
)
from tools.cq.search.enrichment.core import normalize_python_payload, normalize_rust_payload
from tools.cq.search.objects.render import (
    SearchObjectResolvedViewV1,
    SearchObjectSummaryV1,
    SearchOccurrenceV1,
    build_non_code_occurrence_section,
    build_occurrence_hot_files_section,
    build_occurrence_kind_counts_section,
    build_occurrences_section,
    build_resolved_objects_section,
    is_non_code_occurrence,
)
from tools.cq.search.objects.resolve import ObjectResolutionRuntime, build_object_resolved_view
from tools.cq.search.pipeline.classifier import (
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
from tools.cq.search.pipeline.context_window import (
    compute_search_context_window,
    extract_search_context_snippet,
)
from tools.cq.search.pipeline.legacy import SearchPipeline
from tools.cq.search.pipeline.models import (
    CandidateSearchRequest,
    SearchConfig,
    SearchRequest,
    SmartSearchContext,
)
from tools.cq.search.pipeline.partition_contracts import SearchPartitionPlanV1
from tools.cq.search.pipeline.partition_pipeline import run_search_partition
from tools.cq.search.pipeline.profiles import INTERACTIVE, SearchLimits
from tools.cq.search.pipeline.section_builder import (
    insert_neighborhood_preview,
    insert_target_candidates,
)
from tools.cq.search.python.analysis_session import get_python_analysis_session
from tools.cq.search.python.extractors import (
    _ENRICHMENT_ERRORS as _PYTHON_ENRICHMENT_ERRORS,
)
from tools.cq.search.python.extractors import enrich_python_context_by_byte_range
from tools.cq.search.python.semantic_signal import evaluate_python_semantic_signal_from_mapping
from tools.cq.search.rg.collector import RgCollector
from tools.cq.search.rg.runner import build_rg_command, run_rg_json
from tools.cq.search.rust.enrichment import enrich_rust_context_by_byte_range
from tools.cq.search.semantic.diagnostics import (
    build_cross_language_diagnostics,
    build_language_capabilities,
    diagnostics_to_summary_payload,
    is_python_oriented_query_text,
)
from tools.cq.search.semantic.models import (
    LanguageSemanticEnrichmentOutcome,
    LanguageSemanticEnrichmentRequest,
    SemanticContractStateInputV1,
    SemanticContractStateV1,
    SemanticProvider,
    derive_semantic_contract_state,
    enrich_with_language_semantics,
    infer_language_for_path,
    provider_for_language,
    semantic_runtime_enabled,
)
from tools.cq.search.tree_sitter.core.adaptive_runtime import adaptive_query_budget_ms
from tools.cq.search.tree_sitter.core.budgeting import budget_ms_per_anchor
from tools.cq.search.tree_sitter.python_lane.runtime import get_tree_sitter_python_cache_stats
from tools.cq.search.tree_sitter.query.lint import lint_search_query_packs
from tools.cq.search.tree_sitter.rust_lane.runtime import get_tree_sitter_rust_cache_stats
from tools.cq.utils.uuid_factory import uuid7_str

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
_TARGET_CANDIDATE_KINDS: frozenset[str] = frozenset(
    {
        "function",
        "method",
        "class",
        "type",
        "module",
        "callable",
    }
)
_RUST_ENRICHMENT_ERRORS = (RuntimeError, TypeError, ValueError, AttributeError, UnicodeError)
_PYTHON_SEMANTIC_PREFETCH_NON_FATAL_EXCEPTIONS = (
    OSError,
    RuntimeError,
    TimeoutError,
    TypeError,
    ValueError,
)
MAX_SEARCH_CLASSIFY_WORKERS = 4
MAX_PYTHON_SEMANTIC_ENRICH_FINDINGS = 8
_TREE_SITTER_QUERY_BUDGET_FALLBACK_MS = budget_ms_per_anchor(
    timeout_seconds=SMART_SEARCH_LIMITS.timeout_seconds,
    max_anchors=SMART_SEARCH_LIMITS.max_total_matches,
)
_PythonSemanticAnchorKey = tuple[str, int, int, str]
_SEARCH_OBJECT_VIEW_REGISTRY: dict[str, SearchObjectResolvedViewV1] = {}


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
    python_semantic_enrichment: dict[str, object] | None


@dataclass(frozen=True, slots=True)
class MatchClassifyOptions:
    enable_symtable: bool = True
    force_semantic_enrichment: bool = False
    enable_python_semantic: bool = False
    enable_deep_enrichment: bool = True


def _merged_classify_options(
    options: MatchClassifyOptions | None,
    legacy_flags: dict[str, bool],
) -> MatchClassifyOptions:
    base = options or MatchClassifyOptions()
    return MatchClassifyOptions(
        enable_symtable=bool(legacy_flags.get("enable_symtable", base.enable_symtable)),
        force_semantic_enrichment=bool(
            legacy_flags.get("force_semantic_enrichment", base.force_semantic_enrichment)
        ),
        enable_python_semantic=bool(
            legacy_flags.get("enable_python_semantic", base.enable_python_semantic)
        ),
        enable_deep_enrichment=bool(
            legacy_flags.get("enable_deep_enrichment", base.enable_deep_enrichment)
        ),
    )


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
    python_semantic_enrichment: dict[str, object] | None = None
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
        with_neighborhood=False,
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
    options: MatchClassifyOptions | None = None,
    **legacy_flags: bool,
) -> EnrichedMatch:
    """Run three-stage classification pipeline on a raw match.

    Returns:
        EnrichedMatch: Fully classified match with enrichment payloads.
    """
    resolved_options = _merged_classify_options(options, legacy_flags)

    # Stage 1: Fast heuristic
    heuristic = classify_heuristic(raw.text, raw.col, raw.match_text)

    if (
        heuristic.skip_deeper
        and heuristic.category is not None
        and not resolved_options.force_semantic_enrichment
    ):
        file_path = root / raw.file
        context_window, context_snippet = _build_context_enrichment(file_path, raw, lang=lang)
        return _build_heuristic_enriched(
            raw,
            heuristic,
            raw.match_text,
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
        enable_symtable=resolved_options.enable_symtable,
    )
    tree_sitter_budget_ms = adaptive_query_budget_ms(
        language=str(lang),
        fallback_budget_ms=_TREE_SITTER_QUERY_BUDGET_FALLBACK_MS,
    )
    rust_tree_sitter = (
        _maybe_rust_tree_sitter_enrichment(
            file_path,
            raw,
            lang=lang,
            query_budget_ms=tree_sitter_budget_ms,
        )
        if resolved_options.enable_deep_enrichment
        else None
    )
    python_enrichment = (
        _maybe_python_enrichment(
            file_path,
            raw,
            lang=lang,
            resolved_python=resolved_python,
            query_budget_ms=tree_sitter_budget_ms,
        )
        if resolved_options.enable_deep_enrichment
        else None
    )
    python_semantic_enrichment = (
        _maybe_python_semantic_enrichment(
            root,
            file_path,
            raw,
            lang=lang,
            enable_python_semantic=resolved_options.enable_python_semantic,
        )
        if resolved_options.enable_deep_enrichment
        else None
    )
    context_window, context_snippet = _build_context_enrichment(file_path, raw, lang=lang)
    enrichment = MatchEnrichment(
        symtable=symtable_enrichment,
        context_window=context_window,
        context_snippet=context_snippet,
        rust_tree_sitter=rust_tree_sitter,
        python_enrichment=python_enrichment,
        python_semantic_enrichment=python_semantic_enrichment,
    )
    return _build_enriched_match(
        raw,
        raw.match_text,
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
        python_semantic_enrichment=enrichment.python_semantic_enrichment,
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
    query_budget_ms: int | None = None,
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
            query_budget_ms=query_budget_ms,
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
    query_budget_ms: int | None = None,
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
                query_budget_ms=query_budget_ms,
                session=session,
            )
        )
        return normalize_python_payload(payload)
    except _PYTHON_ENRICHMENT_ERRORS:
        return None


def _maybe_python_semantic_enrichment(
    root: Path,
    file_path: Path,
    raw: RawMatch,
    *,
    lang: QueryLanguage,
    enable_python_semantic: bool,
) -> dict[str, object] | None:
    if not enable_python_semantic or lang != "python":
        return None
    if file_path.suffix not in {".py", ".pyi"}:
        return None
    try:
        outcome = enrich_with_language_semantics(
            LanguageSemanticEnrichmentRequest(
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
    return outcome.payload if isinstance(outcome.payload, dict) else None


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
        if match.python_semantic_enrichment:
            python_payload.setdefault("python_semantic", match.python_semantic_enrichment)
        enrichment["python"] = python_payload
    if match.python_semantic_enrichment:
        enrichment["python_semantic"] = match.python_semantic_enrichment
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
        with_neighborhood=False,
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
        "with_neighborhood": bool(config.with_neighborhood),
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
        "python_semantic_overview": dict[str, object](),
        "python_semantic_telemetry": {
            "attempted": 0,
            "applied": 0,
            "failed": 0,
            "skipped": 0,
            "timed_out": 0,
        },
        "rust_semantic_telemetry": {
            "attempted": 0,
            "applied": 0,
            "failed": 0,
            "skipped": 0,
            "timed_out": 0,
        },
        "semantic_planes": dict[str, object](),
        "python_semantic_diagnostics": list[dict[str, object]](),
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
    object_runtime: ObjectResolutionRuntime | None = None,
) -> list[Section]:
    """Build object-resolved sections for CqResult.

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
    object_runtime
        Optional precomputed object-resolution runtime.

    Returns:
    -------
    list[Section]
        Organized sections.
    """
    _ = root
    runtime = object_runtime or build_object_resolved_view(matches, query=query)
    visible_occurrences, non_code_occurrences = _split_occurrences_for_render(
        runtime.view.occurrences,
        include_strings=include_strings,
    )
    object_symbols = {
        summary.object_ref.object_id: summary.object_ref.symbol
        for summary in runtime.view.summaries
    }
    occurrences_by_object: dict[str, list[SearchOccurrenceV1]] = {}
    for row in runtime.view.occurrences:
        occurrences_by_object.setdefault(row.object_id, []).append(row)
    occurrence_rows = runtime.view.occurrences if include_strings else visible_occurrences
    sections: list[Section] = [
        build_resolved_objects_section(
            runtime.view.summaries,
            occurrences_by_object=occurrences_by_object,
        ),
        build_occurrences_section(occurrence_rows, object_symbols=object_symbols),
        build_occurrence_kind_counts_section(occurrence_rows),
    ]
    non_code_section = build_non_code_occurrence_section(non_code_occurrences)
    if non_code_section is not None and not include_strings:
        sections.append(non_code_section)
    sections.append(build_occurrence_hot_files_section(occurrence_rows))

    followups_section = _build_followups_section(matches, query, mode)
    if followups_section is not None:
        sections.append(followups_section)

    return sections


def _split_occurrences_for_render(
    occurrences: list[SearchOccurrenceV1],
    *,
    include_strings: bool,
) -> tuple[list[SearchOccurrenceV1], list[SearchOccurrenceV1]]:
    if include_strings:
        return occurrences, []
    visible: list[SearchOccurrenceV1] = []
    non_code: list[SearchOccurrenceV1] = []
    for row in occurrences:
        if is_non_code_occurrence(row.category):
            non_code.append(row)
        else:
            visible.append(row)
    return visible, non_code


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
        with_neighborhood=request.with_neighborhood,
        argv=argv,
        tc=request.tc,
        started_ms=started,
        run_id=request.run_id or uuid7_str(),
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
        with_neighborhood=bool(kwargs.get("with_neighborhood")),
        limits=_coerce_limits(kwargs.get("limits")),
        tc=cast("Toolchain | None", kwargs.get("tc")),
        argv=_coerce_argv(kwargs.get("argv")),
        started_ms=_coerce_started_ms(kwargs.get("started_ms")),
        run_id=_coerce_run_id(kwargs.get("run_id")),
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


def _coerce_run_id(run_id_value: object) -> str | None:
    if isinstance(run_id_value, str) and run_id_value.strip():
        return run_id_value
    return None


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


def run_candidate_phase(
    ctx: SmartSearchContext,
    *,
    lang: QueryLanguage,
    mode: QueryMode,
) -> tuple[list[RawMatch], SearchStats, str]:
    """Public wrapper around candidate-phase execution.

    Returns:
        tuple[list[RawMatch], SearchStats, str]:
            Raw matches, match statistics, and effective candidate pattern.
    """
    return _run_candidate_phase(ctx, lang=lang, mode=mode)


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


def run_classification_phase(
    ctx: SmartSearchContext,
    *,
    lang: QueryLanguage,
    raw_matches: list[RawMatch],
) -> list[EnrichedMatch]:
    """Public wrapper around classification phase execution.

    Returns:
        list[EnrichedMatch]: Enriched matches after classification.
    """
    return _run_classification_phase(ctx, lang=lang, raw_matches=raw_matches)


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
    python_semantic_prefetch: _PythonSemanticPrefetchResult | None = None


@dataclass(frozen=True, slots=True)
class _PythonSemanticPrefetchResult:
    payloads: dict[_PythonSemanticAnchorKey, dict[str, object]] = dataclass_field(
        default_factory=dict
    )
    attempted_keys: set[_PythonSemanticAnchorKey] = dataclass_field(default_factory=set)
    telemetry: dict[str, int] = dataclass_field(default_factory=dict)
    diagnostics: list[dict[str, object]] = dataclass_field(default_factory=list)


LanguageSearchResult = _LanguageSearchResult
PythonSemanticPrefetchResult = _PythonSemanticPrefetchResult


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
    plan = SearchPartitionPlanV1(
        root=str(ctx.root.resolve()),
        language=lang,
        query=ctx.query,
        mode=mode.value,
        include_strings=ctx.include_strings,
        include_globs=tuple(ctx.include_globs or ()),
        exclude_globs=tuple(ctx.exclude_globs or ()),
        max_total_matches=ctx.limits.max_total_matches,
        run_id=ctx.run_id,
    )
    return cast(
        "_LanguageSearchResult",
        run_search_partition(plan, ctx=ctx, mode=mode),
    )


def _merge_python_semantic_prefetch_results(
    partition_results: list[_LanguageSearchResult],
) -> _PythonSemanticPrefetchResult | None:
    payloads: dict[_PythonSemanticAnchorKey, dict[str, object]] = {}
    attempted_keys: set[_PythonSemanticAnchorKey] = set()
    telemetry = _new_python_semantic_telemetry()
    diagnostics: list[dict[str, object]] = []
    saw_prefetch = False

    for partition in partition_results:
        prefetched = partition.python_semantic_prefetch
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
    return _PythonSemanticPrefetchResult(
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
    from tools.cq.search.semantic.diagnostics import build_capability_diagnostics

    return build_capability_diagnostics(
        features=["pattern_query"],
        lang_scope=lang_scope,
    )


def _build_tree_sitter_runtime_diagnostics(
    telemetry: dict[str, object],
) -> list[Finding]:
    python_bucket = telemetry.get("python")
    if not isinstance(python_bucket, dict):
        return []
    runtime = python_bucket.get("query_runtime")
    if not isinstance(runtime, dict):
        return []
    did_exceed = int(runtime.get("did_exceed_match_limit", 0) or 0)
    cancelled = int(runtime.get("cancelled", 0) or 0)
    findings: list[Finding] = []
    if did_exceed > 0:
        findings.append(
            Finding(
                category="tree_sitter_runtime",
                message=f"tree-sitter match limit exceeded on {did_exceed} anchors",
                severity="warning",
                details=DetailPayload(
                    kind="tree_sitter_runtime",
                    data={
                        "reason": "did_exceed_match_limit",
                        "count": did_exceed,
                    },
                ),
            )
        )
    if cancelled > 0:
        findings.append(
            Finding(
                category="tree_sitter_runtime",
                message=f"tree-sitter query cancelled on {cancelled} anchors",
                severity="warning",
                details=DetailPayload(
                    kind="tree_sitter_runtime",
                    data={
                        "reason": "cancelled",
                        "count": cancelled,
                    },
                ),
            )
        )
    return findings


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
            "query_runtime": {
                "did_exceed_match_limit": 0,
                "cancelled": 0,
            },
            "stages": {
                "ast_grep": {"applied": 0, "degraded": 0, "skipped": 0},
                "python_ast": {"applied": 0, "degraded": 0, "skipped": 0},
                "import_detail": {"applied": 0, "degraded": 0, "skipped": 0},
                "python_resolution": {"applied": 0, "degraded": 0, "skipped": 0},
                "tree_sitter": {"applied": 0, "degraded": 0, "skipped": 0},
            },
            "timings_ms": {
                "ast_grep": 0.0,
                "python_ast": 0.0,
                "import_detail": 0.0,
                "python_resolution": 0.0,
                "tree_sitter": 0.0,
            },
        },
        "rust": {
            "applied": 0,
            "degraded": 0,
            "skipped": 0,
            "query_runtime": {
                "did_exceed_match_limit": 0,
                "cancelled": 0,
            },
            "query_pack_tags": 0,
            "distribution_profile_hits": 0,
            "drift_breaking_profile_hits": 0,
            "drift_removed_node_kinds": 0,
            "drift_removed_fields": 0,
        },
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
    runtime_payload = payload.get("query_runtime")
    runtime_bucket = lang_bucket.get("query_runtime")
    if isinstance(runtime_payload, dict) and isinstance(runtime_bucket, dict):
        if bool(runtime_payload.get("did_exceed_match_limit")):
            runtime_bucket["did_exceed_match_limit"] = (
                int(runtime_bucket.get("did_exceed_match_limit", 0)) + 1
            )
        if bool(runtime_payload.get("cancelled")):
            runtime_bucket["cancelled"] = int(runtime_bucket.get("cancelled", 0)) + 1


def _attach_enrichment_cache_stats(telemetry: dict[str, object]) -> None:
    rust_bucket = telemetry.get("rust")
    if isinstance(rust_bucket, dict):
        rust_bucket.update(get_tree_sitter_rust_cache_stats())
    python_bucket = telemetry.get("python")
    if isinstance(python_bucket, dict):
        python_bucket["tree_sitter_cache"] = get_tree_sitter_python_cache_stats()


def _accumulate_rust_enrichment(
    lang_bucket: dict[str, object],
    payload: dict[str, object],
) -> None:
    def _int_counter(value: object) -> int:
        return value if isinstance(value, int) and not isinstance(value, bool) else 0

    tags = payload.get("query_pack_tags")
    if isinstance(tags, list):
        lang_bucket["query_pack_tags"] = _int_counter(lang_bucket.get("query_pack_tags")) + len(
            tags
        )
    _accumulate_rust_runtime(lang_bucket=lang_bucket, payload=payload, counter=_int_counter)
    _accumulate_rust_bundle(lang_bucket=lang_bucket, payload=payload, counter=_int_counter)


def _accumulate_rust_runtime(
    *,
    lang_bucket: dict[str, object],
    payload: dict[str, object],
    counter: Callable[[object], int],
) -> None:
    runtime_payload = payload.get("query_runtime")
    runtime_bucket = lang_bucket.get("query_runtime")
    if not isinstance(runtime_payload, dict) or not isinstance(runtime_bucket, dict):
        return
    if bool(runtime_payload.get("did_exceed_match_limit")):
        runtime_bucket["did_exceed_match_limit"] = (
            counter(runtime_bucket.get("did_exceed_match_limit")) + 1
        )
    if bool(runtime_payload.get("cancelled")):
        runtime_bucket["cancelled"] = counter(runtime_bucket.get("cancelled")) + 1


def _accumulate_rust_bundle(
    *,
    lang_bucket: dict[str, object],
    payload: dict[str, object],
    counter: Callable[[object], int],
) -> None:
    bundle = payload.get("query_pack_bundle")
    if not isinstance(bundle, dict):
        return
    if bool(bundle.get("distribution_included")):
        lang_bucket["distribution_profile_hits"] = (
            counter(lang_bucket.get("distribution_profile_hits")) + 1
        )
    if bundle.get("drift_compatible") is False:
        lang_bucket["drift_breaking_profile_hits"] = (
            counter(lang_bucket.get("drift_breaking_profile_hits")) + 1
        )
    schema_diff = bundle.get("drift_schema_diff")
    if not isinstance(schema_diff, dict):
        return
    removed_nodes = schema_diff.get("removed_node_kinds")
    removed_fields = schema_diff.get("removed_fields")
    if isinstance(removed_nodes, list):
        lang_bucket["drift_removed_node_kinds"] = counter(
            lang_bucket.get("drift_removed_node_kinds")
        ) + len(removed_nodes)
    if isinstance(removed_fields, list):
        lang_bucket["drift_removed_fields"] = counter(
            lang_bucket.get("drift_removed_fields")
        ) + len(removed_fields)


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
        if not isinstance(payload, dict):
            continue
        if match.language == "python":
            _accumulate_python_enrichment(lang_bucket, payload)
        elif match.language == "rust":
            _accumulate_rust_enrichment(lang_bucket, payload)

    _attach_enrichment_cache_stats(telemetry)
    return telemetry


def _new_python_semantic_telemetry() -> dict[str, int]:
    return {
        "attempted": 0,
        "applied": 0,
        "failed": 0,
        "skipped": 0,
        "timed_out": 0,
    }


def new_python_semantic_telemetry() -> dict[str, int]:
    """Public wrapper for constructing default semantic telemetry.

    Returns:
        dict[str, int]: Initialized telemetry counters.
    """
    return _new_python_semantic_telemetry()


def run_prefetch_python_semantic_for_raw_matches(
    ctx: SmartSearchContext,
    *,
    lang: QueryLanguage,
    raw_matches: list[RawMatch],
) -> _PythonSemanticPrefetchResult:
    """Public wrapper around python semantic prefetch execution.

    Returns:
        _PythonSemanticPrefetchResult: Prefetch payloads and telemetry.
    """
    return _prefetch_python_semantic_for_raw_matches(ctx, lang=lang, raw_matches=raw_matches)


def _python_semantic_anchor_key(
    *,
    file: str,
    line: int,
    col: int,
    match_text: str,
) -> _PythonSemanticAnchorKey:
    return (file, line, col, match_text)


def _python_semantic_anchor_key_from_raw(raw: RawMatch) -> _PythonSemanticAnchorKey:
    return _python_semantic_anchor_key(
        file=raw.file,
        line=raw.line,
        col=raw.col,
        match_text=raw.match_text,
    )


def _python_semantic_anchor_key_from_match(match: EnrichedMatch) -> _PythonSemanticAnchorKey:
    return _python_semantic_anchor_key(
        file=match.file,
        line=match.line,
        col=match.col,
        match_text=match.match_text,
    )


def _python_semantic_failure_diagnostic(
    *,
    reason: str = "session_unavailable",
) -> dict[str, object]:
    return {
        "code": "PYTHON_SEMANTIC001",
        "severity": "warning",
        "message": "PythonSemantic enrichment unavailable for one or more anchors",
        "reason": reason,
    }


def _python_semantic_no_signal_diagnostic(
    reasons: tuple[str, ...],
    *,
    coverage_reason: str | None = None,
) -> dict[str, object]:
    reason_text = _normalize_python_semantic_degradation_reason(
        reasons=reasons,
        coverage_reason=coverage_reason,
    )
    return {
        "code": "PYTHON_SEMANTIC002",
        "severity": "info",
        "message": "PythonSemantic payload resolved but contained no actionable signal",
        "reason": reason_text,
    }


def _python_semantic_partial_signal_diagnostic(
    reasons: tuple[str, ...],
    *,
    coverage_reason: str | None = None,
) -> dict[str, object]:
    reason_text = _normalize_python_semantic_degradation_reason(
        reasons=reasons,
        coverage_reason=coverage_reason,
    )
    return {
        "code": "PYTHON_SEMANTIC003",
        "severity": "info",
        "message": "PythonSemantic payload retained with partial signal",
        "reason": reason_text,
    }


def _normalize_python_semantic_degradation_reason(
    *,
    reasons: tuple[str, ...],
    coverage_reason: str | None,
) -> str:
    explicit_reasons = {
        "unsupported_capability",
        "capability_probe_unavailable",
        "request_interface_unavailable",
        "request_failed",
        "request_timeout",
        "session_unavailable",
        "timeout",
        "no_signal",
    }
    normalized_coverage_reason = coverage_reason
    if normalized_coverage_reason and normalized_coverage_reason.startswith(
        "no_python_semantic_signal:"
    ):
        normalized_coverage_reason = normalized_coverage_reason.removeprefix(
            "no_python_semantic_signal:"
        )
    if normalized_coverage_reason == "timeout":
        normalized_coverage_reason = "request_timeout"
    if (
        isinstance(normalized_coverage_reason, str)
        and normalized_coverage_reason in explicit_reasons
    ):
        return normalized_coverage_reason
    if coverage_reason:
        return "no_signal"

    for reason in reasons:
        normalized_reason = "request_timeout" if reason == "timeout" else reason
        if normalized_reason in explicit_reasons and normalized_reason != "no_signal":
            return normalized_reason
    return "no_signal"


def _python_semantic_coverage_reason(payload: dict[str, object]) -> str | None:
    coverage = payload.get("coverage")
    if not isinstance(coverage, dict):
        return None
    reason = coverage.get("reason")
    if isinstance(reason, str) and reason:
        return reason
    return None


def _prefetch_python_semantic_for_raw_matches(
    ctx: SmartSearchContext,
    *,
    lang: QueryLanguage,
    raw_matches: list[RawMatch],
) -> _PythonSemanticPrefetchResult:
    if lang != "python" or not raw_matches or not semantic_runtime_enabled():
        return _PythonSemanticPrefetchResult(telemetry=_new_python_semantic_telemetry())

    telemetry = _new_python_semantic_telemetry()
    payloads: dict[_PythonSemanticAnchorKey, dict[str, object]] = {}
    attempted_keys: set[_PythonSemanticAnchorKey] = set()
    diagnostics: list[dict[str, object]] = []
    python_semantic_budget_used = 0

    for raw in raw_matches:
        if python_semantic_budget_used >= MAX_PYTHON_SEMANTIC_ENRICH_FINDINGS:
            telemetry["skipped"] += 1
            continue
        if not is_path_in_lang_scope(raw.file, lang):
            continue
        key = _python_semantic_anchor_key_from_raw(raw)
        if key in attempted_keys:
            continue
        attempted_keys.add(key)
        python_semantic_budget_used += 1
        telemetry["attempted"] += 1
        file_path = ctx.root / raw.file
        try:
            outcome = enrich_with_language_semantics(
                LanguageSemanticEnrichmentRequest(
                    language="python",
                    mode="search",
                    root=ctx.root,
                    file_path=file_path,
                    line=raw.line,
                    col=raw.col,
                    run_id=ctx.run_id,
                    symbol_hint=raw.match_text,
                )
            )
        except _PYTHON_SEMANTIC_PREFETCH_NON_FATAL_EXCEPTIONS:
            telemetry["failed"] += 1
            diagnostics.append(_python_semantic_failure_diagnostic(reason="request_failed"))
            continue
        payload = outcome.payload if isinstance(outcome.payload, dict) else None
        if outcome.timed_out:
            telemetry["timed_out"] += 1

        if isinstance(payload, dict) and payload:
            has_signal, reasons = evaluate_python_semantic_signal_from_mapping(payload)
            if has_signal:
                telemetry["applied"] += 1
                payloads[key] = payload
            else:
                telemetry["applied"] += 1
                payloads[key] = payload
                diagnostics.append(
                    _python_semantic_partial_signal_diagnostic(
                        reasons,
                        coverage_reason=_python_semantic_coverage_reason(payload),
                    )
                )
        else:
            telemetry["failed"] += 1
            diagnostics.append(
                _python_semantic_failure_diagnostic(
                    reason=_normalize_python_semantic_degradation_reason(
                        reasons=(outcome.failure_reason,)
                        if isinstance(outcome.failure_reason, str)
                        else (),
                        coverage_reason=outcome.failure_reason,
                    )
                )
            )

    return _PythonSemanticPrefetchResult(
        payloads=payloads,
        attempted_keys=attempted_keys,
        telemetry=telemetry,
        diagnostics=diagnostics,
    )


def _python_semantic_enrich_match(
    ctx: SmartSearchContext,
    match: EnrichedMatch,
) -> LanguageSemanticEnrichmentOutcome:
    if match.language != "python":
        return LanguageSemanticEnrichmentOutcome(failure_reason="provider_unavailable")
    if not semantic_runtime_enabled():
        return LanguageSemanticEnrichmentOutcome(failure_reason="not_attempted_runtime_disabled")
    file_path = ctx.root / match.file
    if file_path.suffix not in {".py", ".pyi"}:
        return LanguageSemanticEnrichmentOutcome(failure_reason="provider_unavailable")
    return enrich_with_language_semantics(
        LanguageSemanticEnrichmentRequest(
            language="python",
            mode="search",
            root=ctx.root,
            file_path=file_path,
            line=match.line,
            col=match.col,
            run_id=ctx.run_id,
            symbol_hint=match.match_text,
        )
    )


def _seed_python_semantic_state(
    prefetched: _PythonSemanticPrefetchResult | None,
) -> tuple[dict[str, int], list[dict[str, object]]]:
    telemetry = _new_python_semantic_telemetry()
    diagnostics: list[dict[str, object]] = []
    if prefetched is None:
        return telemetry, diagnostics
    diagnostics.extend(prefetched.diagnostics)
    for key, value in prefetched.telemetry.items():
        if key in telemetry:
            telemetry[key] += int(value)
    return telemetry, diagnostics


def _python_semantic_payload_from_prefetch(
    prefetched: _PythonSemanticPrefetchResult | None,
    key: _PythonSemanticAnchorKey,
) -> dict[str, object] | None:
    if prefetched is None or key not in prefetched.attempted_keys:
        return None
    return prefetched.payloads.get(key)


def _fetch_python_semantic_payload(
    *,
    ctx: SmartSearchContext,
    match: EnrichedMatch,
    prefetched: _PythonSemanticPrefetchResult | None,
    telemetry: dict[str, int],
) -> tuple[dict[str, object] | None, bool, str | None]:
    if not semantic_runtime_enabled():
        return None, False, None
    key = _python_semantic_anchor_key_from_match(match)
    prefetched_payload = _python_semantic_payload_from_prefetch(prefetched, key)
    if prefetched is not None and key in prefetched.attempted_keys:
        return prefetched_payload, False, None

    telemetry["attempted"] += 1
    try:
        outcome = _python_semantic_enrich_match(ctx, match)
    except _PYTHON_SEMANTIC_PREFETCH_NON_FATAL_EXCEPTIONS:
        return None, True, "request_failed"
    if outcome.timed_out:
        telemetry["timed_out"] += 1
    return outcome.payload, True, outcome.failure_reason


def _merge_match_with_python_semantic_payload(
    *,
    match: EnrichedMatch,
    payload: dict[str, object] | None,
    attempted_in_place: bool,
    failure_reason: str | None,
    telemetry: dict[str, int],
    diagnostics: list[dict[str, object]],
) -> EnrichedMatch:
    if isinstance(payload, dict) and payload:
        has_signal, reasons = evaluate_python_semantic_signal_from_mapping(payload)
        if not has_signal:
            if attempted_in_place:
                telemetry["applied"] += 1
                diagnostics.append(
                    _python_semantic_partial_signal_diagnostic(
                        reasons,
                        coverage_reason=_python_semantic_coverage_reason(payload),
                    )
                )
            return msgspec.structs.replace(match, python_semantic_enrichment=payload)
        if attempted_in_place:
            telemetry["applied"] += 1
        return msgspec.structs.replace(match, python_semantic_enrichment=payload)

    if attempted_in_place:
        telemetry["failed"] += 1
        diagnostics.append(
            _python_semantic_failure_diagnostic(
                reason=_normalize_python_semantic_degradation_reason(
                    reasons=(failure_reason,) if isinstance(failure_reason, str) else (),
                    coverage_reason=failure_reason,
                ),
            )
        )
    return match


def _python_semantic_summary_payload(
    *,
    overview: dict[str, object],
    telemetry: dict[str, int],
    diagnostics: list[dict[str, object]],
) -> tuple[dict[str, object], dict[str, object], list[dict[str, object]]]:
    telemetry_payload = coerce_python_semantic_telemetry(telemetry)
    diagnostics_payload = coerce_python_semantic_diagnostics(diagnostics)
    return (
        to_public_dict(coerce_python_semantic_overview(overview)),
        to_public_dict(telemetry_payload),
        to_public_list(diagnostics_payload),
    )


def _attach_python_semantic_enrichment(
    *,
    ctx: SmartSearchContext,
    matches: list[EnrichedMatch],
    prefetched: _PythonSemanticPrefetchResult | None = None,
) -> tuple[list[EnrichedMatch], dict[str, object], dict[str, object], list[dict[str, object]]]:
    if not semantic_runtime_enabled():
        return matches, {}, cast("dict[str, object]", _new_python_semantic_telemetry()), []

    telemetry, diagnostics = _seed_python_semantic_state(prefetched)

    if not matches:
        overview, telemetry_map, diagnostic_rows = _python_semantic_summary_payload(
            overview={},
            telemetry=telemetry,
            diagnostics=diagnostics,
        )
        return matches, overview, telemetry_map, diagnostic_rows

    enriched: list[EnrichedMatch] = []
    python_semantic_budget_used = 0

    for match in matches:
        if match.language != "python":
            enriched.append(match)
            continue
        if python_semantic_budget_used >= MAX_PYTHON_SEMANTIC_ENRICH_FINDINGS:
            telemetry["skipped"] += 1
            enriched.append(match)
            continue
        python_semantic_budget_used += 1
        payload, attempted_in_place, failure_reason = _fetch_python_semantic_payload(
            ctx=ctx,
            match=match,
            prefetched=prefetched,
            telemetry=telemetry,
        )
        enriched.append(
            _merge_match_with_python_semantic_payload(
                match=match,
                payload=payload,
                attempted_in_place=attempted_in_place,
                failure_reason=failure_reason,
                telemetry=telemetry,
                diagnostics=diagnostics,
            )
        )

    overview = _build_python_semantic_overview(enriched)
    overview_map, telemetry_map, diagnostic_rows = _python_semantic_summary_payload(
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


def _register_search_object_view(
    *,
    run_id: str | None,
    view: SearchObjectResolvedViewV1,
) -> None:
    if not isinstance(run_id, str) or not run_id:
        return
    _SEARCH_OBJECT_VIEW_REGISTRY[run_id] = view


def pop_search_object_view_for_run(run_id: str | None) -> SearchObjectResolvedViewV1 | None:
    """Pop object-resolved search view payload for a completed run.

    Returns:
        SearchObjectResolvedViewV1 | None: Popped view if run_id exists and is cached.
    """
    if not isinstance(run_id, str) or not run_id:
        return None
    return _SEARCH_OBJECT_VIEW_REGISTRY.pop(run_id, None)


@dataclass(slots=True)
class _PythonSemanticOverviewAccumulator:
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


def _accumulate_python_semantic_overview(
    *,
    acc: _PythonSemanticOverviewAccumulator,
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


def _build_python_semantic_overview(matches: list[EnrichedMatch]) -> dict[str, object]:
    acc = _PythonSemanticOverviewAccumulator()
    for match in matches:
        payload = match.python_semantic_enrichment
        if isinstance(payload, dict):
            _accumulate_python_semantic_overview(
                acc=acc, payload=payload, match_text=match.match_text
            )
    return {
        "primary_symbol": acc.primary_symbol,
        "enclosing_class": acc.enclosing_class,
        "total_incoming_callers": acc.total_incoming,
        "total_outgoing_callees": acc.total_outgoing,
        "total_implementations": acc.total_implementations,
        "targeted_diagnostics": acc.diagnostics,
        "matches_enriched": acc.enriched_count,
    }


def _merge_matches_and_python_semantic(
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
    prefetched_python_semantic = _merge_python_semantic_prefetch_results(partition_results)
    return _attach_python_semantic_enrichment(
        ctx=ctx,
        matches=enriched_matches,
        prefetched=prefetched_python_semantic,
    )


def _build_search_summary(
    ctx: SmartSearchContext,
    partition_results: list[_LanguageSearchResult],
    enriched_matches: list[EnrichedMatch],
    *,
    python_semantic_overview: dict[str, object],
    python_semantic_telemetry: dict[str, object],
    python_semantic_diagnostics: list[dict[str, object]],
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
    query_pack_lint = lint_search_query_packs()
    summary["query_pack_lint"] = {
        "status": query_pack_lint.status,
        "errors": list(query_pack_lint.errors),
    }
    if query_pack_lint.status != "ok":
        all_diagnostics.append(
            Finding(
                category="query_pack_lint",
                message=f"Query pack lint failed: {len(query_pack_lint.errors)} errors",
                severity="warning",
                details=DetailPayload(
                    kind="query_pack_lint",
                    data={"errors": list(query_pack_lint.errors)},
                ),
            )
        )
    enrichment_telemetry = _build_enrichment_telemetry(enriched_matches)
    all_diagnostics.extend(_build_tree_sitter_runtime_diagnostics(enrichment_telemetry))
    summary["cross_language_diagnostics"] = diagnostics_to_summary_payload(all_diagnostics)
    summary["language_capabilities"] = build_language_capabilities(lang_scope=ctx.lang_scope)
    summary["enrichment_telemetry"] = enrichment_telemetry
    summary["python_semantic_overview"] = python_semantic_overview
    summary["python_semantic_telemetry"] = python_semantic_telemetry
    summary.setdefault(
        "rust_semantic_telemetry",
        {"attempted": 0, "applied": 0, "failed": 0, "skipped": 0, "timed_out": 0},
    )
    summary["python_semantic_diagnostics"] = python_semantic_diagnostics
    if dropped_by_scope:
        summary["dropped_by_scope"] = dropped_by_scope
    assert_multilang_summary(summary)
    return summary, all_diagnostics


def _collect_definition_candidates(
    ctx: SmartSearchContext,
    object_runtime: ObjectResolutionRuntime,
) -> tuple[list[EnrichedMatch], list[Finding]]:
    _ = ctx
    definition_matches: list[EnrichedMatch] = []
    candidate_findings: list[Finding] = []

    for summary in object_runtime.view.summaries:
        object_ref = summary.object_ref
        kind = _normalize_target_candidate_kind(object_ref.kind)
        if kind not in _TARGET_CANDIDATE_KINDS:
            continue

        representative = object_runtime.representative_matches.get(object_ref.object_id)
        if representative is not None:
            definition_matches.append(representative)
        candidate = _build_object_candidate_finding(
            summary=summary,
            representative=representative,
        )
        candidate_findings.append(candidate)
        if len(candidate_findings) >= MAX_TARGET_CANDIDATES:
            break

    if candidate_findings:
        return definition_matches, candidate_findings

    # Fallback: always provide at least one target candidate from top object.
    if not object_runtime.view.summaries:
        return definition_matches, candidate_findings
    fallback_summary = object_runtime.view.summaries[0]
    representative = object_runtime.representative_matches.get(
        fallback_summary.object_ref.object_id
    )
    if representative is not None:
        definition_matches.append(representative)
    candidate_findings.append(
        _build_object_candidate_finding(summary=fallback_summary, representative=representative)
    )
    return definition_matches, candidate_findings


def _normalize_target_candidate_kind(kind: str | None) -> str:
    if not isinstance(kind, str) or not kind:
        return "reference"
    lowered = kind.lower()
    if lowered in {"method", "function", "callable"}:
        return "function"
    if lowered in {"class"}:
        return "class"
    if lowered in {"module"}:
        return "module"
    if lowered in {"type"}:
        return "type"
    return lowered


def _build_object_candidate_finding(
    *,
    summary: SearchObjectSummaryV1,
    representative: EnrichedMatch | None,
) -> Finding:
    object_ref = summary.object_ref
    kind = _normalize_target_candidate_kind(object_ref.kind)
    symbol = object_ref.symbol
    anchor: Anchor | None = None
    if isinstance(object_ref.canonical_file, str) and isinstance(object_ref.canonical_line, int):
        anchor = Anchor(
            file=object_ref.canonical_file,
            line=max(1, int(object_ref.canonical_line)),
            col=representative.col if representative is not None else None,
        )
    elif representative is not None:
        anchor = Anchor.from_span(representative.span)
    details = {
        "name": symbol,
        "kind": kind,
        "qualified_name": object_ref.qualified_name,
        "signature": representative.text.strip() if representative is not None else symbol,
        "object_id": object_ref.object_id,
        "language": representative.language if representative is not None else object_ref.language,
        "occurrence_count": summary.occurrence_count,
        "resolution_quality": object_ref.resolution_quality,
    }
    return Finding(
        category="definition",
        message=f"{kind}: {symbol}",
        anchor=anchor,
        severity="info",
        details=DetailPayload(kind=kind, data=cast("dict[str, object]", details)),
    )


def _resolve_primary_target_match(
    *,
    candidate_findings: list[Finding],
    object_runtime: ObjectResolutionRuntime,
    definition_matches: list[EnrichedMatch],
    enriched_matches: list[EnrichedMatch],
) -> EnrichedMatch | None:
    if candidate_findings:
        object_id = candidate_findings[0].details.get("object_id")
        if isinstance(object_id, str):
            representative = object_runtime.representative_matches.get(object_id)
            if representative is not None:
                return representative
    if definition_matches:
        return definition_matches[0]
    if object_runtime.view.summaries:
        object_id = object_runtime.view.summaries[0].object_ref.object_id
        representative = object_runtime.representative_matches.get(object_id)
        if representative is not None:
            return representative
    if enriched_matches:
        return enriched_matches[0]
    return None


def _build_structural_neighborhood_preview(
    ctx: SmartSearchContext,
    *,
    primary_target_finding: Finding | None,
    definition_matches: list[EnrichedMatch],
) -> tuple[InsightNeighborhoodV1 | None, list[Finding], list[str]]:
    from tools.cq.core.front_door_insight import build_neighborhood_from_slices
    from tools.cq.neighborhood.tree_sitter_collector import collect_tree_sitter_neighborhood
    from tools.cq.neighborhood.tree_sitter_contracts import TreeSitterNeighborhoodCollectRequest

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
    collector_language = "rust" if target_language == "rust" else "python"
    try:
        collect_result = collect_tree_sitter_neighborhood(
            TreeSitterNeighborhoodCollectRequest(
                root=str(ctx.root),
                target_name=target_name,
                target_file=target_file,
                language=collector_language,
                target_line=primary_target_finding.anchor.line,
                target_col=int(primary_target_finding.anchor.col or 0),
                max_per_slice=5,
            )
        )
    except (OSError, RuntimeError, TimeoutError, ValueError, TypeError) as exc:
        return None, [], [f"tree_sitter_neighborhood_unavailable:{type(exc).__name__}"]

    slices = tuple(collect_result.slices)
    degrades = tuple(collect_result.diagnostics)

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


def _ast_grep_prefilter_scope_paths(
    scope_paths: tuple[Path, ...] | None,
    *,
    lang: QueryLanguage,
) -> tuple[Path, ...] | None:
    if not scope_paths:
        return None
    filtered: list[Path] = []
    for path in scope_paths:
        if path.is_file():
            if get_sg_root(path, lang=lang) is not None:
                filtered.append(path)
            continue
        filtered.append(path)
    return tuple(filtered) if filtered else scope_paths


def _candidate_scope_paths_for_neighborhood(
    *,
    ctx: SmartSearchContext,
    partition_results: list[_LanguageSearchResult],
) -> tuple[Path, ...]:
    ordered_paths: dict[str, Path] = {}
    for partition in partition_results:
        for match in partition.raw_matches:
            rel_path = str(match.file).strip()
            if not rel_path or rel_path in ordered_paths:
                continue
            candidate = (ctx.root / rel_path).resolve()
            if candidate.exists():
                ordered_paths[rel_path] = candidate
    return tuple(ordered_paths.values())


def _apply_search_semantic_insight(
    *,
    ctx: SmartSearchContext,
    insight: FrontDoorInsightV1,
    summary: dict[str, object],
    primary_target_finding: Finding | None,
    primary_target_match: EnrichedMatch | None,
) -> FrontDoorInsightV1:
    from tools.cq.core.front_door_insight import augment_insight_with_semantic

    outcome = _collect_search_semantic_outcome(
        ctx=ctx,
        primary_target_finding=primary_target_finding,
        primary_target_match=primary_target_match,
    )
    if outcome.payload is not None:
        insight = augment_insight_with_semantic(insight, outcome.payload)
        semantic_planes = outcome.payload.get("semantic_planes")
        if isinstance(semantic_planes, dict):
            summary["semantic_planes"] = dict(semantic_planes)
    _update_search_summary_semantic_telemetry(summary, outcome)
    semantic_state = _derive_search_semantic_state(summary, outcome)
    return msgspec.structs.replace(
        insight,
        degradation=msgspec.structs.replace(
            insight.degradation,
            semantic=semantic_state.status,
            notes=tuple(dict.fromkeys([*insight.degradation.notes, *semantic_state.reasons])),
        ),
    )


@dataclass(slots=True)
class _SearchSemanticOutcome:
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
    enriched_matches: list[EnrichedMatch]
    object_runtime: ObjectResolutionRuntime
    summary: dict[str, object]
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
    primary_target_finding: Finding | None
    definition_matches: list[EnrichedMatch]
    has_target_candidates: bool


@dataclass(frozen=True, slots=True)
class SearchResultAssembly:
    """Assembly inputs for compatibility with package-level orchestrators."""

    context: SmartSearchContext
    partition_results: list[object]


SearchContext = SmartSearchContext


def _payload_coverage_status(payload: dict[str, object]) -> tuple[str | None, str | None]:
    status = payload.get("enrichment_status")
    if isinstance(status, str):
        reason = payload.get("degrade_reason")
        return status, reason if isinstance(reason, str) and reason else None
    coverage = payload.get("coverage")
    if isinstance(coverage, dict):
        cov_status = coverage.get("status")
        cov_reason = coverage.get("reason")
        return (
            cov_status if isinstance(cov_status, str) else None,
            cov_reason if isinstance(cov_reason, str) and cov_reason else None,
        )
    return None, None


def _rust_payload_has_signal(payload: dict[str, object]) -> bool:
    def _has_rows(container: object, keys: tuple[str, ...]) -> bool:
        if not isinstance(container, dict):
            return False
        return any(_count_mapping_rows(container.get(key)) > 0 for key in keys)

    has_signal = (
        _has_rows(
            payload.get("symbol_grounding"),
            ("definitions", "type_definitions", "implementations", "references"),
        )
        or _has_rows(payload.get("call_graph"), ("incoming_callers", "outgoing_callees"))
        or _has_rows(payload.get("type_hierarchy"), ("supertypes", "subtypes"))
        or _count_mapping_rows(payload.get("document_symbols")) > 0
        or _count_mapping_rows(payload.get("diagnostics")) > 0
    )
    if has_signal:
        return True
    hover = payload.get("hover_text")
    return isinstance(hover, str) and bool(hover.strip())


def _rust_payload_reason(payload: dict[str, object]) -> str | None:
    semantic_planes = payload.get("semantic_planes")
    if isinstance(semantic_planes, dict):
        degradation = semantic_planes.get("degradation")
        if isinstance(degradation, list):
            for reason in degradation:
                if isinstance(reason, str) and reason:
                    return reason
        reason = semantic_planes.get("reason")
        if isinstance(reason, str) and reason:
            return reason
    degrade_events = payload.get("degrade_events")
    if isinstance(degrade_events, list):
        for event in degrade_events:
            if not isinstance(event, dict):
                continue
            category = event.get("category")
            if isinstance(category, str) and category:
                return category
    return None


def _resolve_search_semantic_target(
    *,
    ctx: SmartSearchContext,
    primary_target_finding: Finding | None,
    primary_target_match: EnrichedMatch | None,
) -> tuple[Path, QueryLanguage, Anchor, str] | None:
    if primary_target_finding is None and primary_target_match is None:
        return None

    anchor = primary_target_finding.anchor if primary_target_finding is not None else None
    if anchor is None and primary_target_match is not None:
        anchor = Anchor.from_span(primary_target_match.span)
    if anchor is None:
        return None

    target_file_path = ctx.root / anchor.file
    inferred_language = infer_language_for_path(target_file_path)
    target_language = (
        primary_target_match.language
        if primary_target_match is not None and primary_target_match.language in {"python", "rust"}
        else inferred_language
    )
    if target_language not in {"python", "rust"}:
        return None

    name_value = primary_target_finding.details.get("name") if primary_target_finding else None
    symbol_hint = str(name_value) if isinstance(name_value, str) else None
    if not symbol_hint and primary_target_match is not None:
        symbol_hint = primary_target_match.match_text
    if not symbol_hint:
        symbol_hint = ctx.query
    return target_file_path, target_language, anchor, symbol_hint


def _apply_prefetched_search_semantic_outcome(
    *,
    outcome: _SearchSemanticOutcome,
    target_language: QueryLanguage,
    primary_target_match: EnrichedMatch | None,
) -> bool:
    if (
        target_language != "python"
        or primary_target_match is None
        or not isinstance(primary_target_match.python_semantic_enrichment, dict)
    ):
        return False

    prefetch_status, prefetch_reason = _payload_coverage_status(
        primary_target_match.python_semantic_enrichment
    )
    if prefetch_status in {"applied", "degraded", "partial_signal", "structural_only"}:
        outcome.payload = primary_target_match.python_semantic_enrichment
        outcome.applied = 1
    elif primary_target_match.python_semantic_enrichment:
        # Retain closest-fit payload even when strict signal threshold is not met.
        outcome.payload = primary_target_match.python_semantic_enrichment
        outcome.applied = 1
        if prefetch_reason:
            outcome.reasons.append(prefetch_reason)
    else:
        outcome.failed = 1
        outcome.reasons.append(
            _normalize_python_semantic_degradation_reason(
                reasons=(),
                coverage_reason=prefetch_reason,
            )
        )
    return True


def _apply_search_semantic_payload_outcome(
    *,
    outcome: _SearchSemanticOutcome,
    target_language: QueryLanguage,
    payload: dict[str, object] | None,
    timed_out: bool,
    failure_reason: str | None,
) -> None:
    outcome.payload = payload
    outcome.timed_out = int(timed_out)
    if payload is None:
        outcome.failed = 1
        normalized_reason = (
            _normalize_python_semantic_degradation_reason(
                reasons=(failure_reason,) if isinstance(failure_reason, str) else (),
                coverage_reason=failure_reason,
            )
            if target_language == "python"
            else (failure_reason or ("request_timeout" if timed_out else "request_failed"))
        )
        outcome.reasons.append(normalized_reason)
        return

    if target_language == "rust":
        if _rust_payload_has_signal(payload):
            outcome.applied = 1
            return
        outcome.failed = 1
        outcome.reasons.append(_rust_payload_reason(payload) or failure_reason or "no_signal")
        outcome.payload = None
        return

    payload_status, payload_reason = _payload_coverage_status(payload)
    if payload_status in {"applied", "degraded", "partial_signal", "structural_only"}:
        outcome.applied = 1
        return
    if payload:
        outcome.applied = 1
        if payload_reason:
            outcome.reasons.append(payload_reason)
        return

    outcome.failed = 1
    outcome.reasons.append(
        _normalize_python_semantic_degradation_reason(
            reasons=(failure_reason,) if isinstance(failure_reason, str) else (),
            coverage_reason=payload_reason or failure_reason,
        )
    )
    outcome.payload = None


def _collect_search_semantic_outcome(
    *,
    ctx: SmartSearchContext,
    primary_target_finding: Finding | None,
    primary_target_match: EnrichedMatch | None,
) -> _SearchSemanticOutcome:
    outcome = _SearchSemanticOutcome()
    resolved = _resolve_search_semantic_target(
        ctx=ctx,
        primary_target_finding=primary_target_finding,
        primary_target_match=primary_target_match,
    )
    if resolved is None:
        if primary_target_match is not None or primary_target_finding is not None:
            outcome.reasons.append("provider_unavailable")
        return outcome

    target_file_path, target_language, anchor, symbol_hint = resolved

    outcome.provider = provider_for_language(target_language)
    outcome.target_language = target_language
    if not semantic_runtime_enabled():
        outcome.reasons.append("not_attempted_runtime_disabled")
        return outcome

    outcome.attempted = 1
    if _apply_prefetched_search_semantic_outcome(
        outcome=outcome,
        target_language=target_language,
        primary_target_match=primary_target_match,
    ):
        return outcome

    semantic_outcome = enrich_with_language_semantics(
        LanguageSemanticEnrichmentRequest(
            language=target_language,
            mode="search",
            root=ctx.root,
            file_path=target_file_path,
            line=max(1, int(anchor.line)),
            col=int(anchor.col or 0),
            run_id=ctx.run_id,
            symbol_hint=symbol_hint,
        )
    )
    _apply_search_semantic_payload_outcome(
        outcome=outcome,
        target_language=target_language,
        payload=semantic_outcome.payload,
        timed_out=semantic_outcome.timed_out,
        failure_reason=semantic_outcome.failure_reason,
    )
    return outcome


def _update_search_summary_semantic_telemetry(
    summary: dict[str, object],
    outcome: _SearchSemanticOutcome,
) -> None:
    if outcome.attempted <= 0 or outcome.target_language not in {"python", "rust"}:
        return
    telemetry_key = (
        "python_semantic_telemetry"
        if outcome.target_language == "python"
        else "rust_semantic_telemetry"
    )
    telemetry = summary.get(telemetry_key)
    if not isinstance(telemetry, dict):
        return
    telemetry["attempted"] = int(telemetry.get("attempted", 0) or 0) + outcome.attempted
    telemetry["applied"] = int(telemetry.get("applied", 0) or 0) + outcome.applied
    telemetry["failed"] = int(telemetry.get("failed", 0) or 0) + outcome.failed
    telemetry["timed_out"] = int(telemetry.get("timed_out", 0) or 0) + outcome.timed_out


def _read_semantic_telemetry(
    summary: dict[str, object],
    *,
    language: QueryLanguage | None,
) -> tuple[int, int, int, int]:
    def _read_entry(raw: object) -> tuple[int, int, int, int]:
        if not isinstance(raw, dict):
            return 0, 0, 0, 0
        attempted = raw.get("attempted")
        applied = raw.get("applied")
        failed = raw.get("failed")
        timed_out = raw.get("timed_out")
        return (
            attempted if isinstance(attempted, int) else 0,
            applied if isinstance(applied, int) else 0,
            failed if isinstance(failed, int) else 0,
            timed_out if isinstance(timed_out, int) else 0,
        )

    if language == "python":
        return _read_entry(summary.get("python_semantic_telemetry"))
    if language == "rust":
        return _read_entry(summary.get("rust_semantic_telemetry"))

    py = _read_entry(summary.get("python_semantic_telemetry"))
    rust = _read_entry(summary.get("rust_semantic_telemetry"))
    return (
        py[0] + rust[0],
        py[1] + rust[1],
        py[2] + rust[2],
        py[3] + rust[3],
    )


def _derive_semantic_provider_from_summary(summary: dict[str, object]) -> SemanticProvider:
    py_attempted, _py_applied, _py_failed, _py_timed_out = _read_semantic_telemetry(
        summary, language="python"
    )
    rust_attempted, _rs_applied, _rs_failed, _rs_timed_out = _read_semantic_telemetry(
        summary, language="rust"
    )
    if rust_attempted > 0 and py_attempted <= 0:
        return "rust_static"
    if py_attempted > 0:
        return "python_static"
    if rust_attempted > 0:
        return "rust_static"
    return "none"


def _derive_search_semantic_state(
    summary: dict[str, object],
    outcome: _SearchSemanticOutcome,
) -> SemanticContractStateV1:
    provider_value: SemanticProvider
    if outcome.provider in {"python_static", "rust_static"}:
        provider_value = outcome.provider
    elif outcome.provider == "none":
        provider_value = _derive_semantic_provider_from_summary(summary)
    else:
        provider_value = "none"
    attempted, applied, failed, timed_out = _read_semantic_telemetry(
        summary,
        language=cast("QueryLanguage | None", outcome.target_language),
    )
    reasons = list(outcome.reasons)
    if provider_value == "none":
        reasons.append("provider_unavailable")
    elif attempted <= 0 and "not_attempted_runtime_disabled" not in reasons:
        reasons.append("not_attempted_by_design")
    if outcome.attempted > 0 and outcome.applied <= 0 and applied > 0:
        reasons.append("top_target_failed")
    return derive_semantic_contract_state(
        SemanticContractStateInputV1(
            provider=provider_value,
            available=provider_value != "none",
            attempted=attempted,
            applied=applied,
            failed=max(failed, attempted - applied if attempted > applied else 0),
            timed_out=timed_out,
            reasons=tuple(dict.fromkeys(reasons)),
        )
    )


def _build_tree_sitter_neighborhood_preview(
    *,
    ctx: SmartSearchContext,
    partition_results: list[_LanguageSearchResult],
    summary: dict[str, object],
    sections: list[Section],
    inputs: _NeighborhoodPreviewInputs,
) -> tuple[InsightNeighborhoodV1 | None, list[str]]:
    summary["tree_sitter_neighborhood"] = {
        "enabled": bool(ctx.with_neighborhood),
        "mode": "opt_in",
    }
    if not ctx.with_neighborhood:
        return None, ["tree_sitter_neighborhood_disabled_by_default"]

    scope_paths = _candidate_scope_paths_for_neighborhood(
        ctx=ctx,
        partition_results=partition_results,
    )
    summary["tree_sitter_neighborhood"]["candidate_scope_files"] = str(len(scope_paths))
    insight_neighborhood, neighborhood_findings, neighborhood_notes = (
        _build_structural_neighborhood_preview(
            ctx,
            primary_target_finding=inputs.primary_target_finding,
            definition_matches=inputs.definition_matches,
        )
    )
    insert_neighborhood_preview(
        sections,
        findings=neighborhood_findings,
        has_target_candidates=inputs.has_target_candidates,
    )
    return insight_neighborhood, neighborhood_notes


def _prepare_search_assembly_inputs(
    ctx: SmartSearchContext,
    partition_results: list[_LanguageSearchResult],
) -> _SearchAssemblyInputs:
    (
        enriched_matches,
        python_semantic_overview,
        python_semantic_telemetry,
        python_semantic_diagnostics,
    ) = _merge_matches_and_python_semantic(ctx, partition_results)
    summary, all_diagnostics = _build_search_summary(
        ctx,
        partition_results,
        enriched_matches,
        python_semantic_overview=python_semantic_overview,
        python_semantic_telemetry=python_semantic_telemetry,
        python_semantic_diagnostics=python_semantic_diagnostics,
    )
    object_runtime = build_object_resolved_view(enriched_matches, query=ctx.query)
    summary["resolved_objects"] = len(object_runtime.view.summaries)
    summary["resolved_occurrences"] = len(object_runtime.view.occurrences)
    sections = build_sections(
        enriched_matches,
        ctx.root,
        ctx.query,
        ctx.mode,
        include_strings=ctx.include_strings,
        object_runtime=object_runtime,
    )
    if all_diagnostics:
        sections.append(Section(title="Cross-Language Diagnostics", findings=all_diagnostics))
    definition_matches, candidate_findings = _collect_definition_candidates(ctx, object_runtime)
    insert_target_candidates(sections, candidates=candidate_findings)
    primary_target_finding = candidate_findings[0] if candidate_findings else None
    primary_target_match = _resolve_primary_target_match(
        candidate_findings=candidate_findings,
        object_runtime=object_runtime,
        definition_matches=definition_matches,
        enriched_matches=enriched_matches,
    )
    insight_neighborhood, neighborhood_notes = _build_tree_sitter_neighborhood_preview(
        ctx=ctx,
        partition_results=partition_results,
        summary=summary,
        sections=sections,
        inputs=_NeighborhoodPreviewInputs(
            primary_target_finding=primary_target_finding,
            definition_matches=definition_matches,
            has_target_candidates=bool(candidate_findings),
        ),
    )
    return _SearchAssemblyInputs(
        enriched_matches=enriched_matches,
        object_runtime=object_runtime,
        summary=summary,
        sections=sections,
        all_diagnostics=all_diagnostics,
        definition_matches=definition_matches,
        candidate_findings=candidate_findings,
        primary_target_finding=primary_target_finding,
        primary_target_match=primary_target_match,
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
                notes=tuple(
                    dict.fromkeys([*insight.degradation.notes, *inputs.neighborhood_notes])
                ),
            ),
        )
    return _apply_search_semantic_insight(
        ctx=ctx,
        insight=insight,
        summary=inputs.summary,
        primary_target_finding=inputs.primary_target_finding,
        primary_target_match=inputs.primary_target_match,
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
        run_id=ctx.run_id,
    )
    run = run_ctx.to_runmeta("search")
    result = CqResult(
        run=run,
        summary=inputs.summary,
        sections=inputs.sections,
        key_findings=_build_search_result_key_findings(inputs),
        evidence=[build_finding(m, ctx.root) for m in inputs.enriched_matches[:MAX_EVIDENCE]],
    )
    _register_search_object_view(run_id=run.run_id, view=inputs.object_runtime.view)
    result.summary["cache_backend"] = snapshot_backend_metrics(root=ctx.root)
    result.summary["cache_maintenance"] = contract_to_builtins(
        maintenance_tick(get_cq_cache_backend(root=ctx.root))
    )
    return assign_result_finding_ids(result)


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
        with_neighborhood, limits, tc, argv.

    Returns:
    -------
    CqResult
        Complete search results.
    """
    request = _coerce_search_request(root=root, query=query, kwargs=kwargs)
    ctx = _build_search_context(request)
    pipeline = SearchPipeline(ctx)
    partition_started = ms()
    partition_results = pipeline.run_partitions(_run_language_partitions)
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
        fallback_partitions = SearchPipeline(fallback_ctx).run_partitions(_run_language_partitions)
        mode_chain.append(QueryMode.LITERAL)
        ctx = msgspec.structs.replace(fallback_ctx, mode_chain=tuple(mode_chain))
        if _partition_total_matches(fallback_partitions) > 0:
            partition_results = fallback_partitions
    elif not ctx.mode_chain:
        ctx = msgspec.structs.replace(ctx, mode_chain=(ctx.mode,))

    assemble_started = ms()
    result = SearchPipeline(ctx).assemble(partition_results, _assemble_smart_search_result)
    result.summary["search_stage_timings_ms"] = {
        "partition": max(0.0, assemble_started - partition_started),
        "assemble": max(0.0, ms() - assemble_started),
        "total": max(0.0, ms() - ctx.started_ms),
    }
    if ctx.run_id:
        for language in expand_language_scope(ctx.lang_scope):
            maybe_evict_run_cache_tag(root=ctx.root, language=language, run_id=ctx.run_id)
    return result


def assemble_result(assembly: SearchResultAssembly) -> CqResult:
    """Assemble search output from precomputed partition results."""
    return SearchPipeline(assembly.context).assemble(
        cast("list[_LanguageSearchResult]", assembly.partition_results),
        _assemble_smart_search_result,
    )


def run_smart_search_pipeline(context: SmartSearchContext) -> CqResult:
    """Run partition and assembly phases for an existing search context."""
    partition_results = SearchPipeline(context).run_partitions(_run_language_partitions)
    return SearchPipeline(context).assemble(partition_results, _assemble_smart_search_result)


__all__ = [
    "KIND_WEIGHTS",
    "SMART_SEARCH_LIMITS",
    "EnrichedMatch",
    "RawMatch",
    "SearchContext",
    "SearchResultAssembly",
    "SearchStats",
    "SmartSearchContext",
    "assemble_result",
    "build_candidate_searcher",
    "build_finding",
    "build_followups",
    "build_sections",
    "build_summary",
    "classify_match",
    "collect_candidates",
    "compute_relevance_score",
    "pop_search_object_view_for_run",
    "run_smart_search_pipeline",
    "smart_search",
]
