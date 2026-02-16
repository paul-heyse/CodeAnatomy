"""Smart Search pipeline for semantically-enriched code search."""

from __future__ import annotations

import multiprocessing
import re
from pathlib import Path
from typing import TYPE_CHECKING, cast

import msgspec

from tools.cq.core.cache import (
    maybe_evict_run_cache_tag,
)
from tools.cq.core.contracts import SummaryBuildRequest
from tools.cq.core.locations import (
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
from tools.cq.search._shared.types import SearchLimits
from tools.cq.search.enrichment.core import normalize_python_payload, normalize_rust_payload
from tools.cq.search.objects.render import (
    SearchOccurrenceV1,
    build_non_code_occurrence_section,
    build_occurrence_hot_files_section,
    build_occurrence_kind_counts_section,
    build_occurrences_section,
    build_resolved_objects_section,
    is_non_code_occurrence,
)
from tools.cq.search.objects.resolve import ObjectResolutionRuntime, build_object_resolved_view
from tools.cq.search.pipeline.assembly import assemble_smart_search_result
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
from tools.cq.search.pipeline.contracts import (
    CandidateSearchRequest,
    SearchConfig,
    SearchPartitionPlanV1,
    SearchRequest,
    SmartSearchContext,
)
from tools.cq.search.pipeline.orchestration import (
    SearchPipeline,
)
from tools.cq.search.pipeline.partition_pipeline import run_search_partition
from tools.cq.search.pipeline.profiles import INTERACTIVE
from tools.cq.search.pipeline.search_object_view_store import pop_search_object_view_for_run
from tools.cq.search.pipeline.smart_search_telemetry import (
    build_enrichment_telemetry as _build_enrichment_telemetry,
)
from tools.cq.search.pipeline.smart_search_telemetry import (
    new_python_semantic_telemetry as _new_python_semantic_telemetry,
)
from tools.cq.search.pipeline.smart_search_types import (
    ClassificationBatchResult,
    ClassificationBatchTask,
    ClassificationResult,
    EnrichedMatch,
    MatchClassifyOptions,
    MatchEnrichment,
    RawMatch,
    ResolvedNodeContext,
    SearchResultAssembly,
    SearchStats,
    SearchSummaryInputs,
    _LanguageSearchResult,
    _PythonSemanticAnchorKey,
    _PythonSemanticPrefetchResult,
)
from tools.cq.search.python.analysis_session import get_python_analysis_session
from tools.cq.search.python.extractors import (
    _ENRICHMENT_ERRORS as _PYTHON_ENRICHMENT_ERRORS,
)
from tools.cq.search.python.extractors import enrich_python_context_by_byte_range
from tools.cq.search.rg.collector import RgCollector
from tools.cq.search.rg.runner import RgCountRequest, build_rg_command, run_rg_count, run_rg_json
from tools.cq.search.rust.enrichment import enrich_rust_context_by_byte_range
from tools.cq.search.semantic.diagnostics import (
    build_cross_language_diagnostics,
    build_language_capabilities,
    diagnostics_to_summary_payload,
    is_python_oriented_query_text,
)
from tools.cq.search.semantic.models import (
    LanguageSemanticEnrichmentRequest,
    enrich_with_language_semantics,
)
from tools.cq.search.tree_sitter.core.adaptive_runtime import adaptive_query_budget_ms
from tools.cq.search.tree_sitter.core.runtime_support import budget_ms_per_anchor
from tools.cq.search.tree_sitter.query.lint import lint_search_query_packs
from tools.cq.utils.uuid_factory import uuid7_str

if TYPE_CHECKING:
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

_RUST_ENRICHMENT_ERRORS = (RuntimeError, TypeError, ValueError, AttributeError, UnicodeError)
MAX_SEARCH_CLASSIFY_WORKERS = 4
_TREE_SITTER_QUERY_BUDGET_FALLBACK_MS = budget_ms_per_anchor(
    timeout_seconds=SMART_SEARCH_LIMITS.timeout_seconds,
    max_anchors=SMART_SEARCH_LIMITS.max_total_matches,
)


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
        pattern = _identifier_pattern(config.query)
    elif config.mode == QueryMode.LITERAL:
        # Exact literal match (non-regex)
        pattern = config.query
    else:
        # User-provided regex (pass through)
        pattern = config.query

    command = build_rg_command(
        RgRunRequest(
            root=config.root,
            pattern=pattern,
            mode=config.mode,
            lang_types=tuple(ripgrep_types_for_scope(config.lang_scope)),
            include_globs=config.include_globs or [],
            exclude_globs=config.exclude_globs or [],
            limits=config.limits,
        )
    )
    return command, pattern


def _identifier_pattern(query: str) -> str:
    """Escape identifier text; ripgrep word boundaries are applied via ``-w``.

    Returns:
        str: Function return value.
    """
    return re.escape(query)


def _adaptive_limits_from_count(request: RgCountRequest, *, lang: QueryLanguage) -> SearchLimits:
    """Choose candidate limits from a quick rg count preflight.

    Returns:
        SearchLimits: Function return value.
    """
    counts = run_rg_count(
        RgCountRequest(
            root=request.root,
            pattern=request.pattern,
            mode=request.mode,
            lang_types=(ripgrep_type_for_language(lang),),
            include_globs=request.include_globs,
            exclude_globs=request.exclude_globs,
            paths=request.paths,
            limits=request.limits,
        )
    )
    active_limits = request.limits or INTERACTIVE
    if not counts:
        return active_limits
    estimated_total = sum(max(0, value) for value in counts.values())
    if estimated_total > active_limits.max_total_matches * 10:
        return msgspec.structs.replace(
            active_limits,
            max_matches_per_file=min(active_limits.max_matches_per_file, 100),
        )
    return active_limits


def _build_search_stats(collector: RgCollector, *, timed_out: bool) -> SearchStats:
    scanned_files = len(
        collector.files_completed or collector.files_started or collector.seen_files
    )
    matched_files = len(collector.seen_files)
    total_matches = len(collector.matches)
    scanned_files_is_estimate = not bool(collector.files_completed or collector.files_started)
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
        rg_stats=summary_stats if isinstance(summary_stats, dict) else None,
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
        ),
        pcre2_available=request.pcre2_available,
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
    if isinstance(raw.match_abs_byte_start, int) and isinstance(raw.match_abs_byte_end, int):
        start = max(0, min(raw.match_abs_byte_start, len(source_bytes)))
        end = max(start + 1, min(raw.match_abs_byte_end, len(source_bytes)))
        return start, end

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
        "context_lines": {
            "before": config.limits.context_before,
            "after": config.limits.context_after,
        },
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
    if config.tc is not None:
        common["rg_pcre2_available"] = bool(getattr(config.tc, "rg_pcre2_available", False))
        common["rg_pcre2_version"] = getattr(config.tc, "rg_pcre2_version", None)
    if isinstance(inputs.stats.rg_stats, dict):
        common["rg_stats"] = inputs.stats.rg_stats.copy()
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
    pattern = _identifier_pattern(ctx.query) if mode == QueryMode.IDENTIFIER else ctx.query
    include_globs = constrain_include_globs_for_language(ctx.include_globs, lang)
    exclude_globs = list(ctx.exclude_globs or [])
    exclude_globs.extend(language_extension_exclude_globs(lang))
    effective_limits = _adaptive_limits_from_count(
        RgCountRequest(
            root=ctx.root,
            pattern=pattern,
            mode=mode,
            lang_types=(ripgrep_type_for_language(lang),),
            include_globs=tuple(include_globs or ()),
            exclude_globs=tuple(exclude_globs),
            limits=ctx.limits,
        ),
        lang=lang,
    )
    raw_matches, stats = collect_candidates(
        CandidateCollectionRequest(
            root=ctx.root,
            pattern=pattern,
            mode=mode,
            limits=effective_limits,
            lang=lang,
            include_globs=include_globs,
            exclude_globs=exclude_globs,
            pcre2_available=bool(getattr(ctx.tc, "rg_pcre2_available", False)),
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
    result = SearchPipeline(ctx).assemble(partition_results, assemble_smart_search_result)
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
    """Assemble search output from precomputed partition results.

    Returns:
        CqResult: Function return value.
    """
    return SearchPipeline(assembly.context).assemble(
        cast("list[_LanguageSearchResult]", assembly.partition_results),
        assemble_smart_search_result,
    )


def run_smart_search_pipeline(context: SmartSearchContext) -> CqResult:
    """Run partition and assembly phases for an existing search context.

    Returns:
        CqResult: Function return value.
    """
    partition_results = SearchPipeline(context).run_partitions(_run_language_partitions)
    return SearchPipeline(context).assemble(partition_results, assemble_smart_search_result)


__all__ = [
    "KIND_WEIGHTS",
    "SMART_SEARCH_LIMITS",
    "EnrichedMatch",
    "RawMatch",
    "SearchResultAssembly",
    "SearchStats",
    "SmartSearchContext",
    "assemble_result",
    "assemble_smart_search_result",
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
