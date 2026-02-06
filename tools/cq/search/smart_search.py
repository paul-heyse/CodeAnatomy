"""Smart Search pipeline for semantically-enriched code search."""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import TypedDict, Unpack, cast

import msgspec

from tools.cq.core.locations import SourceSpan
from tools.cq.core.multilang_orchestrator import (
    execute_by_language_scope,
    merge_partitioned_items,
)
from tools.cq.core.multilang_summary import (
    assert_multilang_summary,
    build_multilang_summary,
)
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
    classify_heuristic,
    clear_caches,
    detect_query_mode,
    enrich_with_symtable_from_table,
    get_cached_source,
    get_def_lines_cached,
    get_sg_root,
    get_symtable_table,
)
from tools.cq.search.collector import RgCollector
from tools.cq.search.context import SmartSearchContext
from tools.cq.search.models import SearchConfig, SearchKwargs
from tools.cq.search.multilang_diagnostics import (
    build_cross_language_diagnostics,
    is_python_oriented_query_text,
)
from tools.cq.search.profiles import INTERACTIVE, SearchLimits
from tools.cq.search.rg_native import build_rg_command, run_rg_json
from tools.cq.search.tree_sitter_rust import enrich_rust_context

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


@lru_cache(maxsize=1)
def _get_context_helpers() -> tuple[
    Callable[[int, list[tuple[int, int]], int], dict[str, int]],
    Callable[[list[str], int, int], str | None],
]:
    """Lazily import context helpers to avoid circular imports.

    Returns:
    -------
    tuple[Callable[[int, list[tuple[int, int]], int], dict[str, int]], Callable[[list[str], int, int], str]]
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
        Column offset of match start.
    match_end
        Column offset of match end.
    submatch_index
        Which submatch on this line.
    """

    span: SourceSpan
    text: str
    match_text: str
    match_start: int
    match_end: int
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


class CandidateSearchKwargs(TypedDict, total=False):
    lang_scope: QueryLanguageScope
    include_globs: list[str] | None
    exclude_globs: list[str] | None


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
class MatchEnrichment:
    symtable: SymtableEnrichment | None
    context_window: dict[str, int] | None
    context_snippet: str | None
    rust_tree_sitter: dict[str, object] | None


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
    """

    scanned_files: int
    matched_files: int
    total_matches: int
    scanned_files_is_estimate: bool = True
    truncated: bool = False
    timed_out: bool = False
    max_files_hit: bool = False
    max_matches_hit: bool = False


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
    config = SearchConfig(
        root=root,
        query=query,
        mode=mode,
        lang_scope=kwargs.get("lang_scope", DEFAULT_QUERY_LANGUAGE_SCOPE),
        limits=limits,
        include_globs=kwargs.get("include_globs"),
        exclude_globs=kwargs.get("exclude_globs"),
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


def collect_candidates(  # noqa: PLR0913
    root: Path,
    *,
    pattern: str,
    mode: QueryMode,
    limits: SearchLimits,
    lang: QueryLanguage,
    include_globs: list[str] | None = None,
    exclude_globs: list[str] | None = None,
) -> tuple[list[RawMatch], SearchStats]:
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
        root=root,
        pattern=pattern,
        mode=mode,
        lang_types=(ripgrep_type_for_language(lang),),
        include_globs=include_globs or [],
        exclude_globs=exclude_globs or [],
        limits=limits,
    )
    collector = RgCollector(limits=limits, match_factory=RawMatch)
    for event in proc.events:
        collector.handle_event(event)
    collector.finalize()
    stats = _build_search_stats(collector, timed_out=proc.timed_out)
    return collector.matches, stats


def classify_match(
    raw: RawMatch,
    root: Path,
    *,
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
    enable_symtable: bool = True,
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

    Returns:
    -------
    EnrichedMatch
        Fully classified and enriched match.
    """
    match_text = raw.match_text

    # Stage 1: Fast heuristic
    heuristic = classify_heuristic(raw.text, raw.col, match_text)

    if heuristic.skip_deeper and heuristic.category is not None:
        return _build_heuristic_enriched(raw, heuristic, match_text, lang=lang)

    file_path = root / raw.file
    classification = _resolve_match_classification(raw, file_path, heuristic, root, lang=lang)
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
    context_window, context_snippet = _build_context_enrichment(file_path, raw, lang=lang)
    enrichment = MatchEnrichment(
        symtable=symtable_enrichment,
        context_window=context_window,
        context_snippet=context_snippet,
        rust_tree_sitter=rust_tree_sitter,
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
) -> ClassificationResult:
    lang_suffixes = {".py", ".pyi"} if lang == "python" else {".rs"}
    if file_path.suffix not in lang_suffixes:
        return _classification_from_heuristic(
            heuristic, default_confidence=0.4, evidence_kind="rg_only"
        )

    record_result = classify_from_records(file_path, root, raw.line, raw.col, lang=lang)
    ast_result = record_result or _classify_from_node(file_path, raw, lang=lang)
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
) -> NodeClassification | None:
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
        scope_name = enrichment.rust_tree_sitter.get("scope_name")
        if isinstance(scope_name, str):
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
        language=lang,
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
    try:
        return enrich_rust_context(
            source,
            line=raw.line,
            col=raw.col,
            cache_key=str(file_path),
        )
    except _RUST_ENRICHMENT_ERRORS:
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
        "line_text": match.text,
        "language": match.language,
    }
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
    if match.rust_tree_sitter:
        data["rust_tree_sitter"] = match.rust_tree_sitter
    return data


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
        common=common,
        lang_scope=config.lang_scope,
        language_order=inputs.languages,
        languages=language_stats,
        cross_language_diagnostics=(),
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
    started = kwargs.get("started_ms")
    if started is None:
        started = ms()
    limits = kwargs.get("limits") or SMART_SEARCH_LIMITS
    argv = kwargs.get("argv") or ["search", query]

    # Clear caches from previous runs
    clear_caches()

    actual_mode = detect_query_mode(query, force_mode=kwargs.get("mode"))
    return SearchConfig(
        root=root,
        query=query,
        mode=actual_mode,
        lang_scope=kwargs.get("lang_scope", DEFAULT_QUERY_LANGUAGE_SCOPE),
        limits=limits,
        include_globs=kwargs.get("include_globs"),
        exclude_globs=kwargs.get("exclude_globs"),
        include_strings=bool(kwargs.get("include_strings", False)),
        argv=argv,
        tc=kwargs.get("tc"),
        started_ms=started,
    )


def _run_candidate_phase(
    ctx: SmartSearchContext,
    *,
    lang: QueryLanguage,
) -> tuple[list[RawMatch], SearchStats, str]:
    pattern = rf"\b{re.escape(ctx.query)}\b" if ctx.mode == QueryMode.IDENTIFIER else ctx.query
    raw_matches, stats = collect_candidates(
        ctx.root,
        pattern=pattern,
        mode=ctx.mode,
        limits=ctx.limits,
        lang=lang,
        include_globs=ctx.include_globs,
        exclude_globs=ctx.exclude_globs,
    )
    return raw_matches, stats, pattern


def _run_classification_phase(
    ctx: SmartSearchContext,
    *,
    lang: QueryLanguage,
    raw_matches: list[RawMatch],
) -> list[EnrichedMatch]:
    return [classify_match(raw, ctx.root, lang=lang) for raw in raw_matches]


@dataclass(frozen=True, slots=True)
class _LanguageSearchResult:
    lang: QueryLanguage
    raw_matches: list[RawMatch]
    stats: SearchStats
    pattern: str
    enriched_matches: list[EnrichedMatch]


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
    summary["cross_language_diagnostics"] = [finding.message for finding in all_diagnostics]
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
