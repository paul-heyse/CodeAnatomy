"""Smart Search pipeline for semantically-enriched code search.

Integrates rpygrep candidate generation with ast-grep classification
and symtable enrichment to provide grouped, ranked search results.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, replace
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, TypedDict, Unpack, cast

import msgspec

from tools.cq.core.schema import (
    Anchor,
    CqResult,
    DetailPayload,
    Finding,
    ScoreDetails,
    Section,
    mk_runmeta,
    ms,
)
from tools.cq.core.structs import CqStruct
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
from tools.cq.search.profiles import INTERACTIVE, SearchLimits
from tools.cq.search.timeout import search_sync_with_timeout

if TYPE_CHECKING:
    from rpygrep import RipGrepSearch

    from tools.cq.core.toolchain import Toolchain


# Derive smart search limits from INTERACTIVE profile
SMART_SEARCH_LIMITS = replace(
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


@lru_cache(maxsize=1)
def _get_context_helpers() -> tuple[
    Callable[[int, list[tuple[int, int]], int], dict[str, int]],
    Callable[[list[str], int, int], str | None],
]:
    """Lazily import context helpers to avoid circular imports.

    Returns
    -------
    tuple[Callable[[int, list[tuple[int, int]], int], dict[str, int]], Callable[[list[str], int, int], str]]
        Context window and snippet helpers.
    """
    from tools.cq.macros.calls import _compute_context_window, _extract_context_snippet

    return _compute_context_window, _extract_context_snippet


class RawMatch(CqStruct, frozen=True):
    """Raw match from rpygrep candidate generation.

    Parameters
    ----------
    file
        Relative file path.
    line
        1-indexed line number.
    col
        0-indexed column offset.
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

    file: str
    line: Annotated[int, msgspec.Meta(ge=1)]
    col: Annotated[int, msgspec.Meta(ge=0)]
    text: str
    match_text: str
    match_start: int
    match_end: int
    submatch_index: int = 0


class SmartSearchKwargs(TypedDict, total=False):
    mode: QueryMode | None
    include_globs: list[str] | None
    exclude_globs: list[str] | None
    include_strings: bool
    limits: SearchLimits | None
    tc: Toolchain | None
    argv: list[str] | None


@dataclass(frozen=True, slots=True)
class SmartSearchRequest:
    root: Path
    query: str
    mode: QueryMode | None = None
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None
    include_strings: bool = False
    limits: SearchLimits | None = None
    tc: Toolchain | None = None
    argv: list[str] | None = None

    @classmethod
    def from_kwargs(cls, root: Path, query: str, kwargs: SmartSearchKwargs) -> SmartSearchRequest:
        return cls(
            root=root,
            query=query,
            mode=kwargs.get("mode"),
            include_globs=kwargs.get("include_globs"),
            exclude_globs=kwargs.get("exclude_globs"),
            include_strings=bool(kwargs.get("include_strings", False)),
            limits=kwargs.get("limits"),
            tc=kwargs.get("tc"),
            argv=kwargs.get("argv"),
        )


@dataclass(frozen=True, slots=True)
class CandidateSearchInputs:
    root: Path
    query: str
    mode: QueryMode
    limits: SearchLimits
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None


class CandidateSearchKwargs(TypedDict, total=False):
    include_globs: list[str] | None
    exclude_globs: list[str] | None


@dataclass(frozen=True, slots=True)
class SearchSummaryInputs:
    query: str
    mode: QueryMode
    stats: SearchStats
    matches: list[EnrichedMatch]
    limits: SearchLimits
    include: list[str] | None = None
    exclude: list[str] | None = None
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
    file
        Relative file path.
    line
        1-indexed line number.
    col
        0-indexed column offset.
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
    """

    file: str
    line: Annotated[int, msgspec.Meta(ge=1)]
    col: Annotated[int, msgspec.Meta(ge=0)]
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
) -> tuple[RipGrepSearch, str]:
    """Build rpygrep searcher for candidate generation.

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

    Returns
    -------
    tuple[RipGrepSearch, str]
        Configured searcher and effective pattern string.
    """
    inputs = CandidateSearchInputs(
        root=root,
        query=query,
        mode=mode,
        limits=limits,
        include_globs=kwargs.get("include_globs"),
        exclude_globs=kwargs.get("exclude_globs"),
    )
    return _build_candidate_searcher(inputs)


def _build_candidate_searcher(inputs: CandidateSearchInputs) -> tuple[RipGrepSearch, str]:
    from rpygrep import RipGrepSearch

    searcher = (
        RipGrepSearch()
        .set_working_directory(inputs.root)
        .add_safe_defaults()
        .include_type("py")
        .case_sensitive(_CASE_SENSITIVE_DEFAULT)
        .before_context(1)
        .after_context(1)
        .max_count(inputs.limits.max_matches_per_file)
        .max_depth(inputs.limits.max_depth)
        .max_file_size(inputs.limits.max_file_size_bytes)
        .as_json()
    )

    if inputs.mode == QueryMode.IDENTIFIER:
        # Word boundary match for identifiers
        pattern = rf"\b{re.escape(inputs.query)}\b"
        searcher = searcher.add_pattern(pattern)
    elif inputs.mode == QueryMode.LITERAL:
        # Exact literal match (non-regex)
        pattern = inputs.query
        searcher = searcher.patterns_are_not_regex().add_pattern(inputs.query)
    else:
        # User-provided regex (pass through)
        pattern = inputs.query
        searcher = searcher.add_pattern(inputs.query)

    # Add include globs
    include_globs = inputs.include_globs or []
    for glob in include_globs:
        searcher = searcher.include_glob(glob)

    # Add exclude globs
    exclude_globs = inputs.exclude_globs or []
    for glob in exclude_globs:
        searcher = searcher.exclude_glob(glob)

    return searcher, pattern


def _parse_rg_line(line: str) -> dict[str, object] | None:
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None


def _collect_candidates_raw(
    searcher: RipGrepSearch,
    limits: SearchLimits,
) -> RgCollector:
    collector = RgCollector(limits=limits, match_factory=RawMatch)
    for line in searcher.run_direct():
        payload = _parse_rg_line(line)
        if not payload:
            continue
        kind = payload.get("type")
        if kind == "summary":
            collector.handle_summary(payload)
            continue
        if kind != "match":
            continue
        collector.handle_match(payload)
    return collector


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


def collect_candidates(
    searcher: RipGrepSearch,
    limits: SearchLimits,
) -> tuple[list[RawMatch], SearchStats]:
    """Execute rpygrep search and collect raw matches.

    Parameters
    ----------
    searcher
        Configured rpygrep searcher.
    limits
        Search safety limits.

    Returns
    -------
    tuple[list[RawMatch], SearchStats]
        Raw matches and collection statistics.
    """
    try:
        collector = search_sync_with_timeout(
            lambda: _collect_candidates_raw(searcher, limits),
            limits.timeout_seconds,
        )
    except TimeoutError:
        collector = RgCollector(limits=limits, match_factory=RawMatch)
        timed_out = True
    else:
        timed_out = False

    stats = _build_search_stats(collector, timed_out=timed_out)
    return collector.matches, stats


def classify_match(
    raw: RawMatch,
    root: Path,
    *,
    enable_symtable: bool = True,
) -> EnrichedMatch:
    """Run three-stage classification pipeline on a raw match.

    Parameters
    ----------
    raw
        Raw match from rpygrep.
    root
        Repository root for file resolution.
    enable_symtable
        Whether to run symtable enrichment.

    Returns
    -------
    EnrichedMatch
        Fully classified and enriched match.
    """
    match_text = raw.match_text

    # Stage 1: Fast heuristic
    heuristic = classify_heuristic(raw.text, raw.col, match_text)

    if heuristic.skip_deeper and heuristic.category is not None:
        return _build_heuristic_enriched(raw, heuristic, match_text)

    file_path = root / raw.file
    classification = _resolve_match_classification(raw, file_path, heuristic, root)
    symtable_enrichment = _maybe_symtable_enrichment(
        file_path,
        raw,
        match_text,
        classification,
        enable_symtable=enable_symtable,
    )
    context_window, context_snippet = _build_context_enrichment(file_path, raw)
    enrichment = MatchEnrichment(
        symtable=symtable_enrichment,
        context_window=context_window,
        context_snippet=context_snippet,
    )
    return _build_enriched_match(
        raw,
        match_text,
        classification,
        enrichment,
    )


def _build_heuristic_enriched(
    raw: RawMatch,
    heuristic: HeuristicResult,
    match_text: str,
) -> EnrichedMatch:
    return EnrichedMatch(
        file=raw.file,
        line=raw.line,
        col=raw.col,
        text=raw.text,
        match_text=match_text,
        category=heuristic.category,
        confidence=heuristic.confidence,
        evidence_kind="heuristic",
    )


def _resolve_match_classification(
    raw: RawMatch,
    file_path: Path,
    heuristic: HeuristicResult,
    root: Path,
) -> ClassificationResult:
    if file_path.suffix != ".py":
        return _classification_from_heuristic(heuristic, default_confidence=0.4, evidence_kind="rg_only")

    record_result = classify_from_records(file_path, root, raw.line, raw.col)
    ast_result = record_result or _classify_from_node(file_path, raw)
    if ast_result is not None:
        return _classification_from_node(ast_result)
    if heuristic.category is not None:
        return _classification_from_heuristic(heuristic, default_confidence=heuristic.confidence)
    return _default_classification()


def _classify_from_node(file_path: Path, raw: RawMatch) -> NodeClassification | None:
    sg_root = get_sg_root(file_path)
    if sg_root is None:
        return None
    return classify_from_node(sg_root, raw.line, raw.col)


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
    match_text: str,
    classification: ClassificationResult,
    *,
    enable_symtable: bool,
) -> SymtableEnrichment | None:
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
    return enrich_with_symtable_from_table(table, match_text, raw.line)


def _build_context_enrichment(
    file_path: Path,
    raw: RawMatch,
) -> tuple[dict[str, int] | None, str | None]:
    source = get_cached_source(file_path)
    if source is None:
        return None, None
    source_lines = source.splitlines()
    def_lines = get_def_lines_cached(file_path)
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
) -> EnrichedMatch:
    return EnrichedMatch(
        file=raw.file,
        line=raw.line,
        col=raw.col,
        text=raw.text,
        match_text=match_text,
        category=classification.category,
        confidence=classification.confidence,
        evidence_kind=classification.evidence_kind,
        node_kind=classification.node_kind,
        containing_scope=classification.containing_scope,
        context_window=enrichment.context_window,
        context_snippet=enrichment.context_snippet,
        symtable=enrichment.symtable,
    )


def _classify_file_role(file_path: str) -> str:
    """Classify file role for ranking.

    Parameters
    ----------
    file_path
        Relative file path.

    Returns
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

    Returns
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

    Returns
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

    Returns
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

    Returns
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
        anchor=Anchor(file=match.file, line=match.line, col=match.col),
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

    Returns
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

    Returns
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
    return SearchSummaryInputs(
        query=cast("str", query),
        mode=cast("QueryMode", mode),
        stats=cast("SearchStats", stats),
        matches=cast("list[EnrichedMatch]", matches),
        limits=cast("SearchLimits", limits),
        include=cast("list[str] | None", kwargs.get("include")),
        exclude=cast("list[str] | None", kwargs.get("exclude")),
        file_globs=cast("list[str] | None", kwargs.get("file_globs")),
        limit=cast("int | None", kwargs.get("limit")),
        pattern=cast("str | None", kwargs.get("pattern")),
    )


def _build_summary(inputs: SearchSummaryInputs) -> dict[str, object]:
    return {
        "query": inputs.query,
        "mode": inputs.mode.value,
        "file_globs": inputs.file_globs or ["*.py", "*.pyi"],
        "include": inputs.include or [],
        "exclude": inputs.exclude or [],
        "context_lines": {"before": 1, "after": 1},
        "limit": inputs.limit if inputs.limit is not None else inputs.limits.max_total_matches,
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

    Returns
    -------
    list[Section]
        Organized sections.
    """
    non_code_categories = {"docstring_match", "comment_match", "string_match"}
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
        sections.append(Section(title="Definitions", findings=[build_finding(m, root) for m in defs[:5]]))
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


def _build_search_context(request: SmartSearchRequest) -> SmartSearchContext:
    started = ms()
    limits = request.limits or SMART_SEARCH_LIMITS
    argv = request.argv or ["search", request.query]

    # Clear caches from previous runs
    clear_caches()

    actual_mode = detect_query_mode(request.query, force_mode=request.mode)
    return SmartSearchContext(
        root=request.root,
        query=request.query,
        mode=actual_mode,
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
) -> tuple[list[RawMatch], SearchStats, str]:
    inputs = CandidateSearchInputs(
        root=ctx.root,
        query=ctx.query,
        mode=ctx.mode,
        limits=ctx.limits,
        include_globs=ctx.include_globs,
        exclude_globs=ctx.exclude_globs,
    )
    searcher, pattern = _build_candidate_searcher(inputs)
    raw_matches, stats = collect_candidates(searcher, ctx.limits)
    return raw_matches, stats, pattern


def _run_classification_phase(
    ctx: SmartSearchContext, raw_matches: list[RawMatch]
) -> list[EnrichedMatch]:
    return [classify_match(raw, ctx.root) for raw in raw_matches]


def _assemble_smart_search_result(
    ctx: SmartSearchContext,
    stats: SearchStats,
    enriched_matches: list[EnrichedMatch],
    pattern: str,
) -> CqResult:
    summary_inputs = SearchSummaryInputs(
        query=ctx.query,
        mode=ctx.mode,
        stats=stats,
        matches=enriched_matches,
        limits=ctx.limits,
        include=ctx.include_globs,
        exclude=ctx.exclude_globs,
        file_globs=["*.py", "*.pyi"],
        limit=ctx.limits.max_total_matches,
        pattern=pattern,
    )
    summary = build_summary(summary_inputs)
    sections = build_sections(
        enriched_matches,
        ctx.root,
        ctx.query,
        ctx.mode,
        include_strings=ctx.include_strings,
    )

    run = mk_runmeta(
        macro="search",
        argv=ctx.argv,
        root=str(ctx.root),
        started_ms=ctx.started_ms,
        toolchain=ctx.tc.to_dict() if ctx.tc else {},
    )

    return CqResult(
        run=run,
        summary=summary,
        sections=sections,
        key_findings=sections[0].findings[:5] if sections else [],
        evidence=[build_finding(m, ctx.root) for m in enriched_matches[:MAX_EVIDENCE]],
    )


def smart_search(
    root: Path,
    query: str,
    **kwargs: Unpack[SmartSearchKwargs],
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

    Returns
    -------
    CqResult
        Complete search results.
    """
    request = SmartSearchRequest.from_kwargs(root, query, kwargs)
    ctx = _build_search_context(request)
    raw_matches, stats, pattern = _run_candidate_phase(ctx)
    enriched_matches = _run_classification_phase(ctx, raw_matches)
    return _assemble_smart_search_result(ctx, stats, enriched_matches, pattern)


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
