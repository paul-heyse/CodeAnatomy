"""Smart Search pipeline for semantically-enriched code search.

Integrates rpygrep candidate generation with ast-grep classification
and symtable enrichment to provide grouped, ranked search results.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Callable
from dataclasses import replace
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

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
from tools.cq.search.classifier import (
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


class RawMatch(msgspec.Struct, frozen=True, omit_defaults=True):
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


class SearchStats(msgspec.Struct, omit_defaults=True):
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


class EnrichedMatch(msgspec.Struct, frozen=True, omit_defaults=True):
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
    *,
    include_globs: list[str] | None = None,
    exclude_globs: list[str] | None = None,
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
    from rpygrep import RipGrepSearch

    searcher = (
        RipGrepSearch()
        .set_working_directory(root)
        .add_safe_defaults()
        .include_type("py")
        .case_sensitive(_CASE_SENSITIVE_DEFAULT)
        .before_context(1)
        .after_context(1)
        .max_count(limits.max_matches_per_file)
        .max_depth(limits.max_depth)
        .max_file_size(limits.max_file_size_bytes)
        .as_json()
    )

    if mode == QueryMode.IDENTIFIER:
        # Word boundary match for identifiers
        pattern = rf"\b{re.escape(query)}\b"
        searcher = searcher.add_pattern(pattern)
    elif mode == QueryMode.LITERAL:
        # Exact literal match (non-regex)
        pattern = query
        searcher = searcher.patterns_are_not_regex().add_pattern(query)
    else:
        # User-provided regex (pass through)
        pattern = query
        searcher = searcher.add_pattern(query)

    # Add include globs
    include_globs = include_globs or []
    for glob in include_globs:
        searcher = searcher.include_glob(glob)

    # Add exclude globs
    exclude_globs = exclude_globs or []
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
) -> tuple[list[RawMatch], set[str], dict[str, object] | None, bool, bool, bool]:
    matches: list[RawMatch] = []
    seen_files: set[str] = set()
    truncated = False
    max_files_hit = False
    max_matches_hit = False
    summary_stats: dict[str, object] | None = None

    def _allow_file(file_path: str) -> bool:
        nonlocal truncated, max_files_hit
        if max_files_hit:
            return False
        if file_path in seen_files:
            return True
        if len(seen_files) >= limits.max_files:
            truncated = True
            max_files_hit = True
            return False
        seen_files.add(file_path)
        return True

    for line in searcher.run_direct():
        payload = _parse_rg_line(line)
        if not payload:
            continue
        kind = payload.get("type")
        if kind == "summary":
            data = payload.get("data", {})
            if isinstance(data, dict):
                stats = data.get("stats")
                if isinstance(stats, dict):
                    summary_stats = stats
            continue
        if kind != "match":
            continue
        if max_matches_hit or max_files_hit:
            continue
        data = payload.get("data", {})
        if not isinstance(data, dict):
            continue
        path_data = data.get("path", {})
        file_path = None
        if isinstance(path_data, dict):
            file_path = path_data.get("text") or path_data.get("bytes")
        if not isinstance(file_path, str):
            continue
        if not _allow_file(file_path):
            continue

        line_number = data.get("line_number")
        if not isinstance(line_number, int):
            continue
        lines_data = data.get("lines", {})
        line_text = ""
        if isinstance(lines_data, dict):
            text_value = lines_data.get("text")
            if isinstance(text_value, str):
                line_text = text_value

        submatches_raw = data.get("submatches")
        submatches: list[object] = submatches_raw if isinstance(submatches_raw, list) else []

        if submatches:
            for i, submatch in enumerate(submatches):
                if len(matches) >= limits.max_total_matches:
                    truncated = True
                    max_matches_hit = True
                    break
                if not isinstance(submatch, dict):
                    continue
                start = submatch.get("start")
                end = submatch.get("end")
                if not isinstance(start, int) or not isinstance(end, int):
                    continue
                match_text = ""
                match_data = submatch.get("match", {})
                if isinstance(match_data, dict):
                    text_value = match_data.get("text")
                    if isinstance(text_value, str):
                        match_text = text_value
                if not match_text and line_text:
                    match_text = line_text[start:end]
                matches.append(
                    RawMatch(
                        file=file_path,
                        line=line_number,
                        col=start,
                        text=line_text,
                        match_text=match_text,
                        match_start=start,
                        match_end=end,
                        submatch_index=i,
                    )
                )
        else:
            if len(matches) >= limits.max_total_matches:
                truncated = True
                max_matches_hit = True
                continue
            matches.append(
                RawMatch(
                    file=file_path,
                    line=line_number,
                    col=0,
                    text=line_text,
                    match_text=line_text,
                    match_start=0,
                    match_end=len(line_text),
                    submatch_index=0,
                )
            )

    return matches, seen_files, summary_stats, truncated, max_files_hit, max_matches_hit


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
        (
            matches,
            seen_files,
            summary_stats,
            truncated,
            max_files_hit,
            max_matches_hit,
        ) = search_sync_with_timeout(
            lambda: _collect_candidates_raw(searcher, limits),
            limits.timeout_seconds,
        )
    except TimeoutError:
        matches: list[RawMatch] = []
        seen_files = set()
        summary_stats = None
        truncated = False
        max_files_hit = False
        max_matches_hit = False
        timed_out = True
    else:
        timed_out = False

    scanned_files = len(seen_files)
    matched_files = len(seen_files)
    total_matches = len(matches)
    scanned_files_is_estimate = True
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

    stats = SearchStats(
        scanned_files=scanned_files,
        matched_files=matched_files,
        total_matches=total_matches,
        truncated=truncated,
        timed_out=timed_out,
        max_files_hit=max_files_hit,
        max_matches_hit=max_matches_hit,
        scanned_files_is_estimate=scanned_files_is_estimate,
    )

    return matches, stats


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

    # Stage 2: AST node classification
    file_path = root / raw.file
    if file_path.suffix != ".py":
        return EnrichedMatch(
            file=raw.file,
            line=raw.line,
            col=raw.col,
            text=raw.text,
            match_text=match_text,
            category=heuristic.category or "text_match",
            confidence=heuristic.confidence or 0.4,
            evidence_kind="rg_only",
        )

    ast_result: NodeClassification | None = None
    record_result = classify_from_records(file_path, root, raw.line, raw.col)
    if record_result is not None:
        ast_result = record_result
    else:
        sg_root = get_sg_root(file_path)
        if sg_root is not None:
            ast_result = classify_from_node(sg_root, raw.line, raw.col)

    # Determine best classification
    if ast_result is not None:
        category = ast_result.category
        confidence = ast_result.confidence
        evidence_kind = ast_result.evidence_kind
        node_kind = ast_result.node_kind
        containing_scope = ast_result.containing_scope
    elif heuristic.category is not None:
        category = heuristic.category
        confidence = heuristic.confidence
        evidence_kind = "heuristic"
        node_kind = None
        containing_scope = None
    else:
        category = "text_match"
        confidence = 0.50
        evidence_kind = "rg_only"
        node_kind = None
        containing_scope = None

    # Stage 3: Symtable enrichment (only for high-value categories)
    symtable_enrichment: SymtableEnrichment | None = None
    if enable_symtable and category in {"definition", "callsite", "reference", "assignment"}:
        source = get_cached_source(file_path)
        if source is not None:
            table = get_symtable_table(file_path, source)
            if table is not None:
                symtable_enrichment = enrich_with_symtable_from_table(
                    table,
                    match_text,
                    raw.line,
                )

    # Compute context window and snippet
    context_window: dict[str, int] | None = None
    context_snippet: str | None = None
    source = get_cached_source(file_path)
    if source is not None:
        source_lines = source.splitlines()
        def_lines = get_def_lines_cached(file_path)
        if def_lines:
            compute_context_window, extract_context_snippet = _get_context_helpers()
            context_window = compute_context_window(raw.line, def_lines, len(source_lines))
            context_snippet = extract_context_snippet(
                source_lines,
                context_window["start_line"],
                context_window["end_line"],
            )

    return EnrichedMatch(
        file=raw.file,
        line=raw.line,
        col=raw.col,
        text=raw.text,
        match_text=match_text,
        category=category,
        confidence=confidence,
        evidence_kind=evidence_kind,
        node_kind=node_kind,
        containing_scope=containing_scope,
        context_window=context_window,
        context_snippet=context_snippet,
        symtable=symtable_enrichment,
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
    score = ScoreDetails(
        confidence_score=match.confidence,
        confidence_bucket=_evidence_to_bucket(match.evidence_kind),
        evidence_kind=match.evidence_kind,
    )

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
        symtable_flags: list[str] = []
        if match.symtable.is_imported:
            symtable_flags.append("imported")
        if match.symtable.is_assigned:
            symtable_flags.append("assigned")
        if match.symtable.is_parameter:
            symtable_flags.append("parameter")
        if match.symtable.is_free:
            symtable_flags.append("closure_var")
        if match.symtable.is_global:
            symtable_flags.append("global")
        if symtable_flags:
            data["binding_flags"] = symtable_flags

    details = DetailPayload(
        kind=match.category,
        score=score,
        data=data,
    )

    return Finding(
        category=match.category,
        message=_category_message(match.category, match),
        anchor=Anchor(
            file=match.file,
            line=match.line,
            col=match.col,
        ),
        severity="info",
        details=details,
    )


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


def build_summary(
    query: str,
    mode: QueryMode,
    stats: SearchStats,
    matches: list[EnrichedMatch],
    limits: SearchLimits,
    *,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    file_globs: list[str] | None = None,
    limit: int | None = None,
    pattern: str | None = None,
) -> dict[str, object]:
    """Build summary dict for CqResult.

    Parameters
    ----------
    query
        Original query string.
    mode
        Query mode.
    stats
        Search statistics.
    matches
        List of enriched matches.
    limits
        Search limits.
    include
        Include patterns.
    exclude
        Exclude patterns.
    file_globs
        File glob patterns.
    limit
        Result limit.
    pattern
        Effective regex pattern.

    Returns
    -------
    dict[str, object]
        Summary dictionary.
    """
    return {
        "query": query,
        "mode": mode.value,
        "file_globs": file_globs or ["*.py", "*.pyi"],
        "include": include or [],
        "exclude": exclude or [],
        "context_lines": {"before": 1, "after": 1},
        "limit": limit if limit is not None else limits.max_total_matches,
        "scanned_files": stats.scanned_files,
        "scanned_files_is_estimate": stats.scanned_files_is_estimate,
        "matched_files": stats.matched_files,
        "total_matches": stats.total_matches,
        "returned_matches": len(matches),
        "scan_method": "hybrid",
        "pattern": pattern,
        "case_sensitive": True,
        "caps_hit": (
            "timeout"
            if stats.timed_out
            else (
                "max_files"
                if stats.max_files_hit
                else ("max_total_matches" if stats.max_matches_hit else "none")
            )
        ),
        "truncated": stats.truncated,
        "timed_out": stats.timed_out,
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
    sections: list[Section] = []

    non_code_categories = {"docstring_match", "comment_match", "string_match"}

    # Sort by relevance
    sorted_matches = sorted(matches, key=compute_relevance_score, reverse=True)

    visible_matches = (
        sorted_matches
        if include_strings
        else [m for m in sorted_matches if m.category not in non_code_categories]
    )

    # Group by containing function/method (fallback to file)
    grouped: dict[str, list[EnrichedMatch]] = {}
    for match in visible_matches:
        key = f"{match.containing_scope} ({match.file})" if match.containing_scope else match.file
        grouped.setdefault(key, []).append(match)

    # Section 1: Top Contexts (representative match per group)
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
    sections.append(Section(title="Top Contexts", findings=top_contexts))

    # Section 2: Identifier panels (definitions, imports, calls)
    if mode == QueryMode.IDENTIFIER:
        defs = [m for m in visible_matches if m.category == "definition"]
        imps = [m for m in visible_matches if m.category in {"import", "from_import"}]
        calls = [m for m in visible_matches if m.category == "callsite"]
        if defs:
            sections.append(
                Section(
                    title="Definitions",
                    findings=[build_finding(m, root) for m in defs[:5]],
                )
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

    # Section 3: Uses by Kind
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
    sections.append(
        Section(
            title="Uses by Kind",
            findings=kind_findings,
            collapsed=True,
        )
    )

    # Section 4: Non-code matches (collapsed by default)
    non_code = [m for m in sorted_matches if m.category in non_code_categories]
    if non_code:
        sections.append(
            Section(
                title="Non-Code Matches (Strings / Comments / Docstrings)",
                findings=[build_finding(m, root) for m in non_code[:20]],
                collapsed=True,
            )
        )

    # Section 5: Hot Files
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
    sections.append(
        Section(
            title="Hot Files",
            findings=hot_file_findings,
            collapsed=True,
        )
    )

    # Section 6: Suggested Follow-ups
    followup_findings = build_followups(matches, query, mode)
    if followup_findings:
        sections.append(
            Section(
                title="Suggested Follow-ups",
                findings=followup_findings,
            )
        )

    return sections


def smart_search(
    root: Path,
    query: str,
    *,
    mode: QueryMode | None = None,
    include_globs: list[str] | None = None,
    exclude_globs: list[str] | None = None,
    include_strings: bool = False,
    limits: SearchLimits | None = None,
    tc: Toolchain | None = None,
    argv: list[str] | None = None,
) -> CqResult:
    """Execute Smart Search pipeline.

    Parameters
    ----------
    root
        Repository root path.
    query
        Search query string.
    mode
        Query mode override (auto-detected if None).
    include_globs
        File patterns to include.
    exclude_globs
        File patterns to exclude.
    include_strings
        Include strings/comments/docstrings in primary ranking.
    limits
        Search safety limits.
    tc
        Toolchain info from CliContext.
    argv
        Original command arguments.

    Returns
    -------
    CqResult
        Complete search results.
    """
    started = ms()
    limits = limits or SMART_SEARCH_LIMITS
    argv = argv or ["search", query]

    # Clear caches from previous runs
    clear_caches()

    # Detect query mode
    actual_mode = detect_query_mode(query, force_mode=mode)

    # Phase 1: Candidate generation
    searcher, pattern = build_candidate_searcher(
        root,
        query,
        actual_mode,
        limits,
        include_globs=include_globs,
        exclude_globs=exclude_globs,
    )
    raw_matches, stats = collect_candidates(searcher, limits)

    # Phase 2: Classification
    enriched_matches = [classify_match(raw, root) for raw in raw_matches]

    # Phase 3: Assembly
    summary = build_summary(
        query,
        actual_mode,
        stats,
        enriched_matches,
        limits,
        include=include_globs,
        exclude=exclude_globs,
        file_globs=["*.py", "*.pyi"],
        limit=limits.max_total_matches,
        pattern=pattern,
    )
    sections = build_sections(
        enriched_matches, root, query, actual_mode, include_strings=include_strings
    )

    # Build CqResult
    run = mk_runmeta(
        macro="search",
        argv=argv,
        root=str(root),
        started_ms=started,
        toolchain=tc.to_dict() if tc else {},
    )

    return CqResult(
        run=run,
        summary=summary,
        sections=sections,
        key_findings=sections[0].findings[:5] if sections else [],
        evidence=[build_finding(m, root) for m in enriched_matches[:MAX_EVIDENCE]],
    )


__all__ = [
    "KIND_WEIGHTS",
    "SMART_SEARCH_LIMITS",
    "EnrichedMatch",
    "RawMatch",
    "SearchStats",
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
