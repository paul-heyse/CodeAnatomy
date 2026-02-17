"""Smart Search pipeline for semantically-enriched code search."""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, cast

import msgspec

from tools.cq.core.cache.run_lifecycle import maybe_evict_run_cache_tag
from tools.cq.core.contracts import SummaryBuildRequest
from tools.cq.core.schema import (
    Anchor,
    CqResult,
    DetailPayload,
    Finding,
    ScoreDetails,
    Section,
    ms,
)
from tools.cq.orchestration.multilang_orchestrator import (
    execute_by_language_scope,
    merge_partitioned_items,
)
from tools.cq.orchestration.multilang_summary import (
    assert_multilang_summary,
    build_multilang_summary,
)
from tools.cq.query.language import (
    DEFAULT_QUERY_LANGUAGE_SCOPE,
    QueryLanguage,
    QueryLanguageScope,
    expand_language_scope,
    file_globs_for_scope,
    is_python_language,
    is_rust_language,
    ripgrep_types_for_scope,
)
from tools.cq.search._shared.core import (
    CandidateCollectionRequest,
    RgRunRequest,
)
from tools.cq.search._shared.types import SearchLimits
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
from tools.cq.search.pipeline.candidate_phase import (
    collect_candidates as _collect_candidates_phase,
)
from tools.cq.search.pipeline.candidate_phase import (
    run_candidate_phase as _run_candidate_phase,
)
from tools.cq.search.pipeline.classifier import (
    MatchCategory,
    QueryMode,
    SymtableEnrichment,
    clear_caches,
    detect_query_mode,
)
from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext
from tools.cq.search.pipeline.classify_phase import run_classify_phase
from tools.cq.search.pipeline.contracts import (
    CandidateSearchRequest,
    SearchConfig,
    SearchPartitionPlanV1,
    SearchRequest,
)
from tools.cq.search.pipeline.enrichment_contracts import (
    python_enrichment_payload,
    python_semantic_enrichment_payload,
    rust_enrichment_payload,
)
from tools.cq.search.pipeline.enrichment_phase import run_enrichment_phase
from tools.cq.search.pipeline.orchestration import (
    SearchPipeline,
)
from tools.cq.search.pipeline.profiles import INTERACTIVE
from tools.cq.search.pipeline.runtime_context import build_search_runtime_context
from tools.cq.search.pipeline.search_object_view_store import pop_search_object_view_for_run
from tools.cq.search.pipeline.smart_search_telemetry import (
    build_enrichment_telemetry as _build_enrichment_telemetry,
)
from tools.cq.search.pipeline.smart_search_telemetry import (
    new_python_semantic_telemetry as _new_python_semantic_telemetry,
)
from tools.cq.search.pipeline.smart_search_types import (
    EnrichedMatch,
    LanguageSearchResult,
    RawMatch,
    SearchResultAssembly,
    SearchStats,
    SearchSummaryInputs,
    _PythonSemanticAnchorKey,
    _PythonSemanticPrefetchResult,
)
from tools.cq.search.rg.runner import build_rg_command
from tools.cq.search.semantic.diagnostics import (
    build_cross_language_diagnostics,
    build_language_capabilities,
    diagnostics_to_summary_payload,
    is_python_oriented_query_text,
)
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


def collect_candidates(request: CandidateCollectionRequest) -> tuple[list[RawMatch], SearchStats]:
    """Compatibility wrapper delegating candidate collection to phase module.

    Returns:
        tuple[list[RawMatch], SearchStats]: Raw matches and aggregate scan stats.
    """
    return _collect_candidates_phase(request)


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
        enrichment["rust"] = rust_enrichment_payload(match.rust_tree_sitter)
    python_payload: dict[str, object] | None = None
    if match.python_enrichment:
        python_payload = python_enrichment_payload(match.python_enrichment)
    elif is_python_language(match.language):
        # Keep a stable python payload container for Python findings even when
        # only secondary enrichment sources are available.
        python_payload = {}
    if python_payload is not None:
        if match.python_semantic_enrichment:
            python_payload.setdefault(
                "python_semantic",
                python_semantic_enrichment_payload(match.python_semantic_enrichment),
            )
        enrichment["python"] = python_payload
    if match.python_semantic_enrichment:
        enrichment["python_semantic"] = python_semantic_enrichment_payload(
            match.python_semantic_enrichment
        )
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


def build_summary(inputs: SearchSummaryInputs) -> dict[str, object]:
    """Build summary dict for CqResult.

    Returns:
    -------
    dict[str, object]
        Normalized summary payload for CQ result rendering.
    """
    return _build_summary(inputs)


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


def _build_search_context(request: SearchRequest) -> SearchConfig:
    started = request.started_ms
    if started is None:
        started = ms()
    limits = request.limits or SMART_SEARCH_LIMITS
    argv = request.argv or ["search", request.query]

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


def run_candidate_phase(
    ctx: SearchConfig,
    *,
    lang: QueryLanguage,
    mode: QueryMode,
) -> tuple[list[RawMatch], SearchStats, str]:
    """Public wrapper around candidate-phase execution.

    Returns:
        tuple[list[RawMatch], SearchStats, str]: Candidate matches, stats, and search pattern.
    """
    return _run_candidate_phase(ctx, lang=lang, mode=mode)


def run_classification_phase(
    ctx: SearchConfig,
    *,
    lang: QueryLanguage,
    raw_matches: list[RawMatch],
    cache_context: ClassifierCacheContext,
) -> list[EnrichedMatch]:
    """Public wrapper around classification phase execution.

    Returns:
        list[EnrichedMatch]: Classified/enriched matches.
    """
    return run_classify_phase(
        ctx,
        lang=lang,
        raw_matches=raw_matches,
        cache_context=cache_context,
    )


def _run_language_partitions(ctx: SearchConfig) -> list[LanguageSearchResult]:
    by_lang = execute_by_language_scope(
        ctx.lang_scope,
        lambda lang: _run_single_partition(ctx, lang, mode=ctx.mode),
    )
    return [by_lang[lang] for lang in expand_language_scope(ctx.lang_scope)]


def _run_single_partition(
    ctx: SearchConfig,
    lang: QueryLanguage,
    *,
    mode: QueryMode,
) -> LanguageSearchResult:
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
    return run_enrichment_phase(plan, config=ctx, mode=mode)


def _merge_python_semantic_prefetch_results(
    partition_results: list[LanguageSearchResult],
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


def _partition_total_matches(partition_results: list[LanguageSearchResult]) -> int:
    return sum(result.stats.total_matches for result in partition_results)


def _should_fallback_to_literal(
    *,
    request: SearchRequest,
    initial_mode: QueryMode,
    partition_results: list[LanguageSearchResult],
) -> bool:
    if request.mode is not None:
        return False
    if initial_mode != QueryMode.IDENTIFIER:
        return False
    return _partition_total_matches(partition_results) == 0


def _merge_language_matches(
    *,
    partition_results: list[LanguageSearchResult],
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
    ctx: SearchConfig,
    partition_results: list[LanguageSearchResult],
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
    python_matches = sum(1 for match in enriched_matches if is_python_language(match.language))
    rust_matches = sum(1 for match in enriched_matches if is_rust_language(match.language))
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
    clear_caches()
    ctx = _build_search_context(request)
    build_search_runtime_context(ctx)
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
    result.summary.search_stage_timings_ms = {
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
        cast("list[LanguageSearchResult]", assembly.partition_results),
        assemble_smart_search_result,
    )


def run_smart_search_pipeline(context: SearchConfig) -> CqResult:
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
    "SearchConfig",
    "SearchResultAssembly",
    "SearchStats",
    "assemble_result",
    "assemble_smart_search_result",
    "build_candidate_searcher",
    "collect_candidates",
    "compute_relevance_score",
    "pop_search_object_view_for_run",
    "run_smart_search_pipeline",
    "smart_search",
]
