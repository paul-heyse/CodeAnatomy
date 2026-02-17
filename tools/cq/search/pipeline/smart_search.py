"""Smart Search pipeline for semantically-enriched code search."""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, cast

import msgspec

from tools.cq.core.cache.run_lifecycle import maybe_evict_run_cache_tag
from tools.cq.core.schema import (
    CqResult,
    Finding,
    Section,
    ms,
)
from tools.cq.core.summary_contract import as_search_summary
from tools.cq.core.types import QueryLanguage, QueryLanguageScope
from tools.cq.orchestration.multilang_orchestrator import (
    execute_by_language_scope,
    merge_partitioned_items,
)
from tools.cq.query.language import (
    DEFAULT_QUERY_LANGUAGE_SCOPE,
    expand_language_scope,
    ripgrep_types_for_scope,
)
from tools.cq.search._shared.core import (
    CandidateCollectionRequest,
    RgRunRequest,
)
from tools.cq.search._shared.types import SearchLimits
from tools.cq.search.objects.resolve import ObjectResolutionRuntime
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
    IncrementalEnrichmentModeV1,
    parse_incremental_enrichment_mode,
)
from tools.cq.search.pipeline.enrichment_phase import run_enrichment_phase
from tools.cq.search.pipeline.orchestration import (
    SearchPipeline,
)
from tools.cq.search.pipeline.profiles import INTERACTIVE
from tools.cq.search.pipeline.runtime_context import build_search_runtime_context
from tools.cq.search.pipeline.search_object_view_store import pop_search_object_view_for_run
from tools.cq.search.pipeline.smart_search_types import (
    EnrichedMatch,
    LanguageSearchResult,
    RawMatch,
    SearchResultAssembly,
    SearchStats,
    SearchSummaryInputs,
)
from tools.cq.search.rg.runner import build_rg_command
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
    from tools.cq.search.pipeline.smart_search_sections import build_finding as _build_finding

    return _build_finding(match, _root)


def build_followups(
    matches: list[EnrichedMatch],
    query: str,
    mode: QueryMode,
) -> list[Finding]:
    from tools.cq.search.pipeline.smart_search_followups import build_followups as _build_followups

    return _build_followups(matches, query, mode)


def build_summary(inputs: SearchSummaryInputs) -> dict[str, object]:
    from tools.cq.search.pipeline.smart_search_summary import build_language_summary

    return build_language_summary(inputs)


def build_sections(
    matches: list[EnrichedMatch],
    root: Path,
    query: str,
    mode: QueryMode,
    *,
    include_strings: bool = False,
    object_runtime: ObjectResolutionRuntime | None = None,
) -> list[Section]:
    from tools.cq.search.pipeline.smart_search_sections import build_sections as _build_sections

    return _build_sections(
        matches,
        root,
        query,
        mode,
        include_strings=include_strings,
        object_runtime=object_runtime,
    )


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
        incremental_enrichment_enabled=request.incremental_enrichment_enabled,
        incremental_enrichment_mode=parse_incremental_enrichment_mode(
            request.incremental_enrichment_mode
        ),
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
        incremental_enrichment_enabled=_coerce_incremental_enrichment_enabled(
            kwargs.get("incremental_enrichment_enabled")
        ),
        incremental_enrichment_mode=_coerce_incremental_enrichment_mode(
            kwargs.get("incremental_enrichment_mode")
        ),
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


def _coerce_incremental_enrichment_enabled(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return True


def _coerce_incremental_enrichment_mode(value: object) -> IncrementalEnrichmentModeV1:
    return parse_incremental_enrichment_mode(value)


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
        incremental_enrichment_enabled=ctx.incremental_enrichment_enabled,
        incremental_enrichment_mode=ctx.incremental_enrichment_mode,
    )
    return run_enrichment_phase(plan, config=ctx, mode=mode)


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


def merge_language_matches(
    *,
    partition_results: list[LanguageSearchResult],
    lang_scope: QueryLanguageScope,
) -> list[EnrichedMatch]:
    """Merge per-language partition matches using scope-aware ordering.

    Returns:
        list[EnrichedMatch]: Merged and ranked matches across language partitions.
    """
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
    from tools.cq.search.pipeline.smart_search_summary import (
        _build_cross_language_diagnostics_for_search as _build_diagnostics,
    )

    return _build_diagnostics(
        query=query,
        lang_scope=lang_scope,
        python_matches=python_matches,
        rust_matches=rust_matches,
    )


def _build_capability_diagnostics_for_search(
    *,
    lang_scope: QueryLanguageScope,
) -> list[Finding]:
    from tools.cq.search.pipeline.smart_search_summary import (
        _build_capability_diagnostics_for_search as _build_capability_diagnostics,
    )

    return _build_capability_diagnostics(lang_scope=lang_scope)


def _build_tree_sitter_runtime_diagnostics(
    telemetry: dict[str, object],
) -> list[Finding]:
    from tools.cq.search.pipeline.smart_search_summary import (
        _build_tree_sitter_runtime_diagnostics as _build_runtime_diagnostics,
    )

    return _build_runtime_diagnostics(telemetry)


def _build_search_summary(
    ctx: SearchConfig,
    partition_results: list[LanguageSearchResult],
    enriched_matches: list[EnrichedMatch],
    *,
    python_semantic_overview: dict[str, object],
    python_semantic_telemetry: dict[str, object],
    python_semantic_diagnostics: list[dict[str, object]],
) -> tuple[dict[str, object], list[Finding]]:
    from tools.cq.search.pipeline.smart_search_summary import (
        build_search_summary as _build_search_summary_v2,
    )

    return _build_search_summary_v2(
        ctx,
        partition_results,
        enriched_matches,
        python_semantic_overview=python_semantic_overview,
        python_semantic_telemetry=python_semantic_telemetry,
        python_semantic_diagnostics=python_semantic_diagnostics,
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
    as_search_summary(result.summary).search_stage_timings_ms = {
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
        assembly.partition_results,
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
    "merge_language_matches",
    "pop_search_object_view_for_run",
    "run_smart_search_pipeline",
    "smart_search",
]
