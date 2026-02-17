"""Smart Search pipeline orchestration entrypoints."""

from __future__ import annotations

from pathlib import Path

import msgspec

from tools.cq.core.cache.run_lifecycle import maybe_evict_run_cache_tag
from tools.cq.core.schema import CqResult, ms, update_result_summary
from tools.cq.core.types import QueryLanguage, QueryLanguageScope
from tools.cq.orchestration.multilang_orchestrator import execute_by_language_scope
from tools.cq.query.language import expand_language_scope
from tools.cq.search._shared.requests import CandidateCollectionRequest
from tools.cq.search._shared.types import SearchLimits
from tools.cq.search.pipeline.assembly import assemble_smart_search_result
from tools.cq.search.pipeline.candidate_phase import (
    collect_candidates as _collect_candidates_phase,
)
from tools.cq.search.pipeline.candidate_phase import (
    run_candidate_phase as _run_candidate_phase,
)
from tools.cq.search.pipeline.classifier import QueryMode, clear_caches
from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext
from tools.cq.search.pipeline.classify_phase import run_classify_phase
from tools.cq.search.pipeline.contracts import SearchConfig, SearchPartitionPlanV1, SearchRequest
from tools.cq.search.pipeline.orchestration import SearchPipeline
from tools.cq.search.pipeline.partition_pipeline import run_search_partition
from tools.cq.search.pipeline.profiles import INTERACTIVE
from tools.cq.search.pipeline.relevance import KIND_WEIGHTS, compute_relevance_score
from tools.cq.search.pipeline.request_parsing import coerce_search_request
from tools.cq.search.pipeline.runtime_context import build_search_runtime_context
from tools.cq.search.pipeline.search_object_view_store import pop_search_object_view_for_run
from tools.cq.search.pipeline.search_runtime import (
    build_candidate_searcher as _build_candidate_searcher_from_runtime,
)
from tools.cq.search.pipeline.search_runtime import (
    build_search_context as _build_search_context_from_runtime,
)
from tools.cq.search.pipeline.search_runtime import (
    merge_language_matches as _merge_language_matches_runtime,
)
from tools.cq.search.pipeline.search_runtime import (
    partition_total_matches as _partition_total_matches_runtime,
)
from tools.cq.search.pipeline.search_runtime import (
    should_fallback_to_literal as _should_fallback_to_literal_runtime,
)
from tools.cq.search.pipeline.smart_search_followups import build_followups
from tools.cq.search.pipeline.smart_search_sections import build_finding, build_sections
from tools.cq.search.pipeline.smart_search_summary import (
    build_language_summary as build_summary,
)
from tools.cq.search.pipeline.smart_search_types import (
    EnrichedMatch,
    LanguageSearchResult,
    RawMatch,
    SearchResultAssembly,
    SearchStats,
)

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


def build_candidate_searcher(
    root: Path,
    query: str,
    mode: QueryMode,
    limits: SearchLimits,
    *,
    lang_scope: QueryLanguageScope = "auto",
    globs: tuple[list[str] | None, list[str] | None] | None = None,
) -> tuple[list[str], str]:
    """Build native ``rg`` command for candidate generation.

    Returns:
        tuple[list[str], str]: Candidate search command and effective pattern.
    """
    return _build_candidate_searcher_from_runtime(
        root,
        query,
        mode,
        limits,
        lang_scope=lang_scope,
        globs=globs,
    )


def collect_candidates(request: CandidateCollectionRequest) -> tuple[list[RawMatch], SearchStats]:
    """Compatibility wrapper delegating candidate collection to phase module.

    Returns:
        tuple[list[RawMatch], SearchStats]: Raw candidate matches and collection stats.
    """
    return _collect_candidates_phase(request)


def _build_search_context(request: SearchRequest) -> SearchConfig:
    return _build_search_context_from_runtime(request, default_limits=SMART_SEARCH_LIMITS)


def run_candidate_phase(
    config: SearchConfig,
    *,
    lang: QueryLanguage,
    mode: QueryMode,
) -> tuple[list[RawMatch], SearchStats, str]:
    """Run candidate collection for one language partition.

    Returns:
        tuple[list[RawMatch], SearchStats, str]: Raw matches, stats, and pattern.
    """
    return _run_candidate_phase(config, lang=lang, mode=mode)


def run_classification_phase(
    config: SearchConfig,
    *,
    lang: QueryLanguage,
    raw_matches: list[RawMatch],
    cache_context: ClassifierCacheContext,
) -> list[EnrichedMatch]:
    """Run classification/enrichment for one language partition.

    Returns:
        list[EnrichedMatch]: Classified and enriched matches.
    """
    return run_classify_phase(
        config,
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
    return run_search_partition(plan, ctx=ctx, mode=mode)


def _partition_total_matches(partition_results: list[LanguageSearchResult]) -> int:
    return _partition_total_matches_runtime(partition_results)


def _should_fallback_to_literal(
    *,
    request: SearchRequest,
    initial_mode: QueryMode,
    partition_results: list[LanguageSearchResult],
) -> bool:
    return _should_fallback_to_literal_runtime(
        request=request,
        initial_mode=initial_mode,
        partition_results=partition_results,
    )


def merge_language_matches(
    *,
    partition_results: list[LanguageSearchResult],
    lang_scope: QueryLanguageScope,
) -> list[EnrichedMatch]:
    """Merge per-language partition matches using scope-aware ordering.

    Returns:
        list[EnrichedMatch]: Deterministically merged match list.
    """
    return _merge_language_matches_runtime(
        partition_results=partition_results,
        lang_scope=lang_scope,
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
    request = coerce_search_request(root=root, query=query, kwargs=kwargs)
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
    result = update_result_summary(
        result,
        {
            "search_stage_timings_ms": {
                "partition": max(0.0, assemble_started - partition_started),
                "assemble": max(0.0, ms() - assemble_started),
                "total": max(0.0, ms() - ctx.started_ms),
            }
        },
    )
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
    "build_finding",
    "build_followups",
    "build_sections",
    "build_summary",
    "collect_candidates",
    "compute_relevance_score",
    "merge_language_matches",
    "pop_search_object_view_for_run",
    "run_smart_search_pipeline",
    "smart_search",
]
