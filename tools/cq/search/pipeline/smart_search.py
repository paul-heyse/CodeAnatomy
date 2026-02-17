"""Smart Search pipeline orchestration entrypoints."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from pathlib import Path

import msgspec

from tools.cq.core.cache.run_lifecycle import maybe_evict_run_cache_tag
from tools.cq.core.schema import CqResult, ms, update_result_summary
from tools.cq.core.types import QueryLanguage, expand_language_scope
from tools.cq.orchestration.multilang_orchestrator import execute_by_language_scope
from tools.cq.search.pipeline.assembly import assemble_smart_search_result
from tools.cq.search.pipeline.candidate_phase import run_candidate_phase as run_candidate_phase_impl
from tools.cq.search.pipeline.classifier import QueryMode, clear_caches
from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext
from tools.cq.search.pipeline.classify_phase import run_classify_phase
from tools.cq.search.pipeline.contracts import SearchConfig, SearchPartitionPlanV1, SearchRequest
from tools.cq.search.pipeline.orchestration import SearchPipeline
from tools.cq.search.pipeline.partition_pipeline import run_search_partition
from tools.cq.search.pipeline.profiles import INTERACTIVE
from tools.cq.search.pipeline.request_parsing import coerce_search_request
from tools.cq.search.pipeline.runtime_context import build_search_runtime_context
from tools.cq.search.pipeline.search_object_view_store import pop_search_object_view_for_run
from tools.cq.search.pipeline.search_runtime import (
    build_search_context as build_search_context_runtime,
)
from tools.cq.search.pipeline.search_runtime import (
    partition_total_matches as partition_total_matches_runtime,
)
from tools.cq.search.pipeline.search_runtime import (
    should_fallback_to_literal as should_fallback_to_literal_runtime,
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
logger = logging.getLogger(__name__)


def _thaw_mapping_values(value: object) -> object:
    if isinstance(value, Mapping):
        return {
            str(key): _thaw_mapping_values(item)
            for key, item in value.items()
            if isinstance(key, str)
        }
    if isinstance(value, list):
        return [_thaw_mapping_values(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_thaw_mapping_values(item) for item in value)
    return value


def _thaw_summary_mappings(summary: object) -> object:
    if not isinstance(summary, msgspec.Struct):
        return summary
    fields = getattr(summary, "__struct_fields__", ())
    if not isinstance(fields, tuple):
        return summary
    updates: dict[str, object] = {}
    for field_name in fields:
        value = getattr(summary, field_name)
        thawed = _thaw_mapping_values(value)
        if thawed is not value:
            updates[field_name] = thawed
    return msgspec.structs.replace(summary, **updates) if updates else summary


def _build_search_context(request: SearchRequest) -> SearchConfig:
    return build_search_context_runtime(request, default_limits=SMART_SEARCH_LIMITS)


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


def run_candidate_phase(
    context: SearchConfig,
    *,
    lang: QueryLanguage,
    mode: QueryMode,
) -> tuple[list[RawMatch], SearchStats, str]:
    """Compatibility wrapper for candidate-phase execution.

    Returns:
        tuple[list[RawMatch], SearchStats, str]: Raw matches, stats, and effective pattern.
    """
    return _run_candidate_phase(context, lang=lang, mode=mode)


def _run_candidate_phase(
    context: SearchConfig,
    *,
    lang: QueryLanguage,
    mode: QueryMode,
) -> tuple[list[RawMatch], SearchStats, str]:
    return run_candidate_phase_impl(context, lang=lang, mode=mode)


def run_classification_phase(
    context: SearchConfig,
    *,
    lang: QueryLanguage,
    raw_matches: list[RawMatch],
    cache_context: ClassifierCacheContext,
) -> list[EnrichedMatch]:
    """Compatibility wrapper for classification-phase execution.

    Returns:
        list[EnrichedMatch]: Classified and enriched matches.
    """
    return run_classify_phase(
        context,
        lang=lang,
        raw_matches=raw_matches,
        cache_context=cache_context,
    )


def _partition_total_matches(partition_results: list[LanguageSearchResult]) -> int:
    return partition_total_matches_runtime(partition_results)


def _should_fallback_to_literal(
    *,
    request: SearchRequest,
    initial_mode: QueryMode,
    partition_results: list[LanguageSearchResult],
) -> bool:
    return should_fallback_to_literal_runtime(
        request=request,
        initial_mode=initial_mode,
        partition_results=partition_results,
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
    logger.debug(
        "smart_search.start query=%r root=%s mode=%s lang_scope=%s run_id=%s",
        query,
        root,
        request.mode,
        request.lang_scope,
        request.run_id,
    )
    clear_caches()
    ctx = _build_search_context(request)
    build_search_runtime_context(ctx)
    pipeline = SearchPipeline(ctx)
    partition_started = ms()
    partition_results = pipeline.run_partitions(_run_language_partitions)
    logger.debug(
        "smart_search.partitions mode=%s results=%s",
        ctx.mode.value,
        [
            {
                "lang": partition.lang,
                "raw_matches": len(partition.raw_matches),
                "enriched_matches": len(partition.enriched_matches),
                "total_matches": partition.stats.total_matches,
                "timed_out": partition.stats.timed_out,
                "truncated": partition.stats.truncated,
            }
            for partition in partition_results
        ],
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
        logger.info(
            "smart_search.fallback_to_literal query=%r lang_scope=%s",
            query,
            ctx.lang_scope,
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
    logger.debug(
        "smart_search.done query=%r mode_chain=%s matches=%s duration_ms=%.2f",
        query,
        [mode.value for mode in ctx.mode_chain],
        result.summary.matches,
        max(0.0, ms() - ctx.started_ms),
    )
    return msgspec.structs.replace(
        result,
        summary=_thaw_summary_mappings(result.summary),
    )


__all__ = [
    "SMART_SEARCH_LIMITS",
    "EnrichedMatch",
    "RawMatch",
    "SearchConfig",
    "SearchResultAssembly",
    "SearchStats",
    "assemble_smart_search_result",
    "build_finding",
    "build_followups",
    "build_sections",
    "build_summary",
    "pop_search_object_view_for_run",
    "run_candidate_phase",
    "run_classification_phase",
    "run_classify_phase",
    "smart_search",
]
