"""Runtime helpers extracted from smart-search orchestration."""

from __future__ import annotations

import logging
import re
from pathlib import Path

from tools.cq.core.enrichment_mode import parse_incremental_enrichment_mode
from tools.cq.core.schema import ms
from tools.cq.core.types import (
    DEFAULT_QUERY_LANGUAGE_SCOPE,
    QueryLanguage,
    QueryLanguageScope,
    ripgrep_types_for_scope,
)
from tools.cq.orchestration.language_scope import merge_partitioned_items
from tools.cq.search._shared.requests import RgRunRequest
from tools.cq.search._shared.types import SearchLimits
from tools.cq.search.pipeline.classifier import QueryMode, detect_query_mode
from tools.cq.search.pipeline.contracts import (
    CandidateSearchRequest,
    SearchConfig,
    SearchRequest,
)
from tools.cq.search.pipeline.relevance import compute_relevance_score
from tools.cq.search.pipeline.smart_search_types import (
    EnrichedMatch,
    LanguageSearchResult,
)
from tools.cq.search.rg.runner import build_rg_command
from tools.cq.utils.uuid_factory import uuid7_str

logger = logging.getLogger(__name__)


def identifier_pattern(query: str) -> str:
    """Escape identifier text; ripgrep word boundaries are applied via ``-w``.

    Returns:
        str: Escaped identifier pattern.
    """
    return re.escape(query)


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

    Returns:
        tuple[list[str], str]: Candidate search command and effective pattern.
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
    return build_candidate_searcher_for_config(config)


def build_candidate_searcher_for_config(config: SearchConfig) -> tuple[list[str], str]:
    """Build native ``rg`` command using a fully-normalized search config.

    Returns:
        tuple[list[str], str]: Candidate search command and effective pattern.
    """
    if config.mode == QueryMode.IDENTIFIER:
        pattern = identifier_pattern(config.query)
    elif config.mode == QueryMode.LITERAL:
        pattern = config.query
    else:
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
    logger.debug(
        "runtime.build_candidate_searcher mode=%s scope=%s pattern=%s",
        config.mode.value,
        config.lang_scope,
        pattern,
    )
    return command, pattern


def build_search_context(
    request: SearchRequest,
    *,
    default_limits: SearchLimits,
) -> SearchConfig:
    """Build canonical search config from request payload.

    Returns:
        SearchConfig: Normalized search runtime context.
    """
    started = request.started_ms
    if started is None:
        started = ms()
    limits = request.limits or default_limits
    argv = request.argv or ["search", request.query]

    actual_mode = detect_query_mode(request.query, force_mode=request.mode)
    config = SearchConfig(
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
    logger.debug(
        "runtime.context_built mode=%s scope=%s run_id=%s",
        config.mode.value,
        config.lang_scope,
        config.run_id,
    )
    return config


def partition_total_matches(partition_results: list[LanguageSearchResult]) -> int:
    """Return sum of partition total matches."""
    return sum(result.stats.total_matches for result in partition_results)


def should_fallback_to_literal(
    *,
    request: SearchRequest,
    initial_mode: QueryMode,
    partition_results: list[LanguageSearchResult],
) -> bool:
    """Return whether smart-search should attempt literal fallback."""
    if request.mode is not None:
        return False
    if initial_mode != QueryMode.IDENTIFIER:
        return False
    return partition_total_matches(partition_results) == 0


def merge_language_matches(
    *,
    partition_results: list[LanguageSearchResult],
    lang_scope: QueryLanguageScope,
) -> list[EnrichedMatch]:
    """Merge per-language partition matches using scope-aware ordering.

    Returns:
        list[EnrichedMatch]: Deterministically merged match list.
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


__all__ = [
    "build_candidate_searcher",
    "build_candidate_searcher_for_config",
    "build_search_context",
    "identifier_pattern",
    "merge_language_matches",
    "partition_total_matches",
    "should_fallback_to_literal",
]
