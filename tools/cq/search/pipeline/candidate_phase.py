"""Candidate collection phase for Smart Search."""

from __future__ import annotations

import logging
import re
from pathlib import Path

import msgspec

from tools.cq.core.types import (
    QueryLanguage,
    constrain_include_globs_for_language,
    is_path_in_lang_scope,
    language_extension_exclude_globs,
    ripgrep_type_for_language,
)
from tools.cq.search._shared.requests import CandidateCollectionRequest, RgRunRequest
from tools.cq.search._shared.types import SearchLimits
from tools.cq.search.pipeline.classifier import QueryMode
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.profiles import INTERACTIVE
from tools.cq.search.pipeline.smart_search_types import RawMatch, SearchStats
from tools.cq.search.rg.collector import RgCollector
from tools.cq.search.rg.runner import RgCountRequest, run_rg_count, run_rg_json

logger = logging.getLogger(__name__)


def _identifier_pattern(query: str) -> str:
    return re.escape(query)


def _adaptive_limits_from_count(request: RgCountRequest, *, _lang: QueryLanguage) -> SearchLimits:
    counts = run_rg_count(
        RgCountRequest(
            root=request.root,
            pattern=request.pattern,
            mode=request.mode,
            lang_types=(ripgrep_type_for_language(_lang),),
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


def prefilter_scope_paths_for_ast(
    scope_paths: tuple[Path, ...] | None,
    *,
    lang: QueryLanguage,
) -> tuple[Path, ...] | None:
    """Filter scope paths to files parseable by ast-grep for the language.

    Returns:
    -------
    tuple[Path, ...] | None
        Filtered paths when parseable files exist; otherwise original scope paths.
    """
    if not scope_paths:
        return None
    from tools.cq.search.pipeline.classifier import get_sg_root
    from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext

    cache_context = ClassifierCacheContext()
    filtered: list[Path] = []
    for path in scope_paths:
        if path.is_file():
            if get_sg_root(path, lang=lang, cache_context=cache_context) is not None:
                filtered.append(path)
            continue
        filtered.append(path)
    return tuple(filtered) if filtered else scope_paths


def collect_candidates(request: CandidateCollectionRequest) -> tuple[list[RawMatch], SearchStats]:
    """Execute native ``rg`` search and collect raw matches.

    Returns:
    -------
    tuple[list[RawMatch], SearchStats]
        Scope-filtered raw matches and aggregate search statistics.
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


def run_candidate_phase(
    config: SearchConfig,
    *,
    lang: QueryLanguage,
    mode: QueryMode,
) -> tuple[list[RawMatch], SearchStats, str]:
    """Run candidate collection for one language partition.

    Returns:
    -------
    tuple[list[RawMatch], SearchStats, str]
        Raw matches, stats, and effective search pattern.
    """
    pattern = _identifier_pattern(config.query) if mode == QueryMode.IDENTIFIER else config.query
    logger.debug(
        "Running candidate phase: root=%s lang=%s mode=%s pattern=%r",
        config.root,
        lang,
        mode.value,
        pattern,
    )
    include_globs = constrain_include_globs_for_language(config.include_globs, lang)
    exclude_globs = list(config.exclude_globs or [])
    exclude_globs.extend(language_extension_exclude_globs(lang))
    effective_limits = _adaptive_limits_from_count(
        RgCountRequest(
            root=config.root,
            pattern=pattern,
            mode=mode,
            lang_types=(ripgrep_type_for_language(lang),),
            include_globs=tuple(include_globs or ()),
            exclude_globs=tuple(exclude_globs),
            limits=config.limits,
        ),
        _lang=lang,
    )
    raw_matches, stats = collect_candidates(
        CandidateCollectionRequest(
            root=config.root,
            pattern=pattern,
            mode=mode,
            limits=effective_limits,
            lang=lang,
            include_globs=include_globs,
            exclude_globs=exclude_globs,
            pcre2_available=bool(getattr(config.tc, "rg_pcre2_available", False)),
        )
    )
    return raw_matches, stats, pattern


__all__ = ["collect_candidates", "prefilter_scope_paths_for_ast", "run_candidate_phase"]
