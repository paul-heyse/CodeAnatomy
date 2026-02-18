"""Query planner helpers for tree-sitter query pattern prioritization."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from typing import TYPE_CHECKING

from tools.cq.search.tree_sitter.contracts.query_models import (
    QueryPackPlanV1,
    QueryPatternPlanV1,
)
from tools.cq.search.tree_sitter.core.language_registry import extract_provenance

if TYPE_CHECKING:
    from tree_sitter import Query

_MIN_ASSERTION_TUPLE_SIZE = 2


def _capture_quantifier_name(query: Query, pattern_idx: int, capture_idx: int) -> str:
    try:
        quantifier = query.capture_quantifier(pattern_idx, capture_idx)
    except (RuntimeError, TypeError, ValueError, IndexError, SystemError):
        return "none"
    text = str(quantifier).strip()
    return text if text else "none"


def _pattern_score(
    *,
    rooted: bool,
    non_local: bool,
    guaranteed_step0: bool,
    has_assertions: bool,
    has_captures: bool,
) -> float:
    """Compute a weighted score for a query pattern.

    Returns:
        float: Deterministic value used for ordering plans.
    """
    score = 0.0
    if rooted:
        score += 2.0
    if guaranteed_step0:
        score += 1.2
    if has_assertions:
        score += 1.0
    if has_captures:
        score += 0.5
    if non_local:
        score -= 2.5
    return score


def build_pattern_plan(query: Query) -> tuple[QueryPatternPlanV1, ...]:
    """Build deterministic per-pattern planning rows from a compiled query.

    Returns:
        tuple[QueryPatternPlanV1, ...]: Sorted pattern plans by score.
    """
    pattern_count = int(getattr(query, "pattern_count", 0))
    capture_count = int(getattr(query, "capture_count", 0))
    plans: list[QueryPatternPlanV1] = []
    for pattern_idx in range(pattern_count):
        rooted = bool(query.is_pattern_rooted(pattern_idx))
        non_local = bool(query.is_pattern_non_local(pattern_idx))
        guaranteed_step = bool(query.is_pattern_guaranteed_at_step(pattern_idx))
        assertions_raw = query.pattern_assertions(pattern_idx)
        assertions: dict[str, tuple[str, bool]] = {}
        if isinstance(assertions_raw, dict):
            for key, value in assertions_raw.items():
                if not isinstance(key, str):
                    continue
                if isinstance(value, tuple) and len(value) >= _MIN_ASSERTION_TUPLE_SIZE:
                    assertions[str(key)] = (str(value[0]), bool(value[1]))
        quantifiers = tuple(
            _capture_quantifier_name(query, pattern_idx, capture_idx)
            for capture_idx in range(capture_count)
        )
        score = _pattern_score(
            rooted=rooted,
            non_local=non_local,
            guaranteed_step0=guaranteed_step,
            has_assertions=bool(assertions),
            has_captures=any(q not in {"", "none"} for q in quantifiers),
        )
        plans.append(
            QueryPatternPlanV1(
                pattern_idx=pattern_idx,
                rooted=rooted,
                non_local=non_local,
                guaranteed_step0=guaranteed_step,
                start_byte=int(query.start_byte_for_pattern(pattern_idx)),
                end_byte=int(query.end_byte_for_pattern(pattern_idx)),
                assertions=assertions,
                capture_quantifiers=quantifiers,
                score=score,
            )
        )
    return tuple(sorted(plans, key=lambda row: row.score, reverse=True))


def build_pack_plan(
    *,
    pack_name: str,
    query: Query,
    query_text: str,
    language: str | None = None,
) -> QueryPackPlanV1:
    """Build a plan summary for one pack for scheduling and cache keys.

    Returns:
        QueryPackPlanV1: Pack score and ordered pattern plans.
    """
    patterns = build_pattern_plan(query)
    pack_score = sum(pattern.score for pattern in patterns)
    digest = hashlib.sha256(query_text.encode("utf-8")).hexdigest()[:16]
    language_obj = getattr(query, "language", None)
    if language_obj is None and isinstance(language, str) and language:
        from tools.cq.search.tree_sitter.core.language_registry import load_tree_sitter_language

        language_obj = load_tree_sitter_language(language)
    grammar_name, semantic_version, abi_version = extract_provenance(language_obj)
    return QueryPackPlanV1(
        pack_name=pack_name,
        query_hash=digest,
        plans=patterns,
        score=pack_score,
        grammar_name=grammar_name,
        semantic_version=semantic_version,
        abi_version=abi_version,
    )


def sort_pack_plans(
    pack_plans: Iterable[tuple[str, str, QueryPackPlanV1]],
) -> tuple[tuple[str, str, QueryPackPlanV1], ...]:
    """Sort query packs from highest to lowest planned score.

    Returns:
        tuple[tuple[str, str, QueryPackPlanV1], ...]: Sorted pack plans.

    Input tuple shape is ``(pack_name, source, plan)`` so callers keep pack source
    text aligned with the planned order.
    """
    return tuple(
        sorted(
            pack_plans,
            key=lambda item: (item[2].score, item[2].query_hash),
            reverse=True,
        )
    )


def compile_pack_source_rows(
    *,
    language: str,
    source_rows: Iterable[tuple[str, str]],
    request_surface: str = "artifact",
    ignored_errors: tuple[type[Exception], ...] = (),
) -> tuple[tuple[str, str, QueryPackPlanV1], ...]:
    """Compile one language's query-pack source rows into planned rows.

    Parameters
    ----------
    language
        Query language for compilation.
    source_rows
        Iterable of ``(pack_name, source)`` pairs.
    request_surface
        Query specialization surface passed to the compiler.
    ignored_errors
        Optional exception types to suppress while compiling individual packs.

    Returns:
    -------
    tuple[tuple[str, str, QueryPackPlanV1], ...]
        Sorted pack rows as ``(pack_name, source, plan)``.
    """
    from tools.cq.search.tree_sitter.query.compiler import compile_query

    rows: list[tuple[str, str, QueryPackPlanV1]] = []
    for pack_name, source in source_rows:
        if not pack_name.endswith(".scm"):
            continue
        if ignored_errors:
            try:
                plan = build_pack_plan(
                    pack_name=pack_name,
                    query=compile_query(
                        language=language,
                        pack_name=pack_name,
                        source=source,
                        request_surface=request_surface,
                    ),
                    query_text=source,
                    language=language,
                )
            except ignored_errors:
                continue
        else:
            plan = build_pack_plan(
                pack_name=pack_name,
                query=compile_query(
                    language=language,
                    pack_name=pack_name,
                    source=source,
                    request_surface=request_surface,
                ),
                query_text=source,
                language=language,
            )
        rows.append((pack_name, source, plan))
    return sort_pack_plans(rows)


def resolve_pack_source_rows(
    *,
    language: str,
    source_rows: Iterable[tuple[str, str]],
    dedupe_by_pack_name: bool = False,
    request_surface: str = "artifact",
    ignored_errors: tuple[type[Exception], ...] = (),
) -> tuple[tuple[str, str, QueryPackPlanV1], ...]:
    """Normalize and compile pack-source rows into sorted plan rows.

    Returns:
        tuple[tuple[str, str, QueryPackPlanV1], ...]: Sorted pack rows.
    """
    normalized = normalize_pack_source_rows(
        source_rows,
        dedupe_by_pack_name=dedupe_by_pack_name,
    )
    return compile_pack_source_rows(
        language=language,
        source_rows=normalized,
        request_surface=request_surface,
        ignored_errors=ignored_errors,
    )


def normalize_pack_source_rows(
    source_rows: Iterable[tuple[str, str]],
    *,
    dedupe_by_pack_name: bool = False,
) -> tuple[tuple[str, str], ...]:
    """Normalize raw query-pack source rows before compilation.

    Parameters
    ----------
    source_rows
        Raw ``(pack_name, source)`` rows.
    dedupe_by_pack_name
        When true, keep one row per pack name (last row wins) and return rows in
        deterministic sorted order.

    Returns:
    -------
    tuple[tuple[str, str], ...]
        Filtered/normalized rows for ``compile_pack_source_rows``.
    """
    if dedupe_by_pack_name:
        deduped: dict[str, str] = {}
        for pack_name, source in source_rows:
            if not pack_name.endswith(".scm"):
                continue
            deduped[pack_name] = source
        return tuple(sorted(deduped.items()))

    rows: list[tuple[str, str]] = []
    for pack_name, source in source_rows:
        if pack_name.endswith(".scm"):
            rows.append((pack_name, source))
    return tuple(rows)


__all__ = [
    "build_pack_plan",
    "build_pattern_plan",
    "compile_pack_source_rows",
    "normalize_pack_source_rows",
    "resolve_pack_source_rows",
    "sort_pack_plans",
]
