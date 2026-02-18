"""Language-scope execution and deterministic partition merge helpers."""

from __future__ import annotations

from collections.abc import Callable, Mapping

from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.core.types import QueryLanguage, QueryLanguageScope, expand_language_scope


def language_priority(scope: QueryLanguageScope) -> dict[QueryLanguage, int]:
    """Return deterministic language ordering for a scope.

    Returns:
        Priority map keyed by language.
    """
    return {lang: idx for idx, lang in enumerate(expand_language_scope(scope))}


def execute_by_language_scope[T](
    scope: QueryLanguageScope,
    run_one: Callable[[QueryLanguage], T],
) -> dict[QueryLanguage, T]:
    """Execute one callback per language in scope.

    Returns:
        Mapping from language to callback result.
    """
    languages = tuple(expand_language_scope(scope))
    if len(languages) == 0:
        return {}
    if len(languages) == 1:
        only_language = languages[0]
        return {only_language: run_one(only_language)}

    scheduler = get_worker_scheduler()
    policy = scheduler.policy
    if policy.query_partition_workers <= 1:
        return {lang: run_one(lang) for lang in languages}

    futures = [scheduler.submit_io(run_one, lang) for lang in languages]
    batch = scheduler.collect_bounded(
        futures,
        timeout_seconds=max(1.0, float(len(languages)) * 5.0),
    )
    if batch.timed_out > 0:
        return {lang: run_one(lang) for lang in languages}
    return dict(zip(languages, batch.done, strict=False))


def merge_partitioned_items[T](
    *,
    partitions: Mapping[QueryLanguage, list[T]],
    scope: QueryLanguageScope,
    get_language: Callable[[T], QueryLanguage],
    get_score: Callable[[T], float],
    get_location: Callable[[T], tuple[str, int, int]],
) -> list[T]:
    """Merge and sort language partitions with stable deterministic ordering.

    Returns:
        Merged items sorted by language priority, score, and location.
    """
    priority = language_priority(scope)
    merged: list[T] = []
    for lang in expand_language_scope(scope):
        merged.extend(partitions.get(lang, []))
    for lang, items in partitions.items():
        if lang not in priority:
            merged.extend(items)
    merged.sort(
        key=lambda item: (
            priority.get(get_language(item), 99),
            -get_score(item),
            *get_location(item),
        )
    )
    return merged


__all__ = [
    "execute_by_language_scope",
    "language_priority",
    "merge_partitioned_items",
]
