"""Classification phase for Smart Search."""

from __future__ import annotations

import multiprocessing
from pathlib import Path

from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.core.types import QueryLanguage
from tools.cq.query.language import is_path_in_lang_scope
from tools.cq.search.pipeline.classification import classify_match
from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.smart_search_types import (
    ClassificationBatchResult,
    ClassificationBatchTask,
    EnrichedMatch,
    MatchClassifyOptions,
    RawMatch,
)
from tools.cq.search.pipeline.worker_policy import resolve_search_worker_count


def _classify_partition_batch(
    task: ClassificationBatchTask,
) -> list[ClassificationBatchResult]:
    root = Path(task.root)
    cache_context = ClassifierCacheContext()
    options = MatchClassifyOptions(
        incremental_enabled=task.incremental_enrichment_enabled,
        incremental_mode=task.incremental_enrichment_mode,
    )
    return [
        ClassificationBatchResult(
            index=idx,
            match=classify_match(
                raw,
                root,
                lang=task.lang,
                cache_context=cache_context,
                options=options,
            ),
        )
        for idx, raw in task.batch
    ]


def run_classify_phase(
    config: SearchConfig,
    *,
    lang: QueryLanguage,
    raw_matches: list[RawMatch],
    cache_context: ClassifierCacheContext,
) -> list[EnrichedMatch]:
    """Run match classification/enrichment for one language partition.

    Returns:
    -------
    list[EnrichedMatch]
        Classified and enriched matches in source order.
    """
    filtered_raw_matches = [m for m in raw_matches if is_path_in_lang_scope(m.file, lang)]
    if not filtered_raw_matches:
        return []
    options = MatchClassifyOptions(
        incremental_enabled=config.incremental_enrichment_enabled,
        incremental_mode=config.incremental_enrichment_mode,
    )

    indexed: list[tuple[int, RawMatch]] = list(enumerate(filtered_raw_matches))
    partitioned: dict[str, list[tuple[int, RawMatch]]] = {}
    for idx, raw in indexed:
        partitioned.setdefault(raw.file, []).append((idx, raw))
    batches = list(partitioned.values())
    workers = resolve_search_worker_count(len(batches))

    if workers <= 1 or len(batches) <= 1:
        return [
            classify_match(
                raw, config.root, lang=lang, cache_context=cache_context, options=options
            )
            for raw in filtered_raw_matches
        ]

    tasks = [
        ClassificationBatchTask(
            root=str(config.root),
            lang=lang,
            incremental_enrichment_enabled=config.incremental_enrichment_enabled,
            incremental_enrichment_mode=config.incremental_enrichment_mode,
            batch=batch,
        )
        for batch in batches
    ]
    scheduler = get_worker_scheduler()
    try:
        futures = [
            scheduler.submit_cpu(_classify_partition_batch, task) for task in tasks[:workers]
        ]
        futures.extend(
            scheduler.submit_cpu(_classify_partition_batch, task) for task in tasks[workers:]
        )
        batch = scheduler.collect_bounded(
            futures,
            timeout_seconds=max(1.0, float(len(tasks))),
        )
        if batch.timed_out > 0:
            return [
                classify_match(
                    raw,
                    config.root,
                    lang=lang,
                    cache_context=cache_context,
                    options=options,
                )
                for raw in filtered_raw_matches
            ]
        indexed_results: list[tuple[int, EnrichedMatch]] = []
        for batch_results in batch.done:
            indexed_results.extend((item.index, item.match) for item in batch_results)
    except (
        multiprocessing.ProcessError,
        OSError,
        RuntimeError,
        TimeoutError,
        ValueError,
        TypeError,
    ):
        return [
            classify_match(
                raw,
                config.root,
                lang=lang,
                cache_context=cache_context,
                options=options,
            )
            for raw in filtered_raw_matches
        ]

    indexed_results.sort(key=lambda pair: pair[0])
    return [match for _idx, match in indexed_results]


__all__ = ["run_classify_phase"]
