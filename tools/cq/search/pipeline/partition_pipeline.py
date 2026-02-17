"""Phased pipeline for smart-search language partition execution."""

from __future__ import annotations

import multiprocessing
from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import msgspec

from tools.cq.core.cache.content_hash import file_content_hash
from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend
from tools.cq.core.cache.fragment_codecs import (
    decode_fragment_payload,
    encode_fragment_payload,
    is_fragment_cache_payload,
)
from tools.cq.core.cache.interface import CqCacheBackend
from tools.cq.core.cache.key_builder import build_cache_key, build_scope_hash
from tools.cq.core.cache.namespaces import (
    is_namespace_cache_enabled,
    resolve_namespace_ttl_seconds,
)
from tools.cq.core.cache.policy import CqCachePolicyV1, default_cache_policy
from tools.cq.core.cache.run_lifecycle import CacheWriteTagRequestV1, resolve_write_cache_tag
from tools.cq.core.cache.telemetry import (
    record_cache_decode_failure,
    record_cache_get,
    record_cache_set,
)
from tools.cq.core.contracts import contract_to_builtins, require_mapping
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.core.types import QueryLanguage
from tools.cq.query.language import constrain_include_globs_for_language
from tools.cq.search._shared.types import QueryMode
from tools.cq.search.cache.contracts import SearchCandidatesCacheV1, SearchEnrichmentAnchorCacheV1
from tools.cq.search.pipeline.candidate_phase import run_candidate_phase
from tools.cq.search.pipeline.classification import classify_match
from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext
from tools.cq.search.pipeline.contracts import SearchPartitionPlanV1
from tools.cq.search.pipeline.smart_search_types import (
    EnrichedMatch,
    LanguageSearchResult,
    RawMatch,
    SearchStats,
)
from tools.cq.search.pipeline.worker_policy import resolve_search_worker_count
from tools.cq.search.tree_sitter.core.infrastructure import run_file_lanes_parallel

if TYPE_CHECKING:
    from tools.cq.search.pipeline.contracts import SearchConfig
    from tools.cq.search.pipeline.smart_search_types import _PythonSemanticPrefetchResult


@dataclass(frozen=True, slots=True)
class _PartitionScopeContext:
    root: Path
    cache: CqCacheBackend
    policy: CqCachePolicyV1
    scope_globs: list[str]
    scope_hash: str | None
    snapshot_digest: str


@dataclass(frozen=True, slots=True)
class _EnrichmentCacheContext:
    cache: CqCacheBackend
    cache_enabled: bool
    namespace: str
    root: Path
    lang: QueryLanguage
    ttl_seconds: int
    tag: str | None


@dataclass(frozen=True, slots=True)
class _EnrichmentMissTask:
    root: str
    lang: QueryLanguage
    items: list[tuple[int, RawMatch, str, str]]


@dataclass(frozen=True, slots=True)
class _EnrichmentMissResult:
    idx: int
    raw: RawMatch
    cache_key: str
    file_hash: str
    enriched_match: EnrichedMatch


def run_search_partition(
    plan: SearchPartitionPlanV1,
    *,
    ctx: SearchConfig,
    mode: QueryMode,
) -> object:
    """Run one language partition search flow with phased cache orchestration.

    Returns:
        object: Language-search result payload for this partition.
    """
    lang = plan.language
    scope = _scope_context(ctx, lang, mode)
    raw_matches, stats, pattern = _candidate_phase(
        ctx=ctx,
        lang=lang,
        mode=mode,
        scope=scope,
    )
    prefetch_future = _maybe_submit_prefetch(
        ctx=ctx,
        lang=lang,
        raw_matches=raw_matches,
    )
    enriched = _enrichment_phase(
        ctx=ctx,
        lang=lang,
        raw_matches=raw_matches,
        scope=scope,
    )
    python_semantic_prefetch = _resolve_prefetch_result(prefetch_future)
    return LanguageSearchResult(
        lang=lang,
        raw_matches=raw_matches,
        stats=stats,
        pattern=pattern,
        enriched_matches=enriched,
        dropped_by_scope=int(getattr(stats, "dropped_by_scope", 0)),
        python_semantic_prefetch=python_semantic_prefetch,
    )


def _scope_context(
    ctx: SearchConfig,
    lang: QueryLanguage,
    mode: QueryMode,
) -> _PartitionScopeContext:
    resolved_root = ctx.root.resolve()
    cache = get_cq_cache_backend(root=resolved_root)
    policy = default_cache_policy(root=resolved_root)
    include_globs = constrain_include_globs_for_language(ctx.include_globs, lang) or []
    scope_globs = list(include_globs)
    scope_globs.extend(
        exclude if exclude.startswith("!") else f"!{exclude}"
        for exclude in (ctx.exclude_globs or [])
    )
    cache_enabled = is_namespace_cache_enabled(
        policy=policy, namespace="search_candidates"
    ) or is_namespace_cache_enabled(policy=policy, namespace="search_enrichment")
    snapshot_digest = "cache_disabled"
    if cache_enabled:
        # Keep cache strictly query/run scoped in the hot path: no repository-wide
        # file tabulation or snapshot building before candidate collection.
        snapshot_digest = (
            build_scope_hash(
                {
                    "scope_roots": (str(resolved_root),),
                    "scope_globs": tuple(scope_globs),
                    "mode": mode.value,
                    "query": ctx.query,
                    "language": lang,
                    "include_strings": ctx.include_strings,
                    "run_id": ctx.run_id or "",
                }
            )
            or f"run:{ctx.run_id or 'unknown'}"
        )
    scope_hash = build_scope_hash(
        {
            "scope_roots": (str(resolved_root),),
            "scope_globs": tuple(scope_globs),
            "mode": mode.value,
            "query": ctx.query,
            "run_id": ctx.run_id or "",
        }
    )
    return _PartitionScopeContext(
        root=resolved_root,
        cache=cache,
        policy=policy,
        scope_globs=scope_globs,
        scope_hash=scope_hash,
        snapshot_digest=snapshot_digest,
    )


def _candidate_phase(
    *,
    ctx: SearchConfig,
    lang: QueryLanguage,
    mode: QueryMode,
    scope: _PartitionScopeContext,
) -> tuple[list[RawMatch], SearchStats, str]:
    namespace = "search_candidates"
    cache_enabled = is_namespace_cache_enabled(policy=scope.policy, namespace=namespace)
    ttl_seconds = resolve_namespace_ttl_seconds(policy=scope.policy, namespace=namespace)
    cache_key = build_cache_key(
        namespace,
        version="v2",
        workspace=str(scope.root),
        language=lang,
        target=ctx.query,
        extras={
            "mode": mode.value,
            "include_strings": ctx.include_strings,
            "include_globs": tuple(ctx.include_globs or ()),
            "exclude_globs": tuple(ctx.exclude_globs or ()),
            "max_total_matches": ctx.limits.max_total_matches,
            "snapshot_digest": scope.snapshot_digest,
        },
    )
    cached_raw = None
    if cache_enabled:
        cached_raw = scope.cache.get(cache_key)
        record_cache_get(
            namespace=namespace,
            hit=is_fragment_cache_payload(cached_raw),
            key=cache_key,
        )
    raw_matches, stats, pattern, hit = _candidate_payload_from_cache(
        cached=cached_raw,
        ctx=ctx,
        lang=lang,
        mode=mode,
    )
    if cache_enabled and not hit:
        payload = SearchCandidatesCacheV1(
            pattern=pattern,
            raw_matches=cast("list[dict[str, object]]", contract_to_builtins(raw_matches)),
            stats=require_mapping(stats),
        )
        tag = resolve_write_cache_tag(
            CacheWriteTagRequestV1(
                policy=scope.policy,
                workspace=str(scope.root),
                language=lang,
                namespace=namespace,
                scope_hash=scope.scope_hash,
                snapshot=scope.snapshot_digest,
                run_id=ctx.run_id,
            )
        )
        ok = scope.cache.set(
            cache_key,
            encode_fragment_payload(payload),
            expire=ttl_seconds,
            tag=tag,
        )
        record_cache_set(namespace=namespace, ok=ok, key=cache_key)
    return raw_matches, stats, pattern


def _candidate_payload_from_cache(
    *,
    cached: object,
    ctx: SearchConfig,
    lang: QueryLanguage,
    mode: QueryMode,
) -> tuple[list[RawMatch], SearchStats, str, bool]:
    payload = decode_fragment_payload(cached, type_=SearchCandidatesCacheV1)
    if payload is not None:
        try:
            raw_matches = [msgspec.convert(item, type=RawMatch) for item in payload.raw_matches]
            stats = msgspec.convert(payload.stats, type=SearchStats)
        except (RuntimeError, TypeError, ValueError):
            record_cache_decode_failure(namespace="search_candidates")
        else:
            return raw_matches, stats, payload.pattern, True
    elif is_fragment_cache_payload(cached):
        record_cache_decode_failure(namespace="search_candidates")

    raw_matches, stats, pattern = run_candidate_phase(ctx, lang=lang, mode=mode)
    return raw_matches, stats, pattern, False


def _maybe_submit_prefetch(
    *,
    ctx: SearchConfig,
    lang: QueryLanguage,
    raw_matches: list[RawMatch],
) -> Future[object] | None:
    if lang != "python" or not raw_matches:
        return None
    from tools.cq.search.pipeline.python_semantic import (
        run_prefetch_python_semantic_for_raw_matches,
    )

    return get_worker_scheduler().submit_io(
        run_prefetch_python_semantic_for_raw_matches,
        ctx,
        lang=lang,
        raw_matches=raw_matches,
    )


def _enrichment_phase(
    *,
    ctx: SearchConfig,
    lang: QueryLanguage,
    raw_matches: list[RawMatch],
    scope: _PartitionScopeContext,
) -> list[EnrichedMatch]:
    namespace = "search_enrichment"
    context = _EnrichmentCacheContext(
        cache=scope.cache,
        cache_enabled=is_namespace_cache_enabled(policy=scope.policy, namespace=namespace),
        namespace=namespace,
        root=scope.root,
        lang=lang,
        ttl_seconds=resolve_namespace_ttl_seconds(policy=scope.policy, namespace=namespace),
        tag=resolve_write_cache_tag(
            CacheWriteTagRequestV1(
                policy=scope.policy,
                workspace=str(scope.root),
                language=lang,
                namespace=namespace,
                scope_hash=scope.scope_hash,
                snapshot=scope.snapshot_digest,
                run_id=ctx.run_id,
            )
        ),
    )
    enriched_results, misses = _probe_enrichment_cache(
        raw_matches=raw_matches,
        context=context,
    )
    if misses:
        _compute_and_persist_enrichment_misses(
            ctx=ctx,
            misses=misses,
            enriched_results=enriched_results,
            context=context,
        )
    return [match for match in enriched_results if isinstance(match, EnrichedMatch)]


def _probe_enrichment_cache(
    *,
    raw_matches: list[RawMatch],
    context: _EnrichmentCacheContext,
) -> tuple[list[EnrichedMatch | None], list[tuple[int, RawMatch, str, str]]]:
    enriched_results: list[EnrichedMatch | None] = [None] * len(raw_matches)
    misses: list[tuple[int, RawMatch, str, str]] = []
    for idx, raw in enumerate(raw_matches):
        file_hash = file_content_hash(context.root / raw.file).digest
        cache_key = build_cache_key(
            context.namespace,
            version="v2",
            workspace=str(context.root),
            language=context.lang,
            target=f"{raw.file}:{raw.line}:{raw.col}",
            extras={
                "match_text": raw.match_text,
                "match_start": raw.match_start,
                "match_end": raw.match_end,
                "submatch_index": raw.submatch_index,
                "file_content_hash": file_hash,
            },
        )
        if context.cache_enabled and file_hash:
            cached = context.cache.get(cache_key)
            record_cache_get(
                namespace=context.namespace,
                hit=is_fragment_cache_payload(cached),
                key=cache_key,
            )
            decoded = _decode_enrichment_cached(cached=cached)
            if decoded is not None:
                enriched_results[idx] = decoded
                continue
        misses.append((idx, raw, cache_key, file_hash))
    return enriched_results, misses


def _decode_enrichment_cached(*, cached: object) -> EnrichedMatch | None:
    payload = decode_fragment_payload(cached, type_=SearchEnrichmentAnchorCacheV1)
    if payload is None:
        if is_fragment_cache_payload(cached):
            record_cache_decode_failure(namespace="search_enrichment")
        return None
    try:
        return msgspec.convert(payload.enriched_match, type=EnrichedMatch)
    except (RuntimeError, TypeError, ValueError):
        record_cache_decode_failure(namespace="search_enrichment")
        return None


def _compute_and_persist_enrichment_misses(
    *,
    ctx: SearchConfig,
    misses: list[tuple[int, RawMatch, str, str]],
    enriched_results: list[EnrichedMatch | None],
    context: _EnrichmentCacheContext,
) -> None:
    miss_results = _classify_enrichment_misses(
        ctx=ctx,
        misses=misses,
        context=context,
    )
    with context.cache.transact():
        for row in miss_results:
            enriched_results[row.idx] = row.enriched_match
            if not (context.cache_enabled and row.file_hash):
                continue
            payload = SearchEnrichmentAnchorCacheV1(
                file=row.raw.file,
                line=max(1, int(row.raw.line)),
                col=max(0, int(row.raw.col)),
                match_text=row.raw.match_text,
                file_content_hash=row.file_hash,
                language=context.lang,
                enriched_match=cast(
                    "dict[str, object]",
                    contract_to_builtins(row.enriched_match),
                ),
            )
            ok = context.cache.set(
                row.cache_key,
                encode_fragment_payload(payload),
                expire=context.ttl_seconds,
                tag=context.tag,
            )
            record_cache_set(namespace=context.namespace, ok=ok, key=row.cache_key)


def _classify_enrichment_misses(
    *,
    ctx: SearchConfig,
    misses: list[tuple[int, RawMatch, str, str]],
    context: _EnrichmentCacheContext,
) -> list[_EnrichmentMissResult]:
    if len(misses) <= 1:
        return _classify_enrichment_miss_batch(
            _EnrichmentMissTask(
                root=str(ctx.root),
                lang=context.lang,
                items=misses,
            )
        )

    partitioned: dict[str, list[tuple[int, RawMatch, str, str]]] = {}
    for item in misses:
        partitioned.setdefault(str(item[1].file), []).append(item)
    batches = list(partitioned.values())
    workers = resolve_search_worker_count(len(batches))
    if workers <= 1:
        return _classify_enrichment_miss_batch(
            _EnrichmentMissTask(
                root=str(ctx.root),
                lang=context.lang,
                items=misses,
            )
        )

    tasks = [
        _EnrichmentMissTask(
            root=str(ctx.root),
            lang=context.lang,
            items=batch,
        )
        for batch in batches
    ]
    try:
        batches_out = run_file_lanes_parallel(
            tasks,
            worker=_classify_enrichment_miss_batch,
            max_workers=workers,
        )
        rows: list[_EnrichmentMissResult] = []
        for done_batch in batches_out:
            rows.extend(done_batch)
    except (
        multiprocessing.ProcessError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
    ):
        rows = _classify_enrichment_miss_batch(
            _EnrichmentMissTask(
                root=str(ctx.root),
                lang=context.lang,
                items=misses,
            )
        )
    rows.sort(key=lambda item: item.idx)
    return rows


def _classify_enrichment_miss_batch(
    task: _EnrichmentMissTask,
) -> list[_EnrichmentMissResult]:
    root = Path(task.root)
    cache_context = ClassifierCacheContext()
    deep_enrichment_seen: set[tuple[str, str]] = set()
    results: list[_EnrichmentMissResult] = []
    for idx, raw, cache_key, file_hash in task.items:
        enrichment_key = (str(raw.file), str(raw.match_text))
        enable_deep_enrichment = enrichment_key not in deep_enrichment_seen
        if enable_deep_enrichment:
            deep_enrichment_seen.add(enrichment_key)
        results.append(
            _EnrichmentMissResult(
                idx=idx,
                raw=raw,
                cache_key=cache_key,
                file_hash=file_hash,
                enriched_match=classify_match(
                    raw,
                    root,
                    lang=task.lang,
                    cache_context=cache_context,
                    enable_deep_enrichment=enable_deep_enrichment,
                ),
            )
        )
    return results


def _resolve_prefetch_result(
    prefetch_future: Future[object] | None,
) -> _PythonSemanticPrefetchResult | None:
    if prefetch_future is None:
        return None
    try:
        return cast("_PythonSemanticPrefetchResult | None", prefetch_future.result())
    except (OSError, RuntimeError, TimeoutError, ValueError, TypeError):
        from tools.cq.search.pipeline.smart_search_telemetry import (
            new_python_semantic_telemetry,
        )
        from tools.cq.search.pipeline.smart_search_types import (
            _PythonSemanticPrefetchResult as _PrefetchResult,
        )

        return _PrefetchResult(telemetry=new_python_semantic_telemetry())


__all__ = ["run_search_partition"]
