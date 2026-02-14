"""Phased pipeline for smart-search language partition execution."""

from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import msgspec

from tools.cq.core.cache import (
    CacheWriteTagRequestV1,
    CqCacheBackend,
    CqCachePolicyV1,
    build_cache_key,
    build_scope_hash,
    build_scope_snapshot_fingerprint,
    default_cache_policy,
    file_content_hash,
    get_cq_cache_backend,
    is_namespace_cache_enabled,
    record_cache_decode_failure,
    record_cache_get,
    record_cache_set,
    resolve_namespace_ttl_seconds,
    resolve_write_cache_tag,
)
from tools.cq.core.cache.contracts import SearchCandidatesCacheV1, SearchEnrichmentAnchorCacheV1
from tools.cq.core.contracts import contract_to_builtins, require_mapping
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.query.language import QueryLanguage, constrain_include_globs_for_language
from tools.cq.query.sg_parser import list_scan_files
from tools.cq.search.classifier import QueryMode
from tools.cq.search.partition_contracts import SearchPartitionPlanV1

if TYPE_CHECKING:
    from tools.cq.search.context import SmartSearchContext


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


def run_search_partition(
    plan: SearchPartitionPlanV1,
    *,
    ctx: SmartSearchContext,
    mode: QueryMode,
) -> object:
    """Run one language partition search flow with phased cache orchestration."""
    lang = plan.language
    sm = _smart_search_module()
    scope = _scope_context(ctx, lang, mode)
    raw_matches, stats, pattern = _candidate_phase(
        ctx=ctx,
        lang=lang,
        mode=mode,
        scope=scope,
        smart_search_mod=sm,
    )
    prefetch_future = _maybe_submit_prefetch(
        ctx=ctx,
        lang=lang,
        raw_matches=raw_matches,
        smart_search_mod=sm,
    )
    enriched = _enrichment_phase(
        ctx=ctx,
        lang=lang,
        raw_matches=raw_matches,
        scope=scope,
        smart_search_mod=sm,
    )
    pyrefly_prefetch = _resolve_prefetch_result(prefetch_future, smart_search_mod=sm)
    return sm._LanguageSearchResult(
        lang=lang,
        raw_matches=raw_matches,
        stats=stats,
        pattern=pattern,
        enriched_matches=enriched,
        dropped_by_scope=int(getattr(stats, "dropped_by_scope", 0)),
        pyrefly_prefetch=pyrefly_prefetch,
    )


def _smart_search_module() -> Any:
    from tools.cq.search import smart_search as smart_search_module

    return smart_search_module


def _scope_context(
    ctx: SmartSearchContext,
    lang: QueryLanguage,
    mode: QueryMode,
) -> _PartitionScopeContext:
    resolved_root = ctx.root.resolve()
    include_globs = constrain_include_globs_for_language(ctx.include_globs, lang) or []
    scope_globs = list(include_globs)
    scope_globs.extend(
        exclude if exclude.startswith("!") else f"!{exclude}"
        for exclude in (ctx.exclude_globs or [])
    )
    scope_files = list_scan_files(
        paths=[resolved_root],
        root=resolved_root,
        globs=scope_globs or None,
        lang=lang,
    )
    scope_snapshot = build_scope_snapshot_fingerprint(
        root=resolved_root,
        files=scope_files,
        language=lang,
        scope_globs=scope_globs,
        scope_roots=[resolved_root],
    )
    scope_hash = build_scope_hash(
        {
            "scope_roots": (str(resolved_root),),
            "scope_globs": tuple(scope_globs),
            "mode": mode.value,
            "query": ctx.query,
        }
    )
    return _PartitionScopeContext(
        root=resolved_root,
        cache=get_cq_cache_backend(root=resolved_root),
        policy=default_cache_policy(root=resolved_root),
        scope_globs=scope_globs,
        scope_hash=scope_hash,
        snapshot_digest=scope_snapshot.digest,
    )


def _candidate_phase(
    *,
    ctx: SmartSearchContext,
    lang: QueryLanguage,
    mode: QueryMode,
    scope: _PartitionScopeContext,
    smart_search_mod: Any,
) -> tuple[list[object], object, str]:
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
            hit=isinstance(cached_raw, dict),
            key=cache_key,
        )
    raw_matches, stats, pattern, hit = _candidate_payload_from_cache(
        cached=cached_raw,
        ctx=ctx,
        lang=lang,
        mode=mode,
        smart_search_mod=smart_search_mod,
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
            contract_to_builtins(payload),
            expire=ttl_seconds,
            tag=tag,
        )
        record_cache_set(namespace=namespace, ok=ok, key=cache_key)
    return raw_matches, stats, pattern


def _candidate_payload_from_cache(
    *,
    cached: object,
    ctx: SmartSearchContext,
    lang: QueryLanguage,
    mode: QueryMode,
    smart_search_mod: Any,
) -> tuple[list[object], object, str, bool]:
    if isinstance(cached, dict):
        try:
            payload = msgspec.convert(cached, type=SearchCandidatesCacheV1)
            raw_matches = [
                msgspec.convert(item, type=smart_search_mod.RawMatch)
                for item in payload.raw_matches
            ]
            stats = msgspec.convert(payload.stats, type=smart_search_mod.SearchStats)
            return raw_matches, stats, payload.pattern, True
        except (RuntimeError, TypeError, ValueError):
            record_cache_decode_failure(namespace="search_candidates")
    raw_matches, stats, pattern = smart_search_mod._run_candidate_phase(ctx, lang=lang, mode=mode)
    return raw_matches, stats, pattern, False


def _maybe_submit_prefetch(
    *,
    ctx: SmartSearchContext,
    lang: QueryLanguage,
    raw_matches: list[Any],
    smart_search_mod: Any,
) -> Future[object] | None:
    if lang != "python" or not raw_matches:
        return None
    return get_worker_scheduler().submit_io(
        smart_search_mod._prefetch_pyrefly_for_raw_matches,
        ctx,
        lang=lang,
        raw_matches=raw_matches,
    )


def _enrichment_phase(
    *,
    ctx: SmartSearchContext,
    lang: QueryLanguage,
    raw_matches: list[Any],
    scope: _PartitionScopeContext,
    smart_search_mod: Any,
) -> list[object]:
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
        smart_search_mod=smart_search_mod,
    )
    if misses:
        _compute_and_persist_enrichment_misses(
            ctx=ctx,
            misses=misses,
            enriched_results=enriched_results,
            context=context,
            smart_search_mod=smart_search_mod,
        )
    return [
        match for match in enriched_results if isinstance(match, smart_search_mod.EnrichedMatch)
    ]


def _probe_enrichment_cache(
    *,
    raw_matches: list[Any],
    context: _EnrichmentCacheContext,
    smart_search_mod: Any,
) -> tuple[list[Any | None], list[tuple[int, Any, str, str]]]:
    enriched_results: list[Any | None] = [None] * len(raw_matches)
    misses: list[tuple[int, Any, str, str]] = []
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
                hit=isinstance(cached, dict),
                key=cache_key,
            )
            if isinstance(cached, dict):
                decoded = _decode_enrichment_cached(
                    cached=cached, smart_search_mod=smart_search_mod
                )
                if decoded is not None:
                    enriched_results[idx] = decoded
                    continue
        misses.append((idx, raw, cache_key, file_hash))
    return enriched_results, misses


def _decode_enrichment_cached(*, cached: dict[str, object], smart_search_mod: Any) -> Any | None:
    try:
        payload = msgspec.convert(cached, type=SearchEnrichmentAnchorCacheV1)
        return msgspec.convert(payload.enriched_match, type=smart_search_mod.EnrichedMatch)
    except (RuntimeError, TypeError, ValueError):
        record_cache_decode_failure(namespace="search_enrichment")
        return None


def _compute_and_persist_enrichment_misses(
    *,
    ctx: SmartSearchContext,
    misses: list[tuple[int, Any, str, str]],
    enriched_results: list[Any | None],
    context: _EnrichmentCacheContext,
    smart_search_mod: Any,
) -> None:
    miss_raw_matches = [item[1] for item in misses]
    miss_enriched = smart_search_mod._run_classification_phase(
        ctx,
        lang=context.lang,
        raw_matches=miss_raw_matches,
    )
    with context.cache.transact():
        for miss_idx, (idx, raw, cache_key, file_hash) in enumerate(misses):
            if miss_idx >= len(miss_enriched):
                break
            enriched_match = miss_enriched[miss_idx]
            enriched_results[idx] = enriched_match
            if not (context.cache_enabled and file_hash):
                continue
            payload = SearchEnrichmentAnchorCacheV1(
                file=raw.file,
                line=max(1, int(raw.line)),
                col=max(0, int(raw.col)),
                match_text=raw.match_text,
                file_content_hash=file_hash,
                language=context.lang,
                enriched_match=cast(
                    "dict[str, object]",
                    contract_to_builtins(enriched_match),
                ),
            )
            ok = context.cache.set(
                cache_key,
                contract_to_builtins(payload),
                expire=context.ttl_seconds,
                tag=context.tag,
            )
            record_cache_set(namespace=context.namespace, ok=ok, key=cache_key)


def _resolve_prefetch_result(
    prefetch_future: Future[object] | None,
    *,
    smart_search_mod: Any,
) -> object | None:
    if prefetch_future is None:
        return None
    try:
        return prefetch_future.result()
    except (OSError, RuntimeError, TimeoutError, ValueError, TypeError):
        return smart_search_mod._PyreflyPrefetchResult(
            telemetry=smart_search_mod._new_pyrefly_telemetry()
        )


__all__ = ["run_search_partition"]
