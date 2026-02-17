"""Phased execution pipeline for language-aware static semantic enrichment."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from pathlib import Path

from tools.cq.core.cache.content_hash import file_content_hash
from tools.cq.core.cache.coordination import publish_once_per_barrier, tree_sitter_lane_guard
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
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.search._shared.core import PythonByteRangeEnrichmentRequest, line_col_to_byte_offset
from tools.cq.search.pipeline.classifier import get_sg_root
from tools.cq.search.python.extractors import enrich_python_context_by_byte_range
from tools.cq.search.rust.enrichment import enrich_rust_context_by_byte_range
from tools.cq.search.semantic.models import (
    LanguageSemanticEnrichmentOutcome,
    LanguageSemanticEnrichmentRequest,
    SemanticOutcomeCacheV1,
    SemanticOutcomeV1,
    SemanticRequestBudgetV1,
    budget_for_mode,
    build_static_semantic_planes,
    call_with_retry,
    enrich_with_language_semantics,
    resolve_language_provider_root,
)
from tools.cq.search.tree_sitter.core.adaptive_runtime import adaptive_query_budget_ms


@dataclass(frozen=True, slots=True)
class _PipelineContext:
    provider_root: Path
    target_file_path: Path
    policy: CqCachePolicyV1
    cache: CqCacheBackend
    cache_enabled: bool
    namespace_ttl: int
    cache_key: str
    lock_key: str
    scope_hash: str | None
    snapshot_digest: str
    budget: SemanticRequestBudgetV1
    lane_limit: int
    lane_ttl_seconds: int


def run_language_semantic_enrichment(
    request: LanguageSemanticEnrichmentRequest,
    *,
    cache_namespace: str,
    lock_retry_count: int,
    lock_retry_sleep_seconds: float,
    runtime_enabled: bool,
) -> LanguageSemanticEnrichmentOutcome:
    """Run language-aware static enrichment with cache probe/single-flight/writeback.

    Returns:
        LanguageSemanticEnrichmentOutcome: Enrichment outcome for the request.
    """
    context = _build_context(request=request, cache_namespace=cache_namespace)
    if not runtime_enabled:
        return LanguageSemanticEnrichmentOutcome(
            timed_out=False,
            failure_reason="budget_exceeded",
            provider_root=context.provider_root,
        )
    cached_outcome = _probe_cached_outcome(context=context, cache_namespace=cache_namespace)
    if cached_outcome is not None:
        return cached_outcome

    _ = (lock_retry_count, lock_retry_sleep_seconds)
    with tree_sitter_lane_guard(
        backend=context.cache,
        lock_key=context.lock_key,
        semaphore_key=f"cq:tree_sitter:lane:{request.language}",
        lane_limit=context.lane_limit,
        ttl_seconds=context.lane_ttl_seconds,
    ):
        waited = _probe_cached_outcome(context=context, cache_namespace=cache_namespace)
        if waited is not None:
            return waited
        outcome = _execute_provider(request=request, context=context)

        def _publish() -> None:
            _persist_outcome(
                context=context,
                outcome=outcome,
                request=request,
                cache_namespace=cache_namespace,
            )

        publish_once_per_barrier(
            backend=context.cache,
            barrier_key=f"{context.lock_key}:publish",
            publish_fn=_publish,
        )
        return outcome


def _build_context(
    *,
    request: LanguageSemanticEnrichmentRequest,
    cache_namespace: str,
) -> _PipelineContext:
    provider_root = resolve_language_provider_root(
        language=request.language,
        command_root=request.root,
        file_path=request.file_path,
    )
    target_file_path = (
        request.file_path if request.file_path.is_absolute() else provider_root / request.file_path
    )
    policy = default_cache_policy(root=provider_root)
    cache = get_cq_cache_backend(root=provider_root)
    cache_enabled = is_namespace_cache_enabled(policy=policy, namespace=cache_namespace)
    namespace_ttl = resolve_namespace_ttl_seconds(policy=policy, namespace=cache_namespace)
    snapshot_digest = file_content_hash(target_file_path).digest
    cache_key = build_cache_key(
        cache_namespace,
        version="v1",
        workspace=str(provider_root.resolve()),
        language=request.language,
        target=str(target_file_path),
        extras={
            "mode": request.mode,
            "line": max(1, int(request.line)),
            "col": max(0, int(request.col)),
            "symbol_hint": request.symbol_hint or "",
            "file_content_hash": snapshot_digest,
        },
    )
    scope_hash = build_scope_hash(
        {
            "provider_root": str(provider_root.resolve()),
            "target_file": str(target_file_path.resolve()),
            "line": max(1, int(request.line)),
            "col": max(0, int(request.col)),
            "symbol_hint": request.symbol_hint or "",
            "mode": request.mode,
        }
    )
    return _PipelineContext(
        provider_root=provider_root,
        target_file_path=target_file_path,
        policy=policy,
        cache=cache,
        cache_enabled=cache_enabled,
        namespace_ttl=namespace_ttl,
        cache_key=cache_key,
        lock_key=f"{cache_key}:lock",
        scope_hash=scope_hash,
        snapshot_digest=snapshot_digest,
        budget=budget_for_mode(request.mode),
        lane_limit=max(1, int(getattr(policy, "max_tree_sitter_lanes", 4))),
        lane_ttl_seconds=max(1, int(getattr(policy, "lane_lock_ttl_seconds", 15))),
    )


def _probe_cached_outcome(
    *,
    context: _PipelineContext,
    cache_namespace: str,
) -> LanguageSemanticEnrichmentOutcome | None:
    if not context.cache_enabled:
        return None
    cached = context.cache.get(context.cache_key)
    record_cache_get(
        namespace=cache_namespace,
        hit=is_fragment_cache_payload(cached),
        key=context.cache_key,
    )
    payload = decode_fragment_payload(cached, type_=SemanticOutcomeCacheV1)
    if payload is None:
        if is_fragment_cache_payload(cached):
            record_cache_decode_failure(namespace=cache_namespace)
        return None
    if not isinstance(payload, SemanticOutcomeCacheV1):
        record_cache_decode_failure(namespace=cache_namespace)
        return None
    return LanguageSemanticEnrichmentOutcome(
        payload=dict(payload.payload) if isinstance(payload.payload, dict) else None,
        timed_out=bool(payload.timed_out),
        failure_reason=payload.failure_reason,
        provider_root=context.provider_root,
    )


def _execute_provider(
    *,
    request: LanguageSemanticEnrichmentRequest,
    context: _PipelineContext,
) -> LanguageSemanticEnrichmentOutcome:
    provider = {
        "python": _execute_python_provider,
        "rust": _execute_rust_provider,
    }.get(request.language, _execute_rust_provider)
    return provider(request=request, context=context)


def _line_col_to_byte_offset(source_bytes: bytes, line: int, col: int) -> int:
    safe_line = max(1, int(line))
    safe_col = max(0, int(col))
    offset = line_col_to_byte_offset(source_bytes, safe_line, safe_col)
    if offset is None:
        return min(len(source_bytes), 0)
    return min(offset, len(source_bytes))


def _source_bytes(path: Path) -> bytes | None:
    try:
        return path.read_bytes()
    except OSError:
        return None


def _execute_python_provider(
    *,
    request: LanguageSemanticEnrichmentRequest,
    context: _PipelineContext,
) -> LanguageSemanticEnrichmentOutcome:
    source_bytes = _source_bytes(context.target_file_path)
    if source_bytes is None:
        return LanguageSemanticEnrichmentOutcome(
            timed_out=False,
            failure_reason="source_unavailable",
            provider_root=context.provider_root,
        )

    byte_start = _line_col_to_byte_offset(source_bytes, request.line, request.col)
    byte_end = min(len(source_bytes), max(byte_start + 1, byte_start + 32))

    payload, timed_out = call_with_retry(
        lambda: _execute_semantic_task(
            lambda: _python_payload(
                request=request,
                context=context,
                source_bytes=source_bytes,
                byte_start=byte_start,
                byte_end=byte_end,
            ),
            timeout_seconds=context.budget.startup_timeout_seconds
            + context.budget.probe_timeout_seconds,
        ),
        max_attempts=context.budget.max_attempts,
        retry_backoff_ms=context.budget.retry_backoff_ms,
    )

    normalized = payload if isinstance(payload, dict) else None
    return LanguageSemanticEnrichmentOutcome(
        payload=dict(normalized) if isinstance(normalized, dict) else None,
        timed_out=timed_out,
        failure_reason=_python_failure_reason(normalized, timed_out=timed_out),
        provider_root=context.provider_root,
    )


def _python_payload(
    *,
    request: LanguageSemanticEnrichmentRequest,
    context: _PipelineContext,
    source_bytes: bytes,
    byte_start: int,
    byte_end: int,
) -> dict[str, object] | None:
    from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext

    sg_root = get_sg_root(
        context.target_file_path,
        lang="python",
        cache_context=ClassifierCacheContext(),
    )
    query_budget_ms = int(
        1000.0 * (context.budget.startup_timeout_seconds + context.budget.probe_timeout_seconds)
    )
    query_budget_ms = adaptive_query_budget_ms(
        language="python",
        fallback_budget_ms=query_budget_ms,
    )
    payload = enrich_python_context_by_byte_range(
        PythonByteRangeEnrichmentRequest(
            sg_root=sg_root,
            source_bytes=source_bytes,
            byte_start=byte_start,
            byte_end=byte_end,
            cache_key=str(context.target_file_path),
            query_budget_ms=query_budget_ms,
        )
    )
    if not isinstance(payload, dict):
        return None

    merged = dict(payload)
    merged["semantic_planes"] = build_static_semantic_planes(language="python", payload=merged)
    merged["source_attribution"] = {
        "language": "python",
        "pipeline": "static_front_door",
        "symbol_hint": request.symbol_hint,
    }
    return merged


def _execute_rust_provider(
    *,
    request: LanguageSemanticEnrichmentRequest,
    context: _PipelineContext,
) -> LanguageSemanticEnrichmentOutcome:
    source_bytes = _source_bytes(context.target_file_path)
    if source_bytes is None:
        return LanguageSemanticEnrichmentOutcome(
            timed_out=False,
            failure_reason="source_unavailable",
            provider_root=context.provider_root,
        )
    source = source_bytes.decode("utf-8", errors="replace")
    byte_start = _line_col_to_byte_offset(source_bytes, request.line, request.col)
    byte_end = min(len(source_bytes), max(byte_start + 1, byte_start + 32))

    payload, timed_out = call_with_retry(
        lambda: _execute_semantic_task(
            lambda: _rust_payload(
                request=request,
                context=context,
                source=source,
                byte_start=byte_start,
                byte_end=byte_end,
            ),
            timeout_seconds=context.budget.startup_timeout_seconds
            + context.budget.probe_timeout_seconds,
        ),
        max_attempts=context.budget.max_attempts,
        retry_backoff_ms=context.budget.retry_backoff_ms,
    )
    normalized = payload if isinstance(payload, dict) else None
    macro_expansion_count = None
    if isinstance(normalized, dict):
        expansions = normalized.get("macro_expansions")
        if isinstance(expansions, list):
            macro_expansion_count = len(expansions)
    return LanguageSemanticEnrichmentOutcome(
        payload=dict(normalized) if isinstance(normalized, dict) else None,
        timed_out=timed_out,
        failure_reason=_rust_failure_reason(normalized, timed_out=timed_out),
        provider_root=context.provider_root,
        macro_expansion_count=macro_expansion_count,
    )


def _rust_payload(
    *,
    request: LanguageSemanticEnrichmentRequest,
    context: _PipelineContext,
    source: str,
    byte_start: int,
    byte_end: int,
) -> dict[str, object] | None:
    query_budget_ms = int(
        1000.0 * (context.budget.startup_timeout_seconds + context.budget.probe_timeout_seconds)
    )
    query_budget_ms = adaptive_query_budget_ms(
        language="rust",
        fallback_budget_ms=query_budget_ms,
    )
    payload = enrich_rust_context_by_byte_range(
        source,
        byte_start=byte_start,
        byte_end=byte_end,
        cache_key=str(context.target_file_path),
        query_budget_ms=query_budget_ms,
    )
    if not isinstance(payload, dict):
        return None

    merged = dict(payload)
    merged["semantic_planes"] = build_static_semantic_planes(language="rust", payload=merged)
    merged["source_attribution"] = {
        "language": "rust",
        "pipeline": "static_front_door",
        "symbol_hint": request.symbol_hint,
    }
    return merged


def _persist_outcome(
    *,
    context: _PipelineContext,
    outcome: LanguageSemanticEnrichmentOutcome,
    request: LanguageSemanticEnrichmentRequest,
    cache_namespace: str,
) -> None:
    if not context.cache_enabled:
        return
    ttl_seconds = (
        context.namespace_ttl
        if outcome.payload is not None
        else _negative_ttl_seconds(
            reason=outcome.failure_reason,
            namespace_ttl=context.namespace_ttl,
        )
    )
    tag = resolve_write_cache_tag(
        CacheWriteTagRequestV1(
            policy=context.policy,
            workspace=str(context.provider_root.resolve()),
            language=request.language,
            namespace=cache_namespace,
            scope_hash=context.scope_hash,
            snapshot=context.snapshot_digest,
            run_id=request.run_id,
        )
    )
    payload = SemanticOutcomeCacheV1(
        payload=outcome.payload,
        timed_out=outcome.timed_out,
        failure_reason=outcome.failure_reason,
    )
    ok = context.cache.set(
        context.cache_key,
        encode_fragment_payload(payload),
        expire=ttl_seconds,
        tag=tag,
    )
    record_cache_set(namespace=cache_namespace, ok=ok, key=context.cache_key)


def _execute_semantic_task(
    fn: Callable[[], dict[str, object] | None],
    *,
    timeout_seconds: float,
) -> dict[str, object] | None:
    scheduler = get_worker_scheduler()
    future = scheduler.submit_semantic(fn)
    try:
        return future.result(timeout=max(0.05, timeout_seconds))
    except FutureTimeoutError as exc:
        future.cancel()
        timeout_error = TimeoutError("semantic_enrichment_timeout")
        raise timeout_error from exc


def _negative_ttl_seconds(*, reason: str | None, namespace_ttl: int) -> int:
    if reason == "budget_exceeded":
        return max(5, min(60, namespace_ttl // 6))
    if reason in {"parse_error", "source_unavailable", "query_pack_no_signal"}:
        return max(15, min(180, namespace_ttl // 3))
    return max(15, min(120, namespace_ttl // 4))


def _python_failure_reason(
    payload: dict[str, object] | None,
    *,
    timed_out: bool,
) -> str | None:
    if timed_out and payload is None:
        return "budget_exceeded"
    if not isinstance(payload, dict):
        return "source_unavailable"
    status = payload.get("enrichment_status")
    if isinstance(status, str) and status == "applied":
        return None
    parse_quality = payload.get("parse_quality")
    if isinstance(parse_quality, dict) and parse_quality.get("has_error"):
        return "parse_error"
    degrade_reason = payload.get("degrade_reason")
    if isinstance(degrade_reason, str) and degrade_reason:
        return degrade_reason
    return "query_pack_no_signal"


def _rust_failure_reason(
    payload: dict[str, object] | None,
    *,
    timed_out: bool,
) -> str | None:
    if timed_out and payload is None:
        return "budget_exceeded"
    if not isinstance(payload, dict):
        return "source_unavailable"
    status = payload.get("enrichment_status")
    if isinstance(status, str) and status == "applied":
        return None
    degrade_reason = payload.get("degrade_reason")
    if isinstance(degrade_reason, str) and degrade_reason:
        return degrade_reason
    return "query_pack_no_signal"


def fail_open(reason: str) -> SemanticOutcomeV1:
    """Return a fail-open semantic outcome for graceful degradation."""
    return SemanticOutcomeV1(payload=None, timed_out=False, failure_reason=reason)


def enrich_semantics(request: LanguageSemanticEnrichmentRequest) -> SemanticOutcomeV1:
    """Execute semantic enrichment via shared language front door.

    Returns:
        SemanticOutcomeV1: Function return value.
    """
    outcome = enrich_with_language_semantics(request)
    return SemanticOutcomeV1(
        payload=dict(outcome.payload) if isinstance(outcome.payload, dict) else None,
        timed_out=bool(outcome.timed_out),
        failure_reason=outcome.failure_reason,
    )


__all__ = [
    "enrich_semantics",
    "enrich_with_language_semantics",
    "fail_open",
    "run_language_semantic_enrichment",
]
