"""Phased execution pipeline for language-aware static semantic enrichment."""

from __future__ import annotations

import time
from collections.abc import Callable
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from pathlib import Path

import msgspec

from tools.cq.core.cache import (
    CacheWriteTagRequestV1,
    CqCacheBackend,
    CqCachePolicyV1,
    build_cache_key,
    build_scope_hash,
    default_cache_policy,
    file_content_hash,
    get_cq_cache_backend,
    is_namespace_cache_enabled,
    record_cache_decode_failure,
    record_cache_delete,
    record_cache_get,
    record_cache_set,
    resolve_namespace_ttl_seconds,
    resolve_write_cache_tag,
)
from tools.cq.core.contracts import contract_to_builtins
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.search.classifier import get_sg_root
from tools.cq.search.language_front_door_contracts import (
    LanguageSemanticEnrichmentOutcome,
    LanguageSemanticEnrichmentRequest,
    SemanticOutcomeCacheV1,
)
from tools.cq.search.language_root_resolution import resolve_language_provider_root
from tools.cq.search.python_enrichment import enrich_python_context_by_byte_range
from tools.cq.search.requests import PythonByteRangeEnrichmentRequest
from tools.cq.search.rust_enrichment import enrich_rust_context_by_byte_range
from tools.cq.search.semantic_planes_static import build_static_semantic_planes
from tools.cq.search.semantic_request_budget import (
    SemanticRequestBudgetV1,
    budget_for_mode,
    call_with_retry,
)


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

    lock_acquired, waited_outcome = _single_flight_gate(
        context=context,
        request=request,
        cache_namespace=cache_namespace,
        lock_retry_count=lock_retry_count,
        lock_retry_sleep_seconds=lock_retry_sleep_seconds,
    )
    if waited_outcome is not None:
        return waited_outcome
    try:
        outcome = _execute_provider(request=request, context=context)
        _persist_outcome(
            context=context,
            outcome=outcome,
            request=request,
            cache_namespace=cache_namespace,
        )
        return outcome
    finally:
        _release_lock(
            context=context,
            lock_acquired=lock_acquired,
            cache_namespace=cache_namespace,
        )


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
        hit=isinstance(cached, dict),
        key=context.cache_key,
    )
    if not isinstance(cached, dict):
        return None
    try:
        payload = msgspec.convert(cached, type=SemanticOutcomeCacheV1)
    except (RuntimeError, TypeError, ValueError):
        record_cache_decode_failure(namespace=cache_namespace)
        return None
    return LanguageSemanticEnrichmentOutcome(
        payload=dict(payload.payload) if isinstance(payload.payload, dict) else None,
        timed_out=bool(payload.timed_out),
        failure_reason=payload.failure_reason,
        provider_root=context.provider_root,
    )


def _single_flight_gate(
    *,
    context: _PipelineContext,
    request: LanguageSemanticEnrichmentRequest,
    cache_namespace: str,
    lock_retry_count: int,
    lock_retry_sleep_seconds: float,
) -> tuple[bool, LanguageSemanticEnrichmentOutcome | None]:
    if not context.cache_enabled:
        return True, None
    lock_acquired = context.cache.add(
        context.lock_key,
        {"owner": request.run_id or ""},
        expire=max(
            1,
            int(context.budget.startup_timeout_seconds + context.budget.probe_timeout_seconds),
        ),
    )
    if lock_acquired:
        return True, None
    for _ in range(lock_retry_count):
        waiter = _probe_cached_outcome(context=context, cache_namespace=cache_namespace)
        if waiter is not None:
            return False, waiter
        time.sleep(lock_retry_sleep_seconds)
    return False, None


def _execute_provider(
    *,
    request: LanguageSemanticEnrichmentRequest,
    context: _PipelineContext,
) -> LanguageSemanticEnrichmentOutcome:
    if request.language == "python":
        return _execute_python_provider(request=request, context=context)
    return _execute_rust_provider(request=request, context=context)


def _line_col_to_byte_offset(source_bytes: bytes, line: int, col: int) -> int:
    safe_line = max(1, int(line))
    safe_col = max(0, int(col))
    lines = source_bytes.splitlines(keepends=True)
    if not lines:
        return 0
    safe_line = min(safe_line, len(lines))
    offset = sum(len(chunk) for chunk in lines[: safe_line - 1])
    line_bytes = lines[safe_line - 1]
    line_text = line_bytes.decode("utf-8", errors="replace")
    clamped_col = min(safe_col, len(line_text))
    byte_col = len(line_text[:clamped_col].encode("utf-8", errors="replace"))
    return min(offset + byte_col, len(source_bytes))


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
    sg_root = get_sg_root(context.target_file_path, lang="python")
    payload = enrich_python_context_by_byte_range(
        PythonByteRangeEnrichmentRequest(
            sg_root=sg_root,
            source_bytes=source_bytes,
            byte_start=byte_start,
            byte_end=byte_end,
            cache_key=str(context.target_file_path),
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
    return LanguageSemanticEnrichmentOutcome(
        payload=dict(normalized) if isinstance(normalized, dict) else None,
        timed_out=timed_out,
        failure_reason=_rust_failure_reason(normalized, timed_out=timed_out),
        provider_root=context.provider_root,
    )


def _rust_payload(
    *,
    request: LanguageSemanticEnrichmentRequest,
    context: _PipelineContext,
    source: str,
    byte_start: int,
    byte_end: int,
) -> dict[str, object] | None:
    payload = enrich_rust_context_by_byte_range(
        source,
        byte_start=byte_start,
        byte_end=byte_end,
        cache_key=str(context.target_file_path),
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
        contract_to_builtins(payload),
        expire=ttl_seconds,
        tag=tag,
    )
    record_cache_set(namespace=cache_namespace, ok=ok, key=context.cache_key)


def _release_lock(
    *,
    context: _PipelineContext,
    lock_acquired: bool,
    cache_namespace: str,
) -> None:
    if not (context.cache_enabled and lock_acquired):
        return
    ok = context.cache.delete(context.lock_key)
    record_cache_delete(namespace=cache_namespace, ok=ok, key=context.lock_key)


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


__all__ = [
    "run_language_semantic_enrichment",
]
