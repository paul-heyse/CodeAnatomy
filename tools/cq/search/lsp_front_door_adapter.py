"""Shared language-aware front-door LSP adapter for search/calls/entity."""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from concurrent.futures import TimeoutError as FutureTimeoutError
from pathlib import Path

from tools.cq.core.cache import (
    build_cache_key,
    build_scope_hash,
    default_cache_policy,
    file_content_hash,
    get_cq_cache_backend,
    is_namespace_cache_enabled,
    record_cache_delete,
    record_cache_get,
    record_cache_set,
    resolve_namespace_ttl_seconds,
    resolve_write_cache_tag,
)
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.core.structs import CqStruct
from tools.cq.query.language import QueryLanguage
from tools.cq.search.lsp.root_resolution import resolve_lsp_provider_root
from tools.cq.search.lsp_contract_state import LspProvider
from tools.cq.search.lsp_request_budget import budget_for_mode, call_with_retry
from tools.cq.search.pyrefly_lsp import PyreflyLspRequest, enrich_with_pyrefly_lsp
from tools.cq.search.rust_lsp import RustLspRequest, enrich_with_rust_lsp

_LSP_DISABLED_VALUES = {"0", "false", "no", "off"}
_LSP_CACHE_NAMESPACE = "lsp_front_door"
_LSP_LOCK_RETRY_COUNT = 20
_LSP_LOCK_RETRY_SLEEP_SECONDS = 0.05


class LanguageLspEnrichmentRequest(CqStruct, frozen=True):
    """Request envelope for language-aware front-door LSP enrichment."""

    language: QueryLanguage
    mode: str
    root: Path
    file_path: Path
    line: int
    col: int
    symbol_hint: str | None = None
    run_id: str | None = None


class LanguageLspEnrichmentOutcome(CqStruct, frozen=True):
    """Normalized LSP enrichment result for front-door callers."""

    payload: dict[str, object] | None = None
    timed_out: bool = False
    failure_reason: str | None = None
    provider_root: Path | None = None


def lsp_runtime_enabled() -> bool:
    """Return whether LSP enrichment is enabled for the current process.

    Returns:
    -------
    bool
        ``False`` when ``CQ_ENABLE_LSP`` explicitly disables LSP.
    """
    raw = os.getenv("CQ_ENABLE_LSP")
    if raw is None:
        return True
    return raw.strip().lower() not in _LSP_DISABLED_VALUES


def infer_language_for_path(file_path: Path) -> QueryLanguage | None:
    """Infer CQ language from file suffix.

    Returns:
    -------
    QueryLanguage | None
        ``python``/``rust`` for known suffixes, else ``None``.
    """
    if file_path.suffix in {".py", ".pyi"}:
        return "python"
    if file_path.suffix == ".rs":
        return "rust"
    return None


def provider_for_language(language: QueryLanguage | str) -> LspProvider:
    """Map CQ language to canonical LSP provider id.

    Returns:
    -------
    LspProvider
        Canonical provider identifier for the language.
    """
    if language == "python":
        return "pyrefly"
    if language == "rust":
        return "rust_analyzer"
    return "none"


def _cache_key_for_request(
    request: LanguageLspEnrichmentRequest,
    *,
    provider_root: Path,
    target_file_path: Path,
) -> str:
    return build_cache_key(
        _LSP_CACHE_NAMESPACE,
        version="v3",
        workspace=str(provider_root.resolve()),
        language=request.language,
        target=str(target_file_path),
        extras={
            "mode": request.mode,
            "line": max(1, int(request.line)),
            "col": max(0, int(request.col)),
            "symbol_hint": request.symbol_hint or "",
            "file_content_hash": file_content_hash(target_file_path).digest,
        },
    )


def _scope_hash_for_request(
    request: LanguageLspEnrichmentRequest,
    *,
    provider_root: Path,
    target_file_path: Path,
) -> str | None:
    return build_scope_hash(
        {
            "provider_root": str(provider_root.resolve()),
            "target_file": str(target_file_path.resolve()),
            "line": max(1, int(request.line)),
            "col": max(0, int(request.col)),
            "symbol_hint": request.symbol_hint or "",
            "mode": request.mode,
        }
    )


def _negative_ttl_seconds(*, reason: str | None, namespace_ttl: int) -> int:
    if reason == "request_timeout":
        return max(5, min(60, namespace_ttl // 6))
    if reason in {"request_failed", "no_signal"}:
        return max(15, min(180, namespace_ttl // 3))
    return max(15, min(120, namespace_ttl // 4))


def _pyrefly_failure_reason(
    payload: dict[str, object] | None,
    *,
    timed_out: bool,
) -> str | None:
    if timed_out and payload is None:
        return "request_timeout"
    if not isinstance(payload, dict):
        return "request_failed"
    coverage = payload.get("coverage")
    if not isinstance(coverage, dict):
        return None
    status = coverage.get("status")
    if isinstance(status, str) and status == "applied":
        return None
    reason = coverage.get("reason")
    return reason if isinstance(reason, str) and reason else "no_signal"


def _rust_failure_reason(
    payload: dict[str, object] | None,
    *,
    timed_out: bool,
) -> str | None:
    if timed_out and payload is None:
        return "request_timeout"
    if not isinstance(payload, dict):
        return "request_failed"
    advanced = payload.get("advanced_planes")
    if isinstance(advanced, dict):
        reason = advanced.get("reason")
        if isinstance(reason, str) and reason:
            return reason
    degrade_events = payload.get("degrade_events")
    if isinstance(degrade_events, list):
        for event in degrade_events:
            if not isinstance(event, dict):
                continue
            category = event.get("category")
            if isinstance(category, str) and category:
                return category
    return None


def _execute_lsp_task(
    fn: Callable[[], dict[str, object] | None],
    *,
    timeout_seconds: float,
) -> dict[str, object] | None:
    scheduler = get_worker_scheduler()
    future = scheduler.submit_lsp(fn)
    try:
        return future.result(timeout=max(0.05, timeout_seconds))
    except FutureTimeoutError as exc:
        future.cancel()
        msg = "lsp_enrichment_timeout"
        raise TimeoutError(msg) from exc


def _decode_cached_outcome(
    cached: dict[str, object],
    *,
    provider_root: Path,
) -> LanguageLspEnrichmentOutcome:
    payload = cached.get("payload")
    if payload is None and "payload" not in cached:
        payload = cached
    timed_out = cached.get("timed_out")
    failure_reason = cached.get("failure_reason")
    return LanguageLspEnrichmentOutcome(
        payload=dict(payload) if isinstance(payload, dict) else None,
        timed_out=bool(timed_out),
        failure_reason=failure_reason if isinstance(failure_reason, str) else None,
        provider_root=provider_root,
    )


def enrich_with_language_lsp(
    request: LanguageLspEnrichmentRequest,
) -> LanguageLspEnrichmentOutcome:
    """Return language-appropriate LSP payload and timeout marker.

    Returns:
    -------
    LanguageLspEnrichmentOutcome
        Normalized payload, timeout marker, and reason metadata.
    """
    provider_root = resolve_lsp_provider_root(
        language=request.language,
        command_root=request.root,
        file_path=request.file_path,
    )
    normalized_file_path = (
        request.file_path if request.file_path.is_absolute() else provider_root / request.file_path
    )
    if not lsp_runtime_enabled():
        return LanguageLspEnrichmentOutcome(
            timed_out=False,
            failure_reason="not_attempted_runtime_disabled",
            provider_root=provider_root,
        )

    budget = budget_for_mode(request.mode)
    policy = default_cache_policy(root=provider_root)
    cache = get_cq_cache_backend(root=provider_root)
    cache_enabled = is_namespace_cache_enabled(policy=policy, namespace=_LSP_CACHE_NAMESPACE)
    namespace_ttl = resolve_namespace_ttl_seconds(policy=policy, namespace=_LSP_CACHE_NAMESPACE)

    cache_key = _cache_key_for_request(
        request,
        provider_root=provider_root,
        target_file_path=normalized_file_path,
    )

    if cache_enabled:
        cached = cache.get(cache_key)
        record_cache_get(
            namespace=_LSP_CACHE_NAMESPACE,
            hit=isinstance(cached, dict),
            key=cache_key,
        )
        if isinstance(cached, dict):
            return _decode_cached_outcome(cached, provider_root=provider_root)

    lock_key = f"{cache_key}:lock"
    lock_acquired = True
    if cache_enabled:
        lock_acquired = cache.add(
            lock_key,
            {"owner": request.run_id or ""},
            expire=max(1, int(budget.startup_timeout_seconds + budget.probe_timeout_seconds)),
        )
        if not lock_acquired:
            for _ in range(_LSP_LOCK_RETRY_COUNT):
                cached = cache.get(cache_key)
                if isinstance(cached, dict):
                    record_cache_get(
                        namespace=_LSP_CACHE_NAMESPACE,
                        hit=True,
                        key=cache_key,
                    )
                    return _decode_cached_outcome(cached, provider_root=provider_root)
                time.sleep(_LSP_LOCK_RETRY_SLEEP_SECONDS)

    try:
        if request.language == "python":
            py_request = PyreflyLspRequest(
                root=provider_root,
                file_path=normalized_file_path,
                line=max(1, int(request.line)),
                col=max(0, int(request.col)),
                symbol_hint=request.symbol_hint,
                timeout_seconds=budget.probe_timeout_seconds,
                startup_timeout_seconds=budget.startup_timeout_seconds,
            )
            payload, timed_out = call_with_retry(
                lambda: _execute_lsp_task(
                    lambda: enrich_with_pyrefly_lsp(py_request),
                    timeout_seconds=budget.startup_timeout_seconds + budget.probe_timeout_seconds,
                ),
                max_attempts=budget.max_attempts,
                retry_backoff_ms=budget.retry_backoff_ms,
            )
            normalized_payload = payload if isinstance(payload, dict) else None
            failure_reason = _pyrefly_failure_reason(normalized_payload, timed_out=timed_out)
            outcome = LanguageLspEnrichmentOutcome(
                payload=dict(normalized_payload) if isinstance(normalized_payload, dict) else None,
                timed_out=timed_out,
                failure_reason=failure_reason,
                provider_root=provider_root,
            )
        else:
            rust_request = RustLspRequest(
                file_path=str(normalized_file_path),
                line=max(0, int(request.line) - 1),
                col=max(0, int(request.col)),
                query_intent="symbol_grounding",
            )
            payload, timed_out = call_with_retry(
                lambda: _execute_lsp_task(
                    lambda: enrich_with_rust_lsp(
                        rust_request,
                        root=provider_root,
                        startup_timeout_seconds=budget.startup_timeout_seconds,
                    ),
                    timeout_seconds=budget.startup_timeout_seconds + budget.probe_timeout_seconds,
                ),
                max_attempts=budget.max_attempts,
                retry_backoff_ms=budget.retry_backoff_ms,
            )
            normalized_payload = payload if isinstance(payload, dict) else None
            failure_reason = _rust_failure_reason(normalized_payload, timed_out=timed_out)
            outcome = LanguageLspEnrichmentOutcome(
                payload=dict(normalized_payload) if isinstance(normalized_payload, dict) else None,
                timed_out=timed_out,
                failure_reason=failure_reason,
                provider_root=provider_root,
            )

        if cache_enabled:
            ttl_seconds = (
                namespace_ttl
                if outcome.payload is not None
                else _negative_ttl_seconds(
                    reason=outcome.failure_reason,
                    namespace_ttl=namespace_ttl,
                )
            )
            scope_hash = _scope_hash_for_request(
                request,
                provider_root=provider_root,
                target_file_path=normalized_file_path,
            )
            tag = resolve_write_cache_tag(
                policy=policy,
                workspace=str(provider_root.resolve()),
                language=request.language,
                namespace=_LSP_CACHE_NAMESPACE,
                scope_hash=scope_hash,
                snapshot=file_content_hash(normalized_file_path).digest,
                run_id=request.run_id,
            )
            ok = cache.set(
                cache_key,
                {
                    "payload": outcome.payload,
                    "timed_out": outcome.timed_out,
                    "failure_reason": outcome.failure_reason,
                },
                expire=ttl_seconds,
                tag=tag,
            )
            record_cache_set(namespace=_LSP_CACHE_NAMESPACE, ok=ok, key=cache_key)
        return outcome
    finally:
        if cache_enabled and lock_acquired:
            ok = cache.delete(lock_key)
            record_cache_delete(namespace=_LSP_CACHE_NAMESPACE, ok=ok, key=lock_key)


__all__ = [
    "LanguageLspEnrichmentOutcome",
    "LanguageLspEnrichmentRequest",
    "enrich_with_language_lsp",
    "infer_language_for_path",
    "lsp_runtime_enabled",
    "provider_for_language",
]
