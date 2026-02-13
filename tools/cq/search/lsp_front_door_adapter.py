"""Shared language-aware front-door LSP adapter for search/calls/entity."""

from __future__ import annotations

import os
from collections.abc import Callable
from concurrent.futures import TimeoutError as FutureTimeoutError
from pathlib import Path

from tools.cq.core.cache import (
    build_cache_key,
    build_cache_tag,
    get_cq_cache_backend,
)
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.core.structs import CqStruct
from tools.cq.query.language import QueryLanguage
from tools.cq.search.lsp_contract_state import LspProvider
from tools.cq.search.lsp_request_budget import budget_for_mode, call_with_retry
from tools.cq.search.pyrefly_lsp import PyreflyLspRequest, enrich_with_pyrefly_lsp
from tools.cq.search.rust_lsp import RustLspRequest, enrich_with_rust_lsp

_LSP_DISABLED_VALUES = {"0", "false", "no", "off"}


class LanguageLspEnrichmentRequest(CqStruct, frozen=True):
    """Request envelope for language-aware front-door LSP enrichment."""

    language: QueryLanguage
    mode: str
    root: Path
    file_path: Path
    line: int
    col: int
    symbol_hint: str | None = None


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


def _safe_file_mtime_ns(file_path: Path) -> int:
    try:
        return file_path.stat().st_mtime_ns
    except (OSError, RuntimeError, ValueError):
        return 0


def _cache_key_for_request(request: LanguageLspEnrichmentRequest) -> str:
    return build_cache_key(
        "lsp_front_door",
        version="v2",
        workspace=str(request.root.resolve()),
        language=request.language,
        target=str(request.file_path),
        extras={
            "mode": request.mode,
            "line": max(1, int(request.line)),
            "col": max(0, int(request.col)),
            "symbol_hint": request.symbol_hint or "",
            "mtime_ns": _safe_file_mtime_ns(request.file_path),
        },
    )


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


def enrich_with_language_lsp(
    request: LanguageLspEnrichmentRequest,
) -> tuple[dict[str, object] | None, bool]:
    """Return language-appropriate LSP payload and timeout marker.

    Returns:
    -------
    tuple[dict[str, object] | None, bool]
        ``(payload, timed_out)`` where ``payload`` is normalized enrichment data.
    """
    if not lsp_runtime_enabled():
        return None, False
    budget = budget_for_mode(request.mode)
    cache = get_cq_cache_backend(root=request.root)
    cache_key = _cache_key_for_request(request)
    cached = cache.get(cache_key)
    if isinstance(cached, dict):
        return dict(cached), False

    if request.language == "python":
        py_request = PyreflyLspRequest(
            root=request.root,
            file_path=request.file_path,
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
        if isinstance(payload, dict):
            expire_seconds = int(max(1.0, budget.probe_timeout_seconds * 300.0))
            cache.set(
                cache_key,
                payload,
                expire=expire_seconds,
                tag=build_cache_tag(
                    workspace=str(request.root.resolve()),
                    language="python",
                ),
            )
            return payload, timed_out
        return None, timed_out

    rust_request = RustLspRequest(
        file_path=str(request.file_path),
        line=max(0, int(request.line) - 1),
        col=max(0, int(request.col)),
        query_intent="symbol_grounding",
    )
    payload, timed_out = call_with_retry(
        lambda: _execute_lsp_task(
            lambda: enrich_with_rust_lsp(
                rust_request,
                root=request.root,
                startup_timeout_seconds=budget.startup_timeout_seconds,
            ),
            timeout_seconds=budget.startup_timeout_seconds + budget.probe_timeout_seconds,
        ),
        max_attempts=budget.max_attempts,
        retry_backoff_ms=budget.retry_backoff_ms,
    )
    if isinstance(payload, dict):
        expire_seconds = int(max(1.0, budget.probe_timeout_seconds * 300.0))
        cache.set(
            cache_key,
            payload,
            expire=expire_seconds,
            tag=build_cache_tag(
                workspace=str(request.root.resolve()),
                language="rust",
            ),
        )
        return payload, timed_out
    return None, timed_out


__all__ = [
    "LanguageLspEnrichmentRequest",
    "enrich_with_language_lsp",
    "infer_language_for_path",
    "lsp_runtime_enabled",
    "provider_for_language",
]
