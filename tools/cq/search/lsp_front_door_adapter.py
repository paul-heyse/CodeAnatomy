"""Shared language-aware front-door LSP adapter for search/calls/entity."""

from __future__ import annotations

import os
from pathlib import Path

from tools.cq.query.language import QueryLanguage
from tools.cq.search.lsp_contract_state import LspProvider
from tools.cq.search.lsp_request_budget import budget_for_mode, call_with_retry
from tools.cq.search.pyrefly_lsp import PyreflyLspRequest, enrich_with_pyrefly_lsp
from tools.cq.search.rust_lsp import RustLspRequest, enrich_with_rust_lsp

_LSP_DISABLED_VALUES = {"0", "false", "no", "off"}


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


def enrich_with_language_lsp(  # noqa: PLR0913
    *,
    language: QueryLanguage,
    mode: str,
    root: Path,
    file_path: Path,
    line: int,
    col: int,
    symbol_hint: str | None,
) -> tuple[dict[str, object] | None, bool]:
    """Return language-appropriate LSP payload and timeout marker.

    Returns:
    -------
    tuple[dict[str, object] | None, bool]
        ``(payload, timed_out)`` where ``payload`` is normalized enrichment data.
    """
    if not lsp_runtime_enabled():
        return None, False
    budget = budget_for_mode(mode)
    if language == "python":
        request = PyreflyLspRequest(
            root=root,
            file_path=file_path,
            line=max(1, int(line)),
            col=max(0, int(col)),
            symbol_hint=symbol_hint,
            timeout_seconds=budget.probe_timeout_seconds,
            startup_timeout_seconds=budget.startup_timeout_seconds,
        )
        payload, timed_out = call_with_retry(
            lambda: enrich_with_pyrefly_lsp(request),
            max_attempts=budget.max_attempts,
            retry_backoff_ms=budget.retry_backoff_ms,
        )
        return payload if isinstance(payload, dict) else None, timed_out

    request = RustLspRequest(
        file_path=str(file_path),
        line=max(0, int(line) - 1),
        col=max(0, int(col)),
        query_intent="symbol_grounding",
    )
    payload, timed_out = call_with_retry(
        lambda: enrich_with_rust_lsp(
            request,
            root=root,
            startup_timeout_seconds=budget.startup_timeout_seconds,
        ),
        max_attempts=budget.max_attempts,
        retry_backoff_ms=budget.retry_backoff_ms,
    )
    return payload if isinstance(payload, dict) else None, timed_out


__all__ = [
    "enrich_with_language_lsp",
    "infer_language_for_path",
    "lsp_runtime_enabled",
    "provider_for_language",
]
