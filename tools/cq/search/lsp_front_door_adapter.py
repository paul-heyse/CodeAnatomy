"""Shared language-aware front-door LSP adapter for search/calls/entity."""

from __future__ import annotations

import os
from pathlib import Path

from tools.cq.query.language import QueryLanguage
from tools.cq.search.lsp_contract_state import LspProvider
from tools.cq.search.lsp_front_door_contracts import (
    LanguageLspEnrichmentOutcome,
    LanguageLspEnrichmentRequest,
)
from tools.cq.search.lsp_front_door_pipeline import run_language_lsp_enrichment

_LSP_DISABLED_VALUES = {"0", "false", "no", "off"}
_LSP_CACHE_NAMESPACE = "lsp_front_door"
_LSP_LOCK_RETRY_COUNT = 20
_LSP_LOCK_RETRY_SLEEP_SECONDS = 0.05


def lsp_runtime_enabled() -> bool:
    """Return whether LSP enrichment is enabled for the current process."""
    raw = os.getenv("CQ_ENABLE_LSP")
    if raw is None:
        return True
    return raw.strip().lower() not in _LSP_DISABLED_VALUES


def infer_language_for_path(file_path: Path) -> QueryLanguage | None:
    """Infer CQ language from file suffix."""
    if file_path.suffix in {".py", ".pyi"}:
        return "python"
    if file_path.suffix == ".rs":
        return "rust"
    return None


def provider_for_language(language: QueryLanguage | str) -> LspProvider:
    """Map CQ language to canonical LSP provider id."""
    if language == "python":
        return "pyrefly"
    if language == "rust":
        return "rust_analyzer"
    return "none"


def enrich_with_language_lsp(
    request: LanguageLspEnrichmentRequest,
) -> LanguageLspEnrichmentOutcome:
    """Return language-appropriate LSP payload and timeout metadata."""
    return run_language_lsp_enrichment(
        request,
        cache_namespace=_LSP_CACHE_NAMESPACE,
        lock_retry_count=_LSP_LOCK_RETRY_COUNT,
        lock_retry_sleep_seconds=_LSP_LOCK_RETRY_SLEEP_SECONDS,
        runtime_enabled=lsp_runtime_enabled(),
    )


__all__ = [
    "LanguageLspEnrichmentOutcome",
    "LanguageLspEnrichmentRequest",
    "enrich_with_language_lsp",
    "infer_language_for_path",
    "lsp_runtime_enabled",
    "provider_for_language",
]
