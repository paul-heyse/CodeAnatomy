"""Shared language-aware front-door static semantic adapter for search/calls/entity."""

from __future__ import annotations

import os
from pathlib import Path

from tools.cq.query.language import QueryLanguage
from tools.cq.search.language_front_door_contracts import (
    LanguageSemanticEnrichmentOutcome,
    LanguageSemanticEnrichmentRequest,
)
from tools.cq.search.language_front_door_pipeline import run_language_semantic_enrichment
from tools.cq.search.semantic_contract_state import SemanticProvider

_SEMANTIC_DISABLED_VALUES = {"0", "false", "no", "off"}
_SEMANTIC_CACHE_NAMESPACE = "semantic_front_door"
_SEMANTIC_LOCK_RETRY_COUNT = 20
_SEMANTIC_LOCK_RETRY_SLEEP_SECONDS = 0.05


def semantic_runtime_enabled() -> bool:
    """Return whether semantic enrichment is enabled for the current process.

    Returns:
        bool: True when semantic enrichment is enabled.
    """
    raw = os.getenv("CQ_ENABLE_SEMANTIC_ENRICHMENT")
    if raw is None:
        return True
    return raw.strip().lower() not in _SEMANTIC_DISABLED_VALUES


def infer_language_for_path(file_path: Path) -> QueryLanguage | None:
    """Infer CQ language from file suffix.

    Returns:
        QueryLanguage | None: Detected language, or ``None`` when unsupported.
    """
    if file_path.suffix in {".py", ".pyi"}:
        return "python"
    if file_path.suffix == ".rs":
        return "rust"
    return None


def provider_for_language(language: QueryLanguage | str) -> SemanticProvider:
    """Map CQ language to canonical static semantic provider id.

    Returns:
        SemanticProvider: Static provider identifier for semantic lookups.
    """
    if language == "python":
        return "python_static"
    if language == "rust":
        return "rust_static"
    return "none"


def enrich_with_language_semantics(
    request: LanguageSemanticEnrichmentRequest,
) -> LanguageSemanticEnrichmentOutcome:
    """Return language-appropriate static semantic payload and timeout metadata."""
    return run_language_semantic_enrichment(
        request,
        cache_namespace=_SEMANTIC_CACHE_NAMESPACE,
        lock_retry_count=_SEMANTIC_LOCK_RETRY_COUNT,
        lock_retry_sleep_seconds=_SEMANTIC_LOCK_RETRY_SLEEP_SECONDS,
        runtime_enabled=semantic_runtime_enabled(),
    )


__all__ = [
    "LanguageSemanticEnrichmentOutcome",
    "LanguageSemanticEnrichmentRequest",
    "enrich_with_language_semantics",
    "infer_language_for_path",
    "provider_for_language",
    "semantic_runtime_enabled",
]
