"""Tests for semantic language provider registry."""

from __future__ import annotations

from pathlib import Path

from tools.cq.search.semantic.models import (
    LanguageSemanticEnrichmentOutcome,
    LanguageSemanticEnrichmentRequest,
)
from tools.cq.search.semantic.registry import LanguageProviderRegistry


def _provider(
    *,
    request: LanguageSemanticEnrichmentRequest,
    context: object,
) -> LanguageSemanticEnrichmentOutcome:
    _ = context
    return LanguageSemanticEnrichmentOutcome(
        payload={"mode": request.mode},
        provider_root=request.root,
    )


def test_registry_register_and_get_normalizes_language_token() -> None:
    """Normalize language token casing for register/get operations."""
    registry = LanguageProviderRegistry()
    registry.register("Python", _provider)

    resolved = registry.get("python")
    assert resolved is not None

    request = LanguageSemanticEnrichmentRequest(
        language="python",
        mode="search",
        root=Path(),
        file_path=Path("x.py"),
        line=1,
        col=0,
    )
    outcome = resolved(request=request, context=object())
    assert outcome.payload == {"mode": "search"}


def test_registry_get_returns_none_for_missing_language() -> None:
    """Return None when no provider is registered for a language."""
    registry = LanguageProviderRegistry()
    assert registry.get("rust") is None
