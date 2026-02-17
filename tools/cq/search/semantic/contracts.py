"""Contracts for semantic enrichment provider dispatch."""

from __future__ import annotations

from typing import Protocol

from tools.cq.search.semantic.models import (
    LanguageSemanticEnrichmentOutcome,
    LanguageSemanticEnrichmentRequest,
)


class LanguageEnrichmentProvider(Protocol):
    """Callable provider for one language semantic enrichment lane."""

    def __call__(
        self,
        *,
        request: LanguageSemanticEnrichmentRequest,
        context: object,
    ) -> LanguageSemanticEnrichmentOutcome:
        """Execute semantic enrichment for a single language lane."""
        ...


__all__ = ["LanguageEnrichmentProvider"]
