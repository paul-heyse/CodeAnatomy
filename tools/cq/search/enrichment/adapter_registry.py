"""Registry object for language enrichment adapters."""

from __future__ import annotations

from dataclasses import dataclass, field

from tools.cq.core.types import QueryLanguage
from tools.cq.search.enrichment.contracts import LanguageEnrichmentPort


@dataclass
class LanguageAdapterRegistry:
    """Mutable mapping from language tokens to enrichment adapters."""

    adapters: dict[QueryLanguage, LanguageEnrichmentPort] = field(default_factory=dict)

    def register(self, lang: QueryLanguage, adapter: LanguageEnrichmentPort) -> None:
        """Register or replace adapter for a language."""
        self.adapters[lang] = adapter

    def get(self, lang: QueryLanguage) -> LanguageEnrichmentPort | None:
        """Return adapter for language when registered."""
        return self.adapters.get(lang)

    def clear(self) -> None:
        """Clear all registered adapters."""
        self.adapters.clear()


__all__ = ["LanguageAdapterRegistry"]
