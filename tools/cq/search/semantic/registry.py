"""Registry for language semantic enrichment providers."""

from __future__ import annotations

from dataclasses import dataclass, field

from tools.cq.search.semantic.contracts import LanguageEnrichmentProvider


@dataclass
class LanguageProviderRegistry:
    """Mutable registry of language-specific enrichment providers."""

    providers: dict[str, LanguageEnrichmentProvider] = field(default_factory=dict)

    def register(self, language: str, provider: LanguageEnrichmentProvider) -> None:
        """Register or replace provider for one language token."""
        self.providers[language.strip().lower()] = provider

    def get(self, language: str) -> LanguageEnrichmentProvider | None:
        """Resolve provider for one normalized language token.

        Returns:
            LanguageEnrichmentProvider | None: Registered provider when available.
        """
        return self.providers.get(language.strip().lower())


__all__ = ["LanguageProviderRegistry"]
