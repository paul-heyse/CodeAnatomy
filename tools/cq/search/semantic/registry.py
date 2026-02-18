"""Registry for language semantic enrichment providers."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from pathlib import Path

from tools.cq.search.semantic.contracts import LanguageEnrichmentProvider

_DEFAULT_LANGUAGE_PROVIDER_REGISTRY_LOCK = threading.Lock()
_DEFAULT_LANGUAGE_PROVIDER_REGISTRY_STATE: dict[str, LanguageProviderRegistry | None] = {
    "registry": None
}


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


def register_default_language_providers(
    *,
    registry: LanguageProviderRegistry,
    python_provider: LanguageEnrichmentProvider,
    rust_provider: LanguageEnrichmentProvider,
) -> LanguageProviderRegistry:
    """Register canonical python/rust providers on a registry instance.

    Returns:
        Updated registry instance.
    """
    registry.register("python", python_provider)
    registry.register("rust", rust_provider)
    return registry


def set_default_language_provider_registry(
    registry: LanguageProviderRegistry | None,
) -> None:
    """Set process-default language provider registry."""
    with _DEFAULT_LANGUAGE_PROVIDER_REGISTRY_LOCK:
        _DEFAULT_LANGUAGE_PROVIDER_REGISTRY_STATE["registry"] = registry


def get_default_language_provider_registry() -> LanguageProviderRegistry | None:
    """Return process-default language provider registry.

    Returns:
        Process-default provider registry, when configured.
    """
    with _DEFAULT_LANGUAGE_PROVIDER_REGISTRY_LOCK:
        return _DEFAULT_LANGUAGE_PROVIDER_REGISTRY_STATE["registry"]


def run_python_byte_range_provider(
    *,
    target_file_path: Path,
    source_bytes: bytes,
    byte_start: int,
    byte_end: int,
    query_budget_ms: int,
) -> dict[str, object] | None:
    """Execute Python concrete byte-range enrichment via registry boundary.

    Returns:
        Python enrichment payload, when available.
    """
    from tools.cq.search.semantic.helpers import run_python_byte_range_enrichment

    return run_python_byte_range_enrichment(
        target_file_path=target_file_path,
        source_bytes=source_bytes,
        byte_start=byte_start,
        byte_end=byte_end,
        query_budget_ms=query_budget_ms,
    )


def run_rust_byte_range_provider(
    *,
    source: str,
    target_file_path: Path,
    byte_start: int,
    byte_end: int,
    query_budget_ms: int,
) -> dict[str, object] | None:
    """Execute Rust concrete byte-range enrichment via registry boundary.

    Returns:
        Rust enrichment payload, when available.
    """
    from tools.cq.search.semantic.helpers import run_rust_byte_range_enrichment

    return run_rust_byte_range_enrichment(
        source=source,
        target_file_path=target_file_path,
        byte_start=byte_start,
        byte_end=byte_end,
        query_budget_ms=query_budget_ms,
    )


__all__ = [
    "LanguageProviderRegistry",
    "get_default_language_provider_registry",
    "register_default_language_providers",
    "run_python_byte_range_provider",
    "run_rust_byte_range_provider",
    "set_default_language_provider_registry",
]
