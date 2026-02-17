"""Registry for language enrichment adapters."""

from __future__ import annotations

from tools.cq.core.types import QueryLanguage
from tools.cq.search.enrichment.contracts import LanguageEnrichmentPort

_LANGUAGE_ADAPTERS: dict[QueryLanguage, LanguageEnrichmentPort] = {}


def register_language_adapter(lang: QueryLanguage, adapter: LanguageEnrichmentPort) -> None:
    """Register an adapter for one query language."""
    _LANGUAGE_ADAPTERS[lang] = adapter


def _ensure_defaults() -> None:
    if _LANGUAGE_ADAPTERS:
        return
    from tools.cq.search.enrichment.python_adapter import PythonEnrichmentAdapter
    from tools.cq.search.enrichment.rust_adapter import RustEnrichmentAdapter

    register_language_adapter("python", PythonEnrichmentAdapter())
    register_language_adapter("rust", RustEnrichmentAdapter())


def get_language_adapter(lang: QueryLanguage) -> LanguageEnrichmentPort | None:
    """Return adapter for language or None when unavailable."""
    _ensure_defaults()
    return _LANGUAGE_ADAPTERS.get(lang)


__all__ = ["get_language_adapter", "register_language_adapter"]
