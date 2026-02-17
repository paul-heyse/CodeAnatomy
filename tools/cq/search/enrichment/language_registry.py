"""Registry for language enrichment adapters."""

from __future__ import annotations

from tools.cq.core.types import QueryLanguage
from tools.cq.search.enrichment.adapter_registry import LanguageAdapterRegistry
from tools.cq.search.enrichment.contracts import LanguageEnrichmentPort

_DEFAULT_ADAPTER_REGISTRY_STATE: dict[str, LanguageAdapterRegistry | None] = {"registry": None}


def _get_default_adapter_registry() -> LanguageAdapterRegistry:
    registry = _DEFAULT_ADAPTER_REGISTRY_STATE.get("registry")
    if registry is None:
        registry = LanguageAdapterRegistry()
        _DEFAULT_ADAPTER_REGISTRY_STATE["registry"] = registry
    return registry


def register_language_adapter(lang: QueryLanguage, adapter: LanguageEnrichmentPort) -> None:
    """Register an adapter for one query language."""
    _get_default_adapter_registry().register(lang, adapter)


def _ensure_defaults() -> None:
    registry = _get_default_adapter_registry()
    if registry.adapters:
        return
    from tools.cq.search.enrichment.python_adapter import PythonEnrichmentAdapter
    from tools.cq.search.enrichment.rust_adapter import RustEnrichmentAdapter

    registry.register("python", PythonEnrichmentAdapter())
    registry.register("rust", RustEnrichmentAdapter())


def get_language_adapter(lang: QueryLanguage) -> LanguageEnrichmentPort | None:
    """Return adapter for language or None when unavailable."""
    _ensure_defaults()
    return _get_default_adapter_registry().get(lang)


def clear_language_adapters() -> None:
    """Clear adapter registry state (test seam)."""
    _get_default_adapter_registry().clear()


__all__ = ["clear_language_adapters", "get_language_adapter", "register_language_adapter"]
