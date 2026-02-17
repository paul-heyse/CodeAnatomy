"""Registry for language enrichment adapters."""

from __future__ import annotations

import threading

from tools.cq.core.types import QueryLanguage
from tools.cq.search.enrichment.adapter_registry import LanguageAdapterRegistry
from tools.cq.search.enrichment.contracts import LanguageEnrichmentPort

_DEFAULT_ADAPTER_REGISTRY_LOCK = threading.Lock()
_DEFAULT_ADAPTER_REGISTRY_STATE: dict[str, LanguageAdapterRegistry | None] = {"registry": None}


def get_default_adapter_registry() -> LanguageAdapterRegistry:
    """Return process-default language-adapter registry."""
    with _DEFAULT_ADAPTER_REGISTRY_LOCK:
        registry = _DEFAULT_ADAPTER_REGISTRY_STATE["registry"]
        if registry is None:
            registry = LanguageAdapterRegistry()
            _DEFAULT_ADAPTER_REGISTRY_STATE["registry"] = registry
        return registry


def set_default_adapter_registry(registry: LanguageAdapterRegistry | None) -> None:
    """Set or reset process-default adapter registry (test seam)."""
    with _DEFAULT_ADAPTER_REGISTRY_LOCK:
        _DEFAULT_ADAPTER_REGISTRY_STATE["registry"] = registry


def register_language_adapter(lang: QueryLanguage, adapter: LanguageEnrichmentPort) -> None:
    """Register an adapter for one query language."""
    get_default_adapter_registry().register(lang, adapter)


def _ensure_defaults() -> None:
    registry = get_default_adapter_registry()
    if registry.adapters:
        return
    with _DEFAULT_ADAPTER_REGISTRY_LOCK:
        if registry.adapters:
            return
        from tools.cq.search.enrichment.python_adapter import PythonEnrichmentAdapter
        from tools.cq.search.enrichment.rust_adapter import RustEnrichmentAdapter

        registry.register("python", PythonEnrichmentAdapter())
        registry.register("rust", RustEnrichmentAdapter())


def get_language_adapter(lang: QueryLanguage) -> LanguageEnrichmentPort | None:
    """Return adapter for language or None when unavailable."""
    _ensure_defaults()
    return get_default_adapter_registry().get(lang)


def clear_language_adapters() -> None:
    """Clear adapter registry state (test seam)."""
    get_default_adapter_registry().clear()


__all__ = [
    "clear_language_adapters",
    "get_default_adapter_registry",
    "get_language_adapter",
    "register_language_adapter",
    "set_default_adapter_registry",
]
