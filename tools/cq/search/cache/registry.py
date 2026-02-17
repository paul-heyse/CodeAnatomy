"""Unified cache registry for search-language cache lifecycle."""

from __future__ import annotations

from collections.abc import Callable


class CacheRegistry:
    """Registry of named caches and clear callbacks by language."""

    def __init__(self) -> None:
        """Initialize empty cache and callback registries."""
        self._caches: dict[str, object] = {}
        self._clear_callbacks: dict[str, list[Callable[[], None]]] = {}

    def register_cache(self, language: str, name: str, cache: object) -> None:
        """Register a cache object under a ``language:name`` key."""
        self._caches[f"{language}:{name}"] = cache

    def register_clear_callback(self, language: str, callback: Callable[[], None]) -> None:
        """Register a language-scoped clear callback."""
        self._clear_callbacks.setdefault(language, []).append(callback)

    def clear_all(self, language: str | None = None) -> None:
        """Clear registered caches and invoke matching callbacks."""
        for key, cache in self._caches.items():
            cache_lang, _ = key.split(":", maxsplit=1)
            if language is not None and cache_lang != language:
                continue
            clear = getattr(cache, "clear", None)
            if callable(clear):
                clear()

        if language is None:
            callbacks = [cb for rows in self._clear_callbacks.values() for cb in rows]
        else:
            callbacks = list(self._clear_callbacks.get(language, []))
        for callback in callbacks:
            callback()


CACHE_REGISTRY = CacheRegistry()


__all__ = ["CACHE_REGISTRY", "CacheRegistry"]
