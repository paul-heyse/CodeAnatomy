"""Bounded cache with configurable eviction policy."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from typing import Literal

EvictionPolicy = Literal["fifo", "lru"]


class BoundedCache[K, V]:
    """Bounded cache with FIFO or LRU eviction.

    Parameters
    ----------
    max_size : int
        Maximum number of entries.
    policy : EvictionPolicy
        Eviction policy ("fifo" or "lru").
    """

    def __init__(self, max_size: int, policy: EvictionPolicy = "fifo") -> None:
        self._max_size = max_size
        self._policy = policy
        self._cache: OrderedDict[K, V] = OrderedDict()

    def get(self, key: K) -> V | None:
        """Get value for key, updating LRU order if applicable.

        Parameters
        ----------
        key : K
            Key to retrieve.

        Returns:
        -------
        V | None
            Value if key exists, None otherwise.
        """
        if key not in self._cache:
            return None
        if self._policy == "lru":
            self._cache.move_to_end(key)
        return self._cache[key]

    def get_or_set(self, key: K, factory: Callable[[], V]) -> V:
        """Get value for key, or compute and store via factory.

        Parameters
        ----------
        key : K
            Key to retrieve or set.
        factory : Callable[[], V]
            Factory function to compute value if key is missing.

        Returns:
        -------
        V
            Cached or newly computed value.
        """
        if key in self._cache:
            if self._policy == "lru":
                self._cache.move_to_end(key)
            return self._cache[key]
        value = factory()
        self._cache[key] = value
        while len(self._cache) > self._max_size:
            self._cache.popitem(last=False)
        return value

    def put(self, key: K, value: V) -> None:
        """Put value for key, evicting oldest if at capacity.

        Parameters
        ----------
        key : K
            Key to set.
        value : V
            Value to store.
        """
        if key in self._cache:
            if self._policy == "lru":
                self._cache.move_to_end(key)
            self._cache[key] = value
            return
        if len(self._cache) >= self._max_size:
            self._cache.popitem(last=False)
        self._cache[key] = value

    def clear(self) -> None:
        """Clear all entries."""
        self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)

    def __contains__(self, key: object) -> bool:
        return key in self._cache


__all__ = ["BoundedCache", "EvictionPolicy"]
