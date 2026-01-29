"""Standard registry protocols and base implementations."""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Protocol, TypeVar, runtime_checkable

K = TypeVar("K")
V = TypeVar("V")


@runtime_checkable
class Registry(Protocol[K, V]):
    """Protocol for registry implementations."""

    @abstractmethod
    def register(self, key: K, value: V) -> None:
        """Register a value with the given key."""

    @abstractmethod
    def get(self, key: K) -> V | None:
        """Retrieve a value by key, or None if not found."""

    @abstractmethod
    def __contains__(self, key: K) -> bool:
        """Check if key is registered."""

    @abstractmethod
    def __iter__(self) -> Iterator[K]:
        """Iterate over registered keys."""

    @abstractmethod
    def __len__(self) -> int:
        """Return count of registered items."""


@dataclass
class MutableRegistry[K, V]:
    """Standard mutable registry with dict storage."""

    _entries: dict[K, V] = field(default_factory=dict)

    def register(self, key: K, value: V, *, overwrite: bool = False) -> None:
        """Register a value for the provided key.

        Parameters
        ----------
        key
            Key to register.
        value
            Value to associate with the key.
        overwrite
            Whether to replace existing entries.

        Raises
        ------
        ValueError
            Raised when the key is already registered and overwrite is ``False``.
        """
        if key in self._entries and not overwrite:
            msg = f"Key {key!r} already registered. Use overwrite=True."
            raise ValueError(msg)
        self._entries[key] = value

    def get(self, key: K) -> V | None:
        """Retrieve a value by key.

        Parameters
        ----------
        key
            Key to look up.

        Returns
        -------
        V | None
            Registered value, or ``None`` when missing.
        """
        return self._entries.get(key)

    def __contains__(self, key: K) -> bool:
        """Check whether a key is registered.

        Parameters
        ----------
        key
            Key to check.

        Returns
        -------
        bool
            ``True`` if the key is registered.
        """
        return key in self._entries

    def __iter__(self) -> Iterator[K]:
        """Iterate over registered keys.

        Returns
        -------
        Iterator[K]
            Iterator over registered keys.
        """
        return iter(self._entries)

    def __len__(self) -> int:
        """Return the count of registered entries.

        Returns
        -------
        int
            Number of registered entries.
        """
        return len(self._entries)

    def items(self) -> Iterator[tuple[K, V]]:
        """Iterate over registry entries.

        Returns
        -------
        Iterator[tuple[K, V]]
            Iterator over key/value pairs.
        """
        return iter(self._entries.items())

    def snapshot(self) -> Mapping[K, V]:
        """Return immutable snapshot of current state.

        Returns
        -------
        Mapping[K, V]
            Snapshot of registry entries.
        """
        return dict(self._entries)


@dataclass(frozen=True)
class ImmutableRegistry[K, V]:
    """Frozen registry built from a sequence of entries."""

    _entries: tuple[tuple[K, V], ...]

    def get(self, key: K) -> V | None:
        """Retrieve a value by key.

        Parameters
        ----------
        key
            Key to look up.

        Returns
        -------
        V | None
            Registered value, or ``None`` when missing.
        """
        for k, v in self._entries:
            if k == key:
                return v
        return None

    def __contains__(self, key: K) -> bool:
        """Check whether a key is registered.

        Parameters
        ----------
        key
            Key to check.

        Returns
        -------
        bool
            ``True`` if the key is registered.
        """
        return any(k == key for k, _ in self._entries)

    def __iter__(self) -> Iterator[K]:
        """Iterate over registered keys.

        Returns
        -------
        Iterator[K]
            Iterator over registered keys.
        """
        return (k for k, _ in self._entries)

    def __len__(self) -> int:
        """Return the count of registered entries.

        Returns
        -------
        int
            Number of registered entries.
        """
        return len(self._entries)

    @classmethod
    def from_dict(cls, data: Mapping[K, V]) -> ImmutableRegistry[K, V]:
        """Create an immutable registry from a mapping.

        Parameters
        ----------
        data
            Source mapping of entries.

        Returns
        -------
        ImmutableRegistry[K, V]
            Frozen registry containing the mapping entries.
        """
        return cls(tuple(data.items()))


__all__ = [
    "ImmutableRegistry",
    "MutableRegistry",
    "Registry",
]
